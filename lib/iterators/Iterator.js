/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* An iterator allows fast pull-based access to a stream of items. */

var EventEmitter = require('events').EventEmitter,
    util = require('util');

// Empty placeholder function
function noop() {}

// Creates a new Iterator
function Iterator(options) {
  if (!(this instanceof Iterator))
    return new Iterator(options);
  EventEmitter.call(this);

  options = options || {};
  // Internal buffer of preloaded items
  this._buffer = [];
  // Desired maximum buffer size; negative means the iterator has ended
  // TODO: setting options.bufferSize to 0 doesn't work (some readable events don't fire)
  this._maxBufferSize = options.bufferSize || 4;

  // Start filling the buffer when the first 'readable' listener is attached
  this.on('newListener', function waitForReadableListener(event) {
    if (event === 'readable') {
      this._fillBufferAsync();
      this.removeListener('newListener', waitForReadableListener);
    }
  });

  // Drain the iterator when 'data' listeners are attached
  this.on('newListener', function waitForDataListener(event) {
    if (event === 'data') {
      var self = this;
      this.removeListener('newListener', waitForDataListener);
      function readAll() {
        // Try to read an item while there are still data listeners
        var item;
        while (EventEmitter.listenerCount(self, 'data') !== 0 &&
               (item = self.read()) !== null)
          self.emit('data', item);
        // Detach the listener if nobody is listening anymore
        if (EventEmitter.listenerCount(self, 'data') === 0) {
          self.removeListener('readable', readAll);
          self.removeListener('newListener', waitForDataListener); // A listener could already be attached
          self.addListener('newListener', waitForDataListener);
        }
      }
      this.removeListener('readable', readAll); // A listener could already be attached
      this.addListener('readable', readAll);
      setImmediate(readAll);
    }
  });

}
util.inherits(Iterator, EventEmitter);

// Reads an item from the iterator, or returns `null` if none is available
Iterator.prototype.read = function () {
  // Try to add a new item to the buffer if it is empty
  var item = null, buffer = this._buffer;
  if (buffer.length === 0 && this._maxBufferSize >= 0) {
    try { this._read(); }
    catch (readError) { this._error(readError); }
  }
  // Return the first item of the buffer if it exists
  if (buffer.length !== 0) {
    item = buffer.shift();
    this._fillBufferAsync();
  }
  return item;
};

// Closes the iterator, clearing pending and future events
Iterator.prototype.close = function () {
  if (!this._ended) {
    this.emit('close');
    this._rejectDataListeners();
  }
};

// Reads items into the buffer by calling `this._push`/`push`
Iterator.prototype._read = function () {
  throw new Error('The _read method has not been implemented.');
};

// Adds an item to the iterator, or ends it if `null` is pushed;
// arrival of new data can be signaled with `push()`
Iterator.prototype._push = function (item) {
  // `null` indicates the end of the stream
  if (item === null)
    return this._end();
  // A missing `item` parameter signals the iterator will become readable
  if (item === undefined)
    return this._ended || this.emit('readable');
  // Negative maximum buffer size means the stream has ended
  if (this._maxBufferSize < 0)
    throw new Error('Cannot push because the iterator was ended.');
  this._buffer.push(item);
  this.emit('readable');
};

// Signals that no further items will become available
Iterator.prototype._end = function () {
  if (this._maxBufferSize >= 0) {
    // Indicate we are no longer accepting new items
    this._maxBufferSize = -1;
    // Try to fill the buffer (if the end is reached, 'end' will be emitted)
    this._fillBufferAsync();
  }
};

// Fills the buffer using `_read` to speed up subsequent `read` calls,
// or emits the 'end' event if no items are left.
Iterator.prototype._fillBuffer = function () {
  // Keep filling as long as the length changes and is less than the maximum
  var buffer = this._buffer, prevBufferLength = -1;
  while (prevBufferLength !== buffer.length && this._buffer.length < this._maxBufferSize) {
    prevBufferLength = buffer.length;
    try { this._read && this._read(); }
    catch (readError) { this._error(readError); }
  }
  // Emit the 'end' event if the iterator is empty
  if (this.ended) {
    this.emit('end');
    this._rejectDataListeners();
  }
};

// Asynchronously calls `_fillBuffer`
Iterator.prototype._fillBufferAsync = function () {
  // Fill if the buffer size is below maximum or the iterator has ended
  if (!this._fillBufferPending && (this._buffer.length < this._maxBufferSize || this.ended)) {
    this._fillBufferPending = true;
    setImmediate(function (self) {
      self._fillBufferPending = false;
      self._fillBuffer();
    }, this);
  }
};

// Continuously fill the buffer as far as possible
Iterator.prototype._bufferAll = function () {
  this._maxBufferSize = Infinity;
  this._fillBufferAsync();
  this.on('readable', this._fillBuffer.bind(this));
};

// Signals that an iterator error has occurred
Iterator.prototype._error = function (error) {
  if (!this._handlingError) {
    this._handlingError = true;
    this.emit('error', error);
  }
};

// Removes all current and future data listeners
Iterator.prototype._rejectDataListeners = function () {
  removeDataListeners.call(this);
  this.removeListener('newListener', removeDataListeners);
  this.on('newListener', removeDataListeners);
};
function removeDataListeners() {
  this.removeAllListeners('data');
  this.removeAllListeners('readable');
  this.removeAllListeners('end');
  this.removeAllListeners('error');
}

// Returns whether the iterator has no more items
Object.defineProperty(Iterator.prototype, 'ended', {
  enumerable: true,
  // An iterator has ended if the buffer is empty and the maximum buffer size negative
  get: function () { return this._buffer.length === 0 && this._maxBufferSize < 0; },
});

// Checks whether a value has been set for the given property
Iterator.prototype.hasProperty = function (propertyName) {
  return ('_properties' in this) && (propertyName in this._properties);
};

// Gets the value of the property,
// either as return value (sync) or through the callback (async, when ready)
Iterator.prototype.getProperty = function (propertyName, callback, self) {
  if (this.hasProperty(propertyName)) {
    var value = this._properties[propertyName];
    return callback ? (self ? callback.call(self, value) : callback(value)) : value;
  }
  callback && this.once(propertyName + 'Set', self ? callback.bind(self) : callback);
};

// Sets the property to the given value
Iterator.prototype.setProperty = function (propertyName, value) {
  var properties = this._properties || (this._properties = Object.create(null));
  properties[propertyName] = value;
  this.emit(propertyName + 'Set', value);
};

// Inherits the properties of the source iterator
Iterator.prototype._inheritProperties = function (source) {
  if (source.setProperty) {
    // Ensure the source has properties
    source.setProperty('child', true);
    this._properties = Object.create(source._properties);
    // Add existing property listeners to source
    for (var event in this._events) {
      /\Set$/.test(event) && this.listeners(event).forEach(function (listener) {
        source.addListener(event, listener);
      });
    }
    // Add new property listeners to source
    this.on('newListener', function (event, listener) {
      if (/\Set$/.test(event))
        source.addListener(event, listener);
    });
  }
};

// Returns the remaining items of the iterator as an array
Iterator.prototype.toArray = function (callback) {
  var self = this, items = [];
  if (this.ended) return done();
  this.on('data',  read);
  this.on('end',   done);
  this.on('error', done);
  function read(item) { items.push(item); }
  function done(error) {
    self.removeListener('data',  read);
    self.removeListener('end',   done);
    self.removeListener('error', done);
    try { callback && callback(error, error ? null : items); }
    finally { callback = items = null; }
  }
};

// Creates a clone of this iterator that replays the stream.
// When the first clone is created, the stream is cached in memory.
// After cloning, the iterator may not be read directly.
Iterator.prototype.clone = function () {
  // If no clone was created of this iterator, initialize the cloning mechanism
  if (!this._origRead) {
    // `_cache` will hold all items read by the buffer
    this._cache = [];
    // Remove the listener limit, as clones might use a lot of listeners
    this.setMaxListeners(0);
    // Disable regular reading; all reads should happen through clones
    this._origRead = this.read;
    this.read = function () {
      throw new Error('This iterator has been cloned and may not be read directly.');
    };
  }

  // Create a new clone that inherits its properties from this iterator
  var clone = new Iterator();
  clone._inheritProperties(this);
  // If there are no items in this stream, just end the clone
  if (this.ended && this._cache.length === 0)
    clone._end();
  // Allow this clone to be read
  else {
    var source = this, readPosition = 0;
    // Reads an item from the clone
    clone.read = function () {
      var item;
      // Try to retrieve the item from cache
      if (readPosition < source._cache.length)
        item = source._cache[readPosition++];
      // If unavailable, try to read it from the source iterator
      else {
        item = source._origRead();
        item && (source._cache[readPosition++] = item);
      }
      // If no more items are available, end the clone
      if (source.ended && readPosition === source._cache.length)
        clone._end();
      return item;
    };
    // Disable the buffered reading mechanism
    clone._read = noop;
    // Signal when new items are available
    this.on('readable', function () { clone.emit('readable'); });
    // Signal when no more items will become available
    this.on('end', function () {
      if (readPosition === source._cache.length)
        clone._end();
    });
    // Pass errors from the source
    this.on('error', function (error) { clone.emit('error', error); });
  }
  // Inherit toString of parent
  var toString = this.toString();
  clone.toString = function () { return '(cloneOf) ' + toString; };

  return clone;
};

// Generates a textual representation of the iterator
Iterator.prototype.toString = function () {
  return '[' + this.constructor.name + ']';
};

// Makes the specified class inherit from the iterator
Iterator.inherits = function (child) {
  util.inherits(child, this);
  child.inherits = this.inherits;
};




/*    PRE-DEFINED ITERATOR TYPES    */


// Creates an empty iterator
function EmptyIterator() {
  if (!(this instanceof EmptyIterator))
    return new EmptyIterator();
  Iterator.call(this);
  this._end();
}
Iterator.inherits(EmptyIterator);



// Creates a single-item iterator
function SingleIterator(item) {
  if (!(this instanceof SingleIterator))
    return new SingleIterator(item);
  Iterator.call(this);
  this._push(item);
  this._end();
}
Iterator.inherits(SingleIterator);



// Creates an iterator that never emits an item
function WaitingIterator() {
  if (!(this instanceof WaitingIterator))
    return new WaitingIterator();
  Iterator.call(this);
  this._rejectDataListeners();
}
Iterator.inherits(WaitingIterator);

// Don't read anything
WaitingIterator.prototype.read = function () { return null; };



// Creates an iterator from an array
function ArrayIterator(array) {
  if (!(this instanceof ArrayIterator))
    return new ArrayIterator(array);
  Iterator.call(this);

  // Push all items of the array and end the iterator
  var length = (array && array.length) | 0;
  if (length > 0) {
    var buffer = this._buffer = new Array(length);
    for (var i = 0; i < length; i++)
      buffer[i] = array[i];
  }
  this._end();
}
Iterator.inherits(ArrayIterator);



// Creates an iterator that bases its output on another iterator or stream.
// The source can be passed as an optional first argument, or set later through `setSource`.
function TransformIterator(source, options) {
  if (!(this instanceof TransformIterator))
    return new TransformIterator(source, options);
  // Shift the arguments if the optional `source` argument is not given
  if (source && typeof source.on !== 'function') options = source, source = null;
  Iterator.call(this, options);
  source && this.setSource(source);
}
Iterator.inherits(TransformIterator);

// Keep track of the transformer's status
var WAITING = 0, TRANSFORMING = 1, ENDING = 2;
TransformIterator.prototype._transformStatus = WAITING;

// Sets the iterator's source
TransformIterator.prototype.setSource = function (source) {
  if (this.hasOwnProperty('_source'))
    throw new Error('Source already set.');
  this._source = source;
  this._properties || this._inheritProperties(source);

  // If the source is still active, listen to its events
  if (!source.ended) {
    var self = this;
    source.on('error', function (error) { self._error(error); });
    source.on('end', function () {
      // If not transforming, end immediately
      if (self._transformStatus === WAITING) self._flush();
      // Otherwise, end as soon as the transformation has finished
      else self._transformStatus = ENDING;
    });
    // When a readable listener is attached, wait for the source to become readable
    this.on('newListener', function waitForReadable(event) {
      if (event === 'readable') {
        self._fillBufferAsync();
        source.on('readable', function () { self._fillBuffer(); });
        this.removeListener('newListener', waitForReadable);
      }
    });
    if (EventEmitter.listenerCount(this, 'readable') !== 0)
      this.emit('newListener', 'readable');
  }
  // If the source has already ended, simply end this iterator
  else this._end();
};

// The default source never emits items
TransformIterator.prototype._source = WaitingIterator();

// Reads data by calling `_transform`
TransformIterator.prototype._read = function () {
  switch (this._transformStatus) {
  // Read if we're not already transforming
  case WAITING:
    var item = this._source.read();
    if (item !== null) {
      var self = this;
      this._transformStatus = TRANSFORMING;
      this._transform(item, function () {
        if (self._transformStatus === TRANSFORMING)
          self._transformStatus = WAITING;
        self.emit('readable', self);
      });
    }
    break;
  // End the iterator if the source has ended
  case ENDING:
    this._transformStatus = WAITING;
    this._flush();
    break;
  }
};

// Transforms data from the source iterator
TransformIterator.prototype._transform = function (item, done) {
  throw new Error('The _transform method has not been implemented.');
};

// Flushes remaining data after the source has ended
TransformIterator.prototype._flush = function () {
  this._end();
};

// Generates a textual representation of the iterator
TransformIterator.prototype.toString = function () {
  return Iterator.prototype.toString.call(this) +
         '\n  <= ' + this.getSourceString();
};

// Generates a textual representation of the source of this iterator
TransformIterator.prototype.getSourceString = function () {
  return this.hasOwnProperty('_source') ? this._source.toString() : '(none)';
};



// Creates an iterator with another iterator or stream as source
function PassthroughIterator(source, options) {
  if (!(this instanceof PassthroughIterator))
    return new PassthroughIterator(source, options);
  TransformIterator.call(this, source, options);
}
TransformIterator.inherits(PassthroughIterator);

// Sets the iterator's source and inherit its properties
PassthroughIterator.prototype.setSource = function (source) {
  TransformIterator.prototype.setSource.call(this, source);
  this._inheritProperties(source);
};

// Reads a chunk from the source
PassthroughIterator.prototype._read = function () {
  // If an item was ready, push it
  var item = this._source && this._source.read();
  if (item !== null) this._push(item);
};

// Generates a textual representation of the iterator
PassthroughIterator.prototype.toString = function () {
  if (!this.hasOwnProperty('_source'))
    return Iterator.prototype.toString.call(this);
  return this.getSourceString();
};



// Creates a push-only iterator with an in-memory buffer
function BufferIterator() {
  if (!(this instanceof BufferIterator))
    return new BufferIterator();
  Iterator.call(this);
}
Iterator.inherits(BufferIterator);

// Don't actively read; wait for _push calls
BufferIterator.prototype._read = noop;




/*             EXPORTS              */


module.exports = Iterator;
Iterator.Iterator = Iterator;
Iterator.EmptyIterator = Iterator.empty = EmptyIterator;
Iterator.SingleIterator = Iterator.single = SingleIterator;
Iterator.WaitingIterator = Iterator.waiting = WaitingIterator;
Iterator.ArrayIterator = Iterator.fromArray = ArrayIterator;
Iterator.TransformIterator = Iterator.transform = TransformIterator;
Iterator.StreamIterator = Iterator.fromStream = PassthroughIterator;
Iterator.PassthroughIterator = Iterator.passthrough = PassthroughIterator;
Iterator.BufferIterator = Iterator.buffered = BufferIterator;
