/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** An iterator allows fast pull-based access to a stream of items. */

var EventEmitter = require('events').EventEmitter,
    util = require('util');

// Creates a new Iterator
function Iterator(options) {
  if (!(this instanceof Iterator))
    return new Iterator(options);
  EventEmitter.call(this);

  options = options || {};
  // Internal buffer of preloaded items
  this._buffer = [];
  // Desired maximum buffer size; negative means the iterator has ended
  this._bufferSize = options.bufferSize || 4;
  // Bound version of `this._push`
  this._pushBound = (function (self) { return function (i) { self._push(i); }; })(this);

  // Start filling the buffer when the first 'readable' listener is attached
  this.on('newListener', function waitForReadable(event, listener) {
    if (event === 'readable') {
      this._fillBufferOrEmitEnd();
      this.removeListener('newListener', waitForReadable);
    }
  });

  // Drain the iterator when 'data' listeners are attached
  var hasDataListener = false;
  this.on('newListener', function (event) {
    if (!hasDataListener && event === 'data') {
      var self = this;
      hasDataListener = true;
      setImmediate(function read() {
        // Try to read an item while there are still data listeners
        var item;
        while ((hasDataListener = EventEmitter.listenerCount(self, 'data') !== 0) &&
               (item = self.read()) !== null)
          self.emit('data', item);
        // If still listening, wait for the next item to arrive
        if (hasDataListener)
          self.once('readable', read);
      });
    }
  });
}
util.inherits(Iterator, EventEmitter);

// Reads an item from the iterator, or returns `null` if none is available
Iterator.prototype.read = function () {
  // Try to add a new item to the buffer if it is empty
  var item = null, buffer = this._buffer;
  if (buffer.length === 0 && this._bufferSize >= 0) {
    try { this._read(this._pushBound); }
    catch (readError) { this._error(readError); }
  }
  // Return the first item of the buffer if it exists
  if (buffer.length !== 0) {
    item = buffer.shift();
    this._fillBufferOrEmitEnd();
  }
  return item;
};

// Reads items into the buffer by calling `this._push`/`push`
Iterator.prototype._read = function (push) {
  throw new Error('The _read method has not been implemented.');
};

// Adds an item to the iterator, or ends it if `null` is pushed;
// arrival of new data can be signaled with `push()`
Iterator.prototype._push = function (item) {
  // `null` indicates the end of the stream
  if (item === null)
    return this._end();
  // a missing `item` parameter signals the iterator will become readable
  if (item === undefined)
    return this._ended || this.emit('readable');
  // negative maximum buffer size means the stream has ended
  if (this._bufferSize < 0)
    throw new Error('Cannot push because the iterator was ended.');
  this._buffer.push(item);
  this.emit('readable');
};

// Signals that no further items will become available
Iterator.prototype._end = function () {
  if (this._bufferSize >= 0) {
    this._bufferSize = -1;
    this._fillBufferOrEmitEnd();
  }
};

// Checks whether the buffer can accept new items
Iterator.prototype._hasBufferSpace = function () {
  return this._buffer.length < this._bufferSize;
};

// Fills the buffer using `_read` to speed up subsequent `read` calls
Iterator.prototype._fillBuffer = function () {
  // Keep filling as long as the length changes and is less than the maximum
  var buffer = this._buffer, prevBufferLength = -1;
  while ((prevBufferLength !== buffer.length) && this._hasBufferSpace()) {
    prevBufferLength = buffer.length;
    try { this._read && this._read(this._pushBound); }
    catch (readError) { this._error(readError); }
  }
};

// Asynchronously calls `_fillBuffer` or emits 'end'
Iterator.prototype._fillBufferOrEmitEnd = function () {
  // Emit the 'end' event if the iterator is empty
  if (this.ended)
    return setImmediate(function (self) {
      self.emit('end');
      // Remove all current and future listeners
      (function cleanup() {
        self.removeAllListeners();
        self.addListener('newListener', cleanup);
      })();
    }, this);

  // If the buffer size is below maximum, call `_fillBuffer` asynchronously
  if (!this._fillBufferPending && this._hasBufferSpace()) {
    this._fillBufferPending = true;
    setImmediate(function (self) {
      self._fillBuffer();
      self._fillBufferPending = false;
    }, this);
  }
};

// Signals that an iterator error has occurred
Iterator.prototype._error = function (error) {
  this.emit('error', error);
};

// Returns whether the iterator has no more items
Object.defineProperty(Iterator.prototype, 'ended', {
  // An iterator has ended if the buffer is empty and the maximum buffer size negative
  get: function () { return this._buffer.length === 0 && this._bufferSize < 0; },
});

// Checks whether a value has been set for the given property
Iterator.prototype.hasProperty = function (propertyName) {
  return ('_properties' in this) && (propertyName in this._properties);
};

// Gets the value of the property,
// either as return value (sync) or through the callback (async, when ready)
Iterator.prototype.getProperty = function (propertyName, callback) {
  if (this.hasProperty(propertyName)) {
    var value = this._properties[propertyName];
    return callback ? callback(value) : value;
  }
  callback && this.once(propertyName + 'Set', callback);
};

// Sets the property to the given value
Iterator.prototype.setProperty = function (propertyName, value) {
  var properties = this._properties || (this._properties = Object.create(null));
  properties[propertyName] = value;
  this.emit(propertyName + 'Set', value);
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

// Returns a cloneable version of this iterator
Iterator.prototype.toCloneable = function (options) {
  return new CloneableIterator(this, options);
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
  // Disallow all listeners
  this.on('newListener', function newListener(event) {
    this.removeAllListeners();
    this.on('newListener', newListener);
  });
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



// Creates an iterator that bases its output on another iterator or stream
function TransformIterator(source, options) {
  if (!(this instanceof TransformIterator))
    return new TransformIterator(source, options);
  Iterator.call(this, options);

  // A source value of `true` means the source will be set later
  if (source !== true) {
    // End if no iterator or stream was passed
    if (!source || typeof(source.on) !== 'function')
      return this._end();
    this.setSource(source);
  }
}
Iterator.inherits(TransformIterator);

// Sets the iterator's source
TransformIterator.prototype.setSource = function (source) {
  if (this.hasOwnProperty('_source'))
    throw new Error('Source already set.');
  this._source = source;

  // Add source listeners
  var self = this;
  source.on('error', function (error) { self._error(error); });
  source.on('end', function () { self._flush(self._pushBound); });
  // When a readable listener is attached, wait for the source to become readable
  this.on('newListener', function waitForReadable(event, listener) {
    if (event === 'readable') {
      self._fillBufferOrEmitEnd();
      source.on('readable', function () { self._fillBufferOrEmitEnd(); });
      this.removeListener('newListener', waitForReadable);
    }
  });
  if (EventEmitter.listenerCount(this, 'readable') !== 0)
    this.emit('newListener', 'readable');
  // Inherit properties from the source
  if (source.setProperty) {
    source.setProperty('hasChild', true);
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

// The default source never emits items
TransformIterator.prototype._source = WaitingIterator();

// Reads data by calling `_transform`
TransformIterator.prototype._read = function (push) {
  // Only read if we're not already transforming
  if (!this._transforming) {
    var item = this._source.read();
    if (item !== null) {
      var self = this, done = function () {
        if (done) {
          done = null;
          self._transforming = false;
          self.emit('readable');
        }
      };
      this._transforming = true;
      this._transform(item, push, done);
    }
  }
};

// Transforms data from the source iterator
TransformIterator.prototype._transform = function (item, push, done) {
  throw new Error('The _transform method has not been implemented.');
};

// Flushes remaining data after the source has ended
TransformIterator.prototype._flush = function (push) {
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



// Returns a cloneable version of the source,
// storing all elements in memory so clones can restart from the beginning
function CloneableIterator(source, options) {
  if (!(this instanceof CloneableIterator))
    return new CloneableIterator(source, options);
  PassthroughIterator.call(this, source, options);

  // Current read position on the buffer
  this._readPosition = 0;
  // Clone always directly from the original
  this.clone = this.clone.bind(this);
}
PassthroughIterator.inherits(CloneableIterator);

// Reads an item from the iterator, or returns `null` if none is available
CloneableIterator.prototype.read = function () {
  // Try to add a new item to the buffer
  var item = null, buffer = this._buffer;
  if (buffer.length === this._readPosition && this._bufferSize >= 0) {
    try { this._read(this._pushBound); }
    catch (readError) { this._error(readError); }
  }
  // Return the item at the read position if it exists
  if (buffer.length > this._readPosition) {
    item = buffer[this._readPosition++];
    this._fillBufferOrEmitEnd();
  }
  return item;
};

// Checks whether the buffer can accept new items
CloneableIterator.prototype._hasBufferSpace = function () {
  return this._buffer.length < this._readPosition + this._bufferSize;
};

// Returns whether the iterator has no more items
Object.defineProperty(CloneableIterator.prototype, 'ended', {
  // The iterator is empty when all items have been read and no more are available
  get: function () {
    return this._buffer.length === this._readPosition && this._bufferSize < 0;
  },
});

// Returns a clone of this iterator that restarts from the beginning
CloneableIterator.prototype.clone = function () {
  // Create clone through prototypal inheritance
  var parent = this, clone = Object.create(parent);
  // Handle events separately from the parent
  clone._events = {};
  clone.on('newListener', function (event, listener) {
    // Redirect all listeners except 'end' to the parent
    if (event !== 'end') {
      clone.removeAllListeners(event);
      parent.addListener(event, listener);
    }
  });
  // Start reading from the beginning
  clone._readPosition = 0;
  return clone;
};




/*             EXPORTS              */


module.exports = Iterator;
Iterator.Iterator = Iterator;
Iterator.EmptyIterator = Iterator.empty = EmptyIterator;
Iterator.SingleIterator = Iterator.single = SingleIterator;
Iterator.WaitingIterator = WaitingIterator;
Iterator.ArrayIterator = Iterator.fromArray = ArrayIterator;
Iterator.TransformIterator = Iterator.transform = TransformIterator;
Iterator.StreamIterator = Iterator.fromStream = PassthroughIterator;
Iterator.PassthroughIterator = Iterator.passthrough = PassthroughIterator;
Iterator.CloneableIterator = CloneableIterator;
