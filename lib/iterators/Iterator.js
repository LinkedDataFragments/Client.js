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
}
util.inherits(Iterator, EventEmitter);

// Reads an item from the iterator, or returns `null` if none is available
Iterator.prototype.read = function () {
  var item = null;
  // If there are still items in the buffer, retrieve the first and refill
  if (this._buffer.length !== 0) {
    item = this._buffer.shift();
    this._fillBufferOrEmitEnd();
  }
  // If the iterator has not ended, try to read an item
  if (this._bufferSize >= 0) {
    try { this._read(); }
    catch (readError) { this._error(readError); }
    // If successful, retrieve it and refill the buffer
    if (this._buffer.length !== 0) {
      item = this._buffer.shift();
      this._fillBufferOrEmitEnd();
    }
  }
  return item;
};

// Reads from the buffer by calling `_push` on each item
Iterator.prototype._read = function () {
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
    return setImmediate(function (self) { !self._ended && self.emit('readable'); }, this);
  // negative maximum buffer size means the stream has ended
  if (this._bufferSize < 0)
    throw new Error('Cannot push because the iterator was ended.');
  this._buffer.push(item);
};

// Signals that no further items will become available
Iterator.prototype._end = function () {
  if (this._bufferSize >= 0) {
    this._bufferSize = -1;
    this._fillBufferOrEmitEnd();
  }
};

// Fills the buffer using `_read` to speed up subsequent `read` calls
Iterator.prototype._fillBuffer = function () {
  var buffer = this._buffer, prevBufferLength = -1;
  while ((prevBufferLength !== buffer.length) && (buffer.length < this._bufferSize)) {
    prevBufferLength = buffer.length;
    try { this._read(); }
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
  if ((this._buffer.length < this._bufferSize) && !this._fillBufferPending) {
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


// Returns whether the iterator has no more elements
Object.defineProperty(Iterator.prototype, 'ended', {
  // An iterator has ended if the buffer is empty and the maximum buffer size negative
  get: function () { return this._buffer.length === 0 && this._bufferSize < 0; },
});




/*    PRE-DEFINED ITERATOR TYPES    */


// Creates an empty iterator
function EmptyIterator() {
  if (!(this instanceof EmptyIterator))
    return new EmptyIterator();
  Iterator.call(this);
  this._end();
}
util.inherits(EmptyIterator, Iterator);



// Creates an iterator from an array
function ArrayIterator(array) {
  if (!(this instanceof ArrayIterator))
    return new ArrayIterator(array);
  Iterator.call(this);

  // Push all elements of the array and end the iterator
  var length = (array && array.length) | 0;
  if (length > 0) {
    var buffer = this._buffer = new Array(length);
    for (var i = 0; i < length; i++)
      buffer[i] = array[i];
  }
  this._end();
}
util.inherits(ArrayIterator, Iterator);



// Creates an iterator from a stream
function StreamIterator(stream) {
  if (!(this instanceof StreamIterator))
    return new StreamIterator(stream);
  Iterator.call(this);

  // If no stream was passed, just end
  if (!stream || typeof(stream.on) !== 'function')
    return this._end();
  // Otherwise, listen to the stream's events
  this._stream = stream;
  stream.on('end', this._end.bind(this));
  stream.on('error', this._error.bind(this));
}
util.inherits(StreamIterator, Iterator);

// Tries to read a chunk from the stream
StreamIterator.prototype._read = function () {
  // If an item was ready, push it
  var item = this._stream.read();
  if (item !== null) return this._push(item);
  // If not, wait for the stream to become readable
  return this._stream.once('readable', this._push.bind(this));
};




/*             EXPORTS              */

module.exports = Iterator;
Iterator.Iterator = Iterator;
Iterator.EmptyIterator = Iterator.empty = EmptyIterator;
Iterator.ArrayIterator = Iterator.fromArray = ArrayIterator;
Iterator.StreamIterator = Iterator.fromStream = StreamIterator;
