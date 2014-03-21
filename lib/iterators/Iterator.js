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

// Adds an item to the iterator, or ends it if `null` is pushed
Iterator.prototype._push = function (item) {
  if (item === null || item === undefined)
    return this._end();
  if (this._bufferSize < 0)
    throw new Error('Cannot push because the iterator was ended.');
  this._buffer.push(item);
};

// Signals that no further items will be emitted
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

module.exports = Iterator;
