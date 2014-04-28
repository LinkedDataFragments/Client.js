/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A WindowTransformIterator transforms source items in sliding windows with given length. */

var TransformIterator = require('./Iterator').TransformIterator;

// Creates a new WindowTransformIterator with the given filter
function WindowTransformIterator(source, options) {
  if (!(this instanceof WindowTransformIterator))
    return new WindowTransformIterator(source, options);
  TransformIterator.call(this, source, options);

  var window = options && options.window;
  this._windowLength = isFinite(window) && window > 0 ? ~~window : Infinity;
  this._transformBuffer = [];
}
TransformIterator.inherits(WindowTransformIterator);

// Reads a transformed item from the iterator
WindowTransformIterator.prototype._read = function () {
  // Only read if we're not already transforming
  if (this._transformBuffer !== null) {
    // Try to fill the buffer until the desired window length has been read
    var item, source = this._source, buffer = this._transformBuffer, self = this;
    while (!source.ended && buffer.length < this._windowLength) {
      if ((item = source.read()) === null) return;
      buffer.push(item);
    }
    // If the buffer is full, transform the window
    if (buffer.length === this._windowLength || source.ended) {
      // End if the buffer is empty
      if (buffer.length === 0) return this._end();
      // Invalidate the buffer and transform it
      this._transformBuffer = null;
      this._transformWindow(buffer, function done(remainingItems) {
        if (self) {
          self._transformBuffer = remainingItems || [];
          self.emit('readable', self = null);
        }
      });
    }
  }
};

// Transforms the items, adding transformed items through `this._push`
// and returning the remaining items through the `done` callback
WindowTransformIterator.prototype._transformWindow = function (items, done) {
  throw new Error('The _transformWindow method has not been implemented.');
};

module.exports = WindowTransformIterator;
