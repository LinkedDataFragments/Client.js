/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A LimitIterator emits a given number of items from a source. */

var TransformIterator = require('./Iterator').TransformIterator;

// Creates a new LimitIterator with the given offset and limit
function LimitIterator(source, offset, limit, options) {
  if (!(this instanceof LimitIterator))
    return new LimitIterator(source, options);
  TransformIterator.call(this, source, options);

  this._offset = offset = isFinite(offset) ? Math.max(~~offset, 0) : 0;
  this._limit  = limit  = isFinite(limit)  ? Math.max(~~limit,  0) : Infinity;
  (limit === 0) && this._end();
}
TransformIterator.inherits(LimitIterator);

// Reads `limit` items after skipping `offset` items
LimitIterator.prototype._read = function () {
  // Read items as long as the limit has not been reached
  if (this._limit !== 0) {
    var source = this._source, item;
    if (source) {
      // Skip items until the desired offset is reached
      while (this._offset !== 0) {
        if ((item = source.read()) === null) return;
        this._offset--;
      }
      // Read the next item
      if ((item = source.read()) !== null) {
        this._limit--;
        this._push(item);
      }
    }
  }
  // End if there are no more items left
  else this._end();
};

module.exports = LimitIterator;
