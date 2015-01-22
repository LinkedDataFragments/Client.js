/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A LimitIterator emits a given number of items from a source. */

var TransformIterator = require('./Iterator').TransformIterator;

// Creates a new LimitIterator with the given offset and limit
function LimitIterator(source, offset, limit, options) {
  if (!(this instanceof LimitIterator))
    return new LimitIterator(source, offset, limit, options);
  TransformIterator.call(this, source, options);

  this._offset = offset = isFinite(offset) ? Math.max(~~offset, 0) : 0;
  this._limit  = limit  = isFinite(limit)  ? Math.max(~~limit,  0) : Infinity;
  (limit === 0) && this._end();
}
TransformIterator.inherits(LimitIterator);

// Reads `limit` items after skipping `offset` items
LimitIterator.prototype._read = function () {
  // Read only if the limit has not been reached yet
  if (!this._reading && this._limit !== 0) {
    var source = this._source, item;
    if (source) {
      this._reading = true;
      // Skip items until the desired offset is reached
      while (this._offset !== 0) {
        if ((item = source.read()) === null)
          return this._reading = false;
        this._offset--;
      }
      // Read the next item (careful: a recursive invocation)
      if ((item = source.read()) !== null) {
        if (this._maxBufferSize > -1) this._push(item); // TODO: eliminate and check
        if (--this._limit <= 0) this._end();
      }
      this._reading = false;
    }
  }
};

module.exports = LimitIterator;
