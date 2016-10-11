/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* A SortIterator emits the items of a source in a sorted way. */

var TransformIterator = require('asynciterator').TransformIterator;

// Creates a new SortIterator with the given filter
function SortIterator(source, sort, options) {
  if (!(this instanceof SortIterator))
    return new SortIterator(source, sort, options);
  // Shift arguments if `sort` is omitted
  if (typeof sort !== 'function')
    options = sort, sort = null;
  TransformIterator.call(this, source, options);

  // The `window` parameter indicates the length of the sliding window to apply sorting
  var window = options && options.window;
  this._windowLength = isFinite(window) && window > 0 ? ~~window : Infinity;
  this._sort = sort || defaultSort;
  this._sorted = [];
}
TransformIterator.subclass(SortIterator);

// Reads the smallest item in the current sorting window
SortIterator.prototype._read = function (count, done) {
  var item, sorted = this._sorted, source = this._source, length = sorted.length;
  if (source) {
    // Try to read items until we reach the desired window length
    while (length !== this._windowLength && (item = source.read()) !== null) {
      // Insert the item in the sorted window (smallest last)
      var left = 0, right = length - 1, mid, order;
      while (left <= right) {
        order = this._sort(item, sorted[mid = (left + right) >> 1]);
        if      (order < 0) left  = mid + 1;
        else if (order > 0) right = mid - 1;
        else left = mid, right = -1;
      }
      sorted.splice(left, 0, item), length++;
    }
    // Push the smallest item in the window
    (length === this._windowLength) && this._push(sorted.pop());
  }
  done();
};

// Flushes remaining data after the source has ended
SortIterator.prototype._flush = function (done) {
  var sorted = this._sorted, length = sorted.length;
  while (length--)
    this._push(sorted.pop());
  done();
};

// Default sorting function
function defaultSort(a, b) {
  if (a < b) return -1;
  if (a > b) return  1;
  return 0;
}

module.exports = SortIterator;
