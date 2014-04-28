/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SortIterator sorts items using a sliding window. */

var WindowTransformIterator = require('./WindowTransformIterator');

// Creates a new SortIterator with the given filter
function SortIterator(source, sort, options) {
  if (!(this instanceof SortIterator))
    return new SortIterator(source, sort, options);
  // Shift arguments if `sort` is omitted
  if (typeof sort !== 'function')
    options = sort, sort = null;
  WindowTransformIterator.call(this, source, options);

  this._sort = sort || defaultSort;
}
WindowTransformIterator.inherits(SortIterator);

// Sorts the items in the sliding window by pushing the smallest first
SortIterator.prototype._transformWindow = function (items, done) {
  var length = items.length, sort = this._sort, i;
  // If this is the final window, sort and push all of its items
  if (this._source.ended) {
    items.sort(sort);
    for (i = 0; i < length; i++)
      this._push(items[i]);
    items = null;
  }
  // If this is a sliding window, push its smallest item
  else {
    var bestIndex = 0, bestItem = items[0];
    for (i = 1; i < length; i++)
      if (sort(items[i], bestItem) < 0)
        bestIndex = i, bestItem = items[i];
    this._push(bestItem);
    items.splice(bestIndex, 1);
  }
  done(items);
};

// Default sorting function
function defaultSort (a, b) {
  if (a < b) return -1;
  if (a > b) return  1;
  return 0;
}

module.exports = SortIterator;
