/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A DistinctIterator emits the unique items from a source. */

var FilterIterator = require('./FilterIterator'),
    isEqual = require('lodash').isEqual;

// Creates a new DistinctIterator with the given filter
function DistinctIterator(source, options) {
  if (!(this instanceof DistinctIterator))
    return new DistinctIterator(source, options);
  FilterIterator.call(this, source, options);

  // The `window` parameter indicates how much items to keep for uniqueness testing
  var window = options && options.window;
  this._windowLength = isFinite(window) && window > 0 ? ~~window : Infinity;
  this._uniques = [];
}
FilterIterator.inherits(DistinctIterator);

// Filters distinct items from the source
DistinctIterator.prototype._filter = function (item) {
  // Reject the item if it is already in the list of uniques
  var uniques = this._uniques, length = uniques.length, i = length;
  while (i !== 0)
    if (isEqual(item, uniques[--i]))
      return false;
  // Shorten the list of uniques if we reach the window length
  (length === this._windowLength) && uniques.shift();
  // Add the item to the list of uniques
  return uniques.push(item);
};

module.exports = DistinctIterator;
