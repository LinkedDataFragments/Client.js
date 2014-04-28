/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A DistinctIterator emit unique items from a source. */

var WindowTransformIterator = require('./WindowTransformIterator'),
    _ = require('underscore');

// Creates a new DistinctIterator with the given filter
function DistinctIterator(source, options) {
  if (!(this instanceof DistinctIterator))
    return new DistinctIterator(source, options);
  WindowTransformIterator.call(this, source, options);
}
WindowTransformIterator.inherits(DistinctIterator);

// Pushes distinct items in the transform window
DistinctIterator.prototype._transformWindow = function (items, done) {
  var i, j, item, length = items.length, remaining = [];
  // If this is the final window, push all unique items
  if (this._source.ended) {
    for (i = 0; item = items[i], i < length; i++) {
      for (j = 0; item !== null && j < remaining.length; j++)
        _.isEqual(item, remaining[j]) && (item = null);
      if (item !== null)
        this._push(item), remaining.push(item);
    }
    remaining = null;
  }
  // If this is a sliding window, push the first item and eliminate duplicates
  else {
    for (i = 1, item = items[0]; i < length; i++)
      if (!_.isEqual(item, items[i]))
        remaining.push(items[i]);
    this._push(item);
  }
  done(remaining);
};

module.exports = DistinctIterator;
