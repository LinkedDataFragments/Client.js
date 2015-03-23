/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A DistinctIterator emits the unique items from a source. */

var FilterIterator = require('./FilterIterator'),
    crypto = require('crypto');

// Creates a new DistinctIterator with the given filter
function DistinctIterator(source, options) {
  if (!(this instanceof DistinctIterator))
    return new DistinctIterator(source, options);
  FilterIterator.call(this, source, null, options);
  this._uniques = {};
}
FilterIterator.inherits(DistinctIterator);

// Filters distinct items from the source
DistinctIterator.prototype._filter = function (item) {
  // An item is considered unique if its hash has not occurred before
  var uniques = this._uniques, itemHash = createHash(item);
  if (!(itemHash in uniques)) {
    uniques[itemHash] = true;
    this._push(item);
  }
};

// Creates a unique hash for the given object
function createHash(object) {
  var hash = crypto.createHash('sha1');
  hash.update(JSON.stringify(object));
  return hash.digest('base64');
}

module.exports = DistinctIterator;
