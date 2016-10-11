/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* A DistinctIterator emits the unique items from a source. */

var SimpleTransformIterator = require('asynciterator').SimpleTransformIterator,
    crypto = require('crypto');

// Creates a new DistinctIterator with the given filter
function DistinctIterator(source, options) {
  if (!(this instanceof DistinctIterator))
    return new DistinctIterator(source, options);
  SimpleTransformIterator.call(this, source, options);
  this._uniques = Object.create(null);
}
SimpleTransformIterator.subclass(DistinctIterator);

// Filters distinct items from the source
DistinctIterator.prototype._filter = function (item) {
  // An item is considered unique if its hash has not occurred before
  var uniques = this._uniques, itemHash = this._hash(item);
  if (!(itemHash in uniques)) {
    uniques[itemHash] = true;
    this._push(item);
  }
};

// Creates a unique hash for the given item
DistinctIterator.prototype._hash = function (item) {
  var hash = crypto.createHash('sha1');
  hash.update(JSON.stringify(item));
  return hash.digest('base64');
};

module.exports = DistinctIterator;
