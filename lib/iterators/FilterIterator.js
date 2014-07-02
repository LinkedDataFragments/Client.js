/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A FilterIterator filters the results of another iterator. */

var TransformIterator = require('./Iterator').TransformIterator;

// Creates a new FilterIterator with the given filter
function FilterIterator(source, filter, options) {
  if (!(this instanceof FilterIterator))
    return new FilterIterator(filter, options);
  TransformIterator.call(this, source, options);

  if (typeof filter === 'function')
    this._filter = filter;
}
TransformIterator.inherits(FilterIterator);

// Reads the items from the source that match the filter
FilterIterator.prototype._read = function () {
  var source = this._source;
  if (source) {
    // Try reading items until one matches the filter
    var item;
    do { if ((item = source.read()) === null) return; }
    while (!this._filter(item));
    // Push a matching item
    this._push(item);
  }
};

// Returns whether the item should be emitted by the iterator
FilterIterator.prototype._filter = function (item) {
  throw new Error('The _filter method has not been implemented.');
};

module.exports = FilterIterator;
