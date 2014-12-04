/**
 * Created by joachimvh on 4/12/2014.
 */

var Iterator = require('../iterators/Iterator'),
  MultiTransformIterator = require('../iterators/MultiTransformIterator'),
  rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  Logger = require('../util/Logger'),
  TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator');

// Creates a new GraphIterator
function CrossJoinIterator(parent1, parent2, options) {
  if (!(this instanceof CrossJoinIterator))
    return new CrossJoinIterator(parent1, parent2, options);
  MultiTransformIterator.call(this, parent1, options);

  this._parent2 = parent2;
  this._buffer = [];
}
MultiTransformIterator.inherits(CrossJoinIterator);

CrossJoinIterator.prototype._createTransformer = function (binding1, options) {
  var parent2 = this._parent2.clone();
  return new SingleCrossJoinIterator(binding1, parent2.clone());
};

function SingleCrossJoinIterator(binding1, parent2, options) {
  MultiTransformIterator.call(this, parent2, options);
  this._binding1 = binding1;
}
MultiTransformIterator.inherits(SingleCrossJoinIterator);

SingleCrossJoinIterator.prototype._createTransformer = function (binding2, options) {
  var binding = [];
  var v;
  for (v in this._binding1)
    binding[v] = this._binding1[v];
  for (v in binding2)
    binding[v] = binding2[v];

  return Iterator.SingleIterator(binding);
};

// Reads a binding from the given fragment
SingleCrossJoinIterator.prototype._readTransformer = function () {
  return this._buffer.pop();
};

module.exports = CrossJoinIterator;