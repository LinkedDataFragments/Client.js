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
  Iterator.call(this, options);

  this._parent2 = parent2;
  this._buffer = [];
  this._buffer1;
  this._buffer2;
  this._idx1 = 0;
  this._idx2 = 0;
  var self = this;
  parent1.toArray(function (error, data) {
    self._buffer1 = data;
    if (self._buffer1 && self._buffer2)
      self.emit('readable');
  });
  parent2.toArray(function (error, data) {
    self._buffer2 = data;
    if (self._buffer1 && self._buffer2)
      self.emit('readable');
  });
}
Iterator.inherits(CrossJoinIterator);

CrossJoinIterator.prototype._read = function () {
  if (!this._buffer1 || !this._buffer2)
    return;

  if (this._buffer1.length === 0 || this._buffer2.length === 0 || this._idx2 >= this._buffer2.length) {
    this._end();
    return;
  }

  var b1 = this._buffer1[this._idx1++];
  var b2 = this._buffer2[this._idx2];

  if (this._idx1 >= this._buffer1.length) {
    this._idx1 = 0;
    this._idx2++;
  }

  var binding = [];
  var v;
  for (v in b1)
    binding[v] = b1[v];
  for (v in b2)
    binding[v] = b2[v];
  this._push(binding);
}

module.exports = CrossJoinIterator;
