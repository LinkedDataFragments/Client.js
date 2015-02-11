/**
 * Created by joachimvh on 4/12/2014.
 */
/* Creates a cross join iterator for the given set of iterators. The number of results is equal to the product of the number of results for all sources. */

var Iterator = require('../iterators/Iterator'),
  MultiTransformIterator = require('../iterators/MultiTransformIterator'),
  rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  Logger = require('../util/Logger'),
  TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator');

// Creates a new GraphIterator
function CrossJoinIterator(parents, options) {
  if (!(this instanceof CrossJoinIterator))
    return new CrossJoinIterator(parents, options);
  Iterator.call(this, options);

  var self = this;
  this._crosses = [];
  _.each(parents, function (parent) {
    var cross = {source: parent, buffer: [], idx: 0};
    self._crosses.push(cross);
    parent.on('readable', function () {
      readSource(cross);
    });
  });
  this._reading = false;

  function readSource(cross) {
    var item = cross.source.read();
    if (item) {
      cross.buffer.push(item);
      self._read();
    }
  }
}
Iterator.inherits(CrossJoinIterator);

CrossJoinIterator.prototype._read = function () {
  if (_.every(this._crosses, function (cross) { return cross.source.ended && cross.buffer.length === cross.idx; }))
    this._end();

  if (_.some(this._crosses, function (cross) { return cross.buffer.length <= 0; }))
    return;

  // prevent multiple calls from using same indices
  if (this._reading)
    return;
  this._reading = true;

  var i;
  for (i = 0; i < this._crosses.length; ++i)
    this._recursiveJoin(0, i, []);
  for (i = 0; i < this._crosses.length; ++i)
    this._crosses[i].idx = this._crosses[i].buffer.length;

  this._reading = false;
};

// Joins all sources in such a way there are no missing results but also no duplicates.
CrossJoinIterator.prototype._recursiveJoin = function (idx, crossIdx, bindings) {
  if (idx >= this._crosses.length) {
    this._joinBinding(bindings);
  } else {
    var cross = this._crosses[idx];
    bindings.push(null);
    for (var i = idx < crossIdx ? 0 : cross.idx; i < cross.buffer.length; ++i) {
      bindings[idx] = cross.buffer[i];
      this._recursiveJoin(idx + 1, crossIdx, bindings);
    }
    bindings.pop();
  }
};

CrossJoinIterator.prototype._joinBinding = function (bs) {
  var binding = [];

  for (var i = 0; i < bs.length; ++i) {
    var b = bs[i];
    for (var v in b)
      binding[v] = b[v];
  }
  this._push(binding);
};

module.exports = CrossJoinIterator;
