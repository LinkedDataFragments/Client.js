/**
 * Created by joachimvh on 4/12/2014.
 */

var Iterator = require('../iterators/Iterator'),
  MultiTransformIterator = require('../iterators/MultiTransformIterator'),
  rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  Logger = require('../util/Logger'),
  TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
  CrossJoinIterator = require('./CrossJoinIterator'),
  ClusteringController = require('./ClusteringController');

// Creates a new GraphIterator
function GraphIterator(parent, patterns, options) {
  // Empty patterns have no effect; return a pass-through iterator
  if (!patterns || !patterns.length)
    return new Iterator.passthrough(parent, options);
  // A one-element pattern can be solved by a triple pattern iterator
  if (patterns.length === 1)
    return new TriplePatternIterator(parent, patterns[0], options);
  if (!(this instanceof GraphIterator))
    return new GraphIterator(parent, patterns, options);

  // If we have unconnected patterns, combine them with a CrossJoinIterator
  var clusteredPatterns = rdf.findConnectedPatterns(patterns);
  if (clusteredPatterns.length > 1) {
    var leftPatterns = clusteredPatterns.pop();
    var rightPatterns = _.flatten(clusteredPatterns);
    return new CrossJoinIterator(new GraphIterator(parent, leftPatterns, options), new GraphIterator(parent, rightPatterns, options), options);
  }
  MultiTransformIterator.call(this, parent, options);

  this._patterns = patterns;
  this._client = this._options.fragmentsClient;
}
MultiTransformIterator.inherits(GraphIterator);

GraphIterator.prototype._createTransformer = function (binding, options) {
  // binding will usually be an empty binding
  var boundPatterns = rdf.applyBindings(binding, this._patterns);
  var pipeline = new Iterator.PassthroughIterator(true);
  var self = this;
  ClusteringController.create(boundPatterns, options, function (controller) {
    controller.start(function (results, finished) {
      self._buffer = self._buffer.concat(results);
      if (results.length > 0)
        self.emit('readable');
      if (finished)
        self.emit('end');
    });
    pipeline.setSource(self);
  });
  return pipeline;
};
GraphIterator.prototype._readTransformer = function () {
  if (this._buffer.length <= 0)
    return null;
  return this._buffer.pop();
};

module.exports = GraphIterator;
