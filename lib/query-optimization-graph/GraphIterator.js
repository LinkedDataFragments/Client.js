/**
 * Created by joachimvh on 4/12/2014.
 */
/* Iterator wrapper for ClusteringController. Handles the splitting of unconnected patterns and possibly adding already existing bindings. */

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
    return new Iterator.PassthroughIterator(parent, options);
  // A one-element pattern can be solved by a triple pattern iterator
  if (patterns.length === 1)
    return new TriplePatternIterator(parent, patterns[0], options);
  if (!(this instanceof GraphIterator))
    return new GraphIterator(parent, patterns, options);

  // If we have unconnected patterns, combine them with a CrossJoinIterator
  var clusteredPatterns = rdf.findConnectedPatterns(patterns);
  if (clusteredPatterns.length > 1) {
    var parents = [];
    for (var i = 0; i < clusteredPatterns.length; ++i)
      parents.push(new GraphIterator(parent.clone(), clusteredPatterns[i], options));
    return new CrossJoinIterator(parents, options);
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
  ClusteringController.create(boundPatterns, options, function (controller) {
    controller.start();
    pipeline.setSource(controller);
  });
  return pipeline;
};

module.exports = GraphIterator;
