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
    return new CrossJoinIterator(new GraphIterator(parent.clone(), leftPatterns, options), new GraphIterator(parent.clone(), rightPatterns, options), options);
  }
  MultiTransformIterator.call(this, parent, options);

  this._patterns = patterns;
  this._client = this._options.fragmentsClient;
  this._count = 0;
  this._controller = null;
  this._data = [];
}
MultiTransformIterator.inherits(GraphIterator);

GraphIterator.prototype._createTransformer = function (binding, options) {
  // binding will usually be an empty binding
  var boundPatterns = rdf.applyBindings(binding, this._patterns);
  var pipeline = new Iterator.PassthroughIterator(true);
  var self = this;
  ClusteringController.create(boundPatterns, options, function (controller) {
    self.controller = controller;
    controller.start(function (results, finished) {
      self._data = self._data.concat(results);
      /*for (var i = 0; i < results.length; ++i) {
        pipeline._push(results[i]);
      }*/
      self._count += results.length;
      if (results.length > 0) {
        self.controller.pause();
        self.emit('readable');
      }
      if (finished)
        pipeline._end();
    });
    //pipeline.setSource(self);
  });
  return pipeline;
};

GraphIterator.prototype._readTransformer = function (transformer, item) {
  //return transformer.read();
  if (this._data.length <= 0) {
    this.controller && this.controller.resume();
    return null;
  }
  
  return this._data.pop();
};

module.exports = GraphIterator;
