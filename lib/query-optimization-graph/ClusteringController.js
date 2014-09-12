/**
 * Created by joachimvh on 11/09/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
  Iterator = require('../iterators/Iterator'),
  MultiTransformIterator = require('../iterators/MultiTransformIterator'),
  Logger = require ('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil'),
  RDFStoreInterface = require('./RDFStoreInterface'),
  Stream = require('./Stream'),
  Cluster = require('./Cluster'),
  Node = require('./Node');

function ClusteringController (nodes, clusters) {
  this.clusters = clusters;
  this.nodes = nodes;
}

ClusteringController.create = function (patterns, options, callback) {
  var clusters = {};
  var nodes = [];

  var delayedCallback = _.after(_.size(patterns), function () {
    var controller = new ClusteringController(nodes, clusters);
    callback(controller);
  });

  _.each(patterns, function (pattern) {
    var fragment = options.fragmentsClient.getFragmentByPattern(pattern);
    fragment.getProperty('metadata', function(metadata) {
      fragment.close();
      var node = new Node(pattern, metadata.totalTriples, options);
      nodes.push(node);
      var vars = ClusteringUtil.getVariables(pattern);
      _.each(vars, function (v) {
        clusters[v] = clusters[v] || new Cluster(v);
        clusters[v].nodes.push(node);
      });
      delayedCallback();
    });
  });
};

ClusteringController.prototype.read = function () {
  var minNode = _.min(this.nodes, function (node) { return node.cost(); });

  if (minNode === Infinity)
    return console.error('Finished, totally not a bug!');

  _.each(this.nodes, function (node) {
    console.error (rdf.toQuickString(node.pattern) + " costs " + node.cost());
  });

  var minCost = minNode.cost();

  var self = this;
  if (minCost > 0)
    _.each(self.nodes, function (node) { node.spend(minCost); });

  minNode.read(function (add, remove) {
    // TODO: add triples to store

    var vars = ClusteringUtil.getVariables(minNode.pattern);
    _.each(vars, function (v) {
      self.clusters[v].removeBindings(_.filter(_.pluck(remove, v)));
      self.clusters[v].addBindings(_.filter(_.pluck(add, v)));
    });

    // TODO: bounds

    var delayedCallback = _.after(_.size(self.clusters), function () {
      setImmediate(function () { self.read(); });
    });
    _.each(self.clusters, function (cluster) {
      cluster.update(delayedCallback);
    });
  });
};

module.exports = ClusteringController;