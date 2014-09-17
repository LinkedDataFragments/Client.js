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
  this.logger = new Logger("ClusteringController");
  this.store = new RDFStoreInterface();

  // TODO: make this real code
  var self = this;
  _.each(this.clusters, function (cluster) { cluster.DEBUGcontroller = self;});
}

ClusteringController.create = function (patterns, options, callback) {
  var clusters = {};
  var nodes = [];

  var delayedCallback = _.after(_.size(patterns), function () {
    var controller = new ClusteringController(nodes, clusters);
    setImmediate(callback(controller));
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
  //var minNode = _.min(this.nodes, function (node) { return node.cost(); });
  var votes = _.object(_.map(this.clusters, function (cluster) { return [cluster.v, cluster.vote()]; }));
  var hungryVars = _.uniq(_.filter(_.invoke(this.nodes, 'waitingFor')));
  var hungryVotes = _.filter(votes, function (vote, v) { return vote && _.contains(hungryVars, v); });
  var filteredVotes = _.isEmpty(hungryVotes) ? _.filter(votes) : hungryVotes;
  var minNode = _.min(filteredVotes, function (node) { return node.cost(); });
  var votingResults = _.map(votes, function (node, v) {
    if (!node)
      return null;
    else if (node === minNode)
      return "*" + v + ":" +rdf.toQuickString(node.pattern) + "*";
    else
      return v + ":" +rdf.toQuickString(node.pattern);
  });
  this.logger.info("requested: " + hungryVars);
  this.logger.info("votes: " + votingResults);

  if (minNode === Infinity)
    return console.error('Finished, totally not a bug!');

  var minCost = minNode.cost();

  var self = this;
  if (minCost > 0)
    _.each(self.nodes, function (node) { node.spend(minCost); });

  minNode.read(function (add, remove) {
    // TODO: add triples to store
    self.store.addTriples(add, function () {
      var vars = ClusteringUtil.getVariables(minNode.pattern);
      _.each(vars, function (v) {
        var pos = minNode.getVariablePosition(v);
        self.clusters[v].removeBindings(_.filter(_.pluck(remove, pos)));
        self.clusters[v].addBindings(_.filter(_.pluck(add, pos)));
        if (minNode.ended()) {
          var bounds = _.uniq(_.pluck(minNode.triples, pos));
          self.clusters[v].addBounds(bounds);
        }
      });
    });

    // TODO: ----------- bounds ------------
    // TODO: need to do an estimation of the % matches between multiple streams, else we will always have to wait for bounds
    // TODO: don't need data from download streams if no stream is waiting on more data (except for ^)
    // TODO: don't start using bindingstreams unless we have matching values from all attached download streams?
    // TODO: disadvantage: can keep waiting, can already filter some of the results, advantage: need data from the other stream anyway

    var delayedCallback = _.after(_.size(self.clusters), function () {
      setImmediate(function () { self.read(); });
    });
    _.each(self.clusters, function (cluster) {
      cluster.update(delayedCallback);
    });
  });
};

ClusteringController.prototype.read2 = function () {
  var minNode = _.min(this.nodes, function (node) { return node.cost(); });

  if (minNode === Infinity)
    return console.error('Finished, totally not a bug!');

  var minCost = minNode.cost();

  // TODO: count unique values on page download -> more is good (/total ?)
  var self = this;
  _.each(this.nodes, function (node) {
    // TODO: problem: expensive streams, need to detect bound or not
    node.read2(minCost, function (add, remove) {
      // TODO: count each value?
      var vars = ClusteringUtil.getVariables(node.pattern);
      _.each(vars, function (v) {
        self.clusters[v].removeBindings(_.filter(_.pluck(remove, v)));
        if (node.supplies(v))
          self.clusters[v].addBindings(_.filter(_.pluck(add, v)));
        // TODO: bounds and stuff
      });
    });
  });
};

ClusteringController.prototype.getAllPaths = function (node1, node2, used, varStep) {
  if (node1 === node2 && !_.isEmpty(used))
    return [[node1]];
  used = used || [];
  used = used.concat([node1.pattern]);
  var self = this;
  var legalNeighbours = _.flatten(_.map(_.difference(ClusteringUtil.getVariables(node1.pattern), [varStep]), function (v) { return self.clusters[v].nodes; }));
  legalNeighbours = _.filter(legalNeighbours, function (node) { return !ClusteringUtil.containsObject(used, node.pattern); });
  legalNeighbours = _.uniq(legalNeighbours, function (node) { return rdf.toQuickString(node.pattern); });
  var paths = _.flatten(_.map(legalNeighbours, function (node) {
    // TODO: possibly incorrect if multiple vars match or one var occurs multiple times
    var neighbourPaths = self.getAllPaths(node, node2, used, _.first(_.difference(_.intersection(ClusteringUtil.getVariables(node1.pattern), ClusteringUtil.getVariables(node.pattern)), [varStep])));
    return _.map(neighbourPaths, function (path) { return [node1].concat(path); });
  }), true);
  return paths;
};

ClusteringController.prototype.validatePath = function (binding, path) {
  // TODO: starting from both sides probably faster
  var validBindings = [binding];
  while (!_.isEmpty(path) && !_.isEmpty(validBindings)) {
    var node = _.first(path);
    path = _.rest(path);
    validBindings = _.flatten(_.map(validBindings, function (binding) {
      // TODO: lots of double work prolly
      return _.filter(_.map(node.activeStream.triples, function (triple) {
        try { return rdf.extendBindings(binding, node.pattern, triple);}
        catch (bindingError) { return null; }
      }));
    }));
  }
  return !_.isEmpty(validBindings);
};

module.exports = ClusteringController;