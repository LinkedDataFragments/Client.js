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
  //this.logger.disable();
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
        node.DEBUGclusters[v] = clusters[v];
      });
      delayedCallback();
    });
  });
};

ClusteringController.prototype.start = function () {
  // start with best node to make sure supply gets called at least once
  var minNode = _.min(this.nodes, function (node) { return node.cost(); });

  // only minNode starts as download stream, rest becomes bound
  // TODO: won't work for unconnected parts
  var varsToUpdate = ClusteringUtil.getVariables(minNode.pattern);
  var varsDone = [];
  while (!_.isEmpty(varsToUpdate)) {
    var v = varsToUpdate.shift();
    varsDone.push(v);
    var newVars = [];
    _.each(_.sortBy(this.clusters[v].nodes, function (node) { return node.fullStream.count; }), function (node) {
      // haven't updated this node yet
      if (node.activeStream === node.fullStream && node !== minNode && !node.fixed) {
        // TODO: this shouldn't be necessary, already create all binding streams?
        node.bindStreams[v] = new Stream.BindingStream(node.fullStream.count, node.pattern, v, node._options);
        node.activeStream = node.bindStreams[v];
        newVars = newVars.concat(ClusteringUtil.getVariables(node.pattern));
      }
    });
    newVars = _.uniq(newVars);
    newVars = _.difference(newVars, varsDone);
    varsToUpdate = _.union(varsToUpdate, newVars);
  }

  _.each(this.nodes, function (node) {
    node.logger.info("initial bindVar: " + node.activeStream.bindVar);
  });

  this.readNode(minNode);
};

ClusteringController.prototype.read = function () {
  //var minNode = _.min(this.nodes, function (node) { return node.cost(); });
//  var votes = _.object(_.map(this.clusters, function (cluster) { return [cluster.v, cluster.vote()]; }));
//  var hungryVotes = _.filter(votes, function (vote, v) { return vote && _.contains(hungryVars, v); });
//  var filteredVotes = _.isEmpty(hungryVotes) ? _.filter(votes) : hungryVotes;
//  var minNode = _.min(filteredVotes, function (node) { return node.cost(); });

//  var hungryVars = []; //_.uniq(_.filter(_.invoke(this.nodes, 'waitingFor')));
//  var hungryClusters = _.filter(this.clusters, function (cluster) { return _.contains(hungryVars, cluster.v); });
//  if (_.isEmpty(hungryClusters))
//    hungryClusters = this.clusters;
  var votes = {};
  var self = this;
  var delayedCallback = _.after(_.size(this.clusters), function () {
    votes = _.omit(votes, _.isNull);
    // TODO: find a solution for these
//    var nonSupplyNodes = _.filter(self.nodes, function (node) { return _.isEmpty(node.activeSupplyVars()) && !node.ended() && !(node.activeStream.isHungry && node.activeStream.isHungry()); });
//    _.each(nonSupplyNodes, function (node, idx) { votes[idx] = node; });
    // binding streams have priority (since the values they get should have been thoroughly checked already
    // TODO: binding streams have priority over their suppliers, not over all other nodes
    var boundNodes = _.omit(votes, function (node) { return !node.activeStream.feed; });
    var suppliers = _.flatten(_.map(boundNodes, function (node) {
      if (node.activeStream.bindVar) return _.pluck(self.clusters[node.activeStream.bindVar].suppliers(), 'pattern');
    }));
    // TODO: is it possible we end up with an empty list?
    var candidates = _.filter(votes, function (node) { return !ClusteringUtil.containsObject(suppliers, node.pattern); });
    var minNode = _.min(candidates, function (node) { return node.cost(); });

    var votingResults = _.map(votes, function (node, v) {
      if (!node)
        return null;

      var patternStr = v + ":" + rdf.toQuickString(node.pattern) + "(" + node.activeStream.bindVar + ")";

      if (node === minNode)
        patternStr = "*" + patternStr + "*";
      return patternStr;
    });
    //self.logger.info("requested: " + hungryVars);
    self.logger.info("votes: " + votingResults);

    if (minNode === Infinity)
      return self.logger.info('Finished, totally not a bug!');
    else
      self.readNode(minNode);
  });
  _.each(this.clusters, function (cluster) {
    cluster.vote(function (node) {
      votes[cluster.v] = node;
      delayedCallback();
    });
  });
};

// TODO: start a read while we are updating? (since we need to wait on http response anyway
ClusteringController.prototype.readNode = function (minNode) {
  var minCost = minNode.cost();
  var self = this;
  self.logger.info("cost: " + minCost);
  if (minCost > 0)
    _.each(self.nodes, function (node) { node.spend(minCost); });
  minNode.read(function (add, remove) {
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


    // TODO: switching to download stream here, maybe I can do this on a per cluster basis? (not sure if safe, should check)
    // TODO: maybe also move switching binding streams?

    var delayedCallback = _.after(_.size(self.clusters), function () {
      // only switch to download stream if we are sure it is for the best
      // TODO: estimate can be wonky at start if there are multiple streams, need better value
      // TODO: not sure of best time yet, need to be after supply to have estimates?
      // start with the cheapest node and continue until we find an acceptable switch (or no nodes are left)
      _.some(_.sortBy(_.filter(self.nodes, function (node) { return node.activeStream.bindVar; }), function (node) { return node.fullStream.count; }), function (node) {
        var v = node.activeStream.bindVar; // TODO: will this always be the correct choice?
        // value will be infinite if no values have been matched yet
        if (_.isFinite(self.clusters[v].estimate) && self.clusters[v].estimate > node.fullStream.cost) {
          node.logger.info("SWITCH STREAM " + v + " -> " + undefined);
          node.activeStream = node.fullStream;
          return true; // found a match, wait until next iteration to try again
        }
      });

      //console.time("END CHECK");
      // TODO: DEBUG let's see how many results we have
      self.store.matchBindings(_.pluck(self.nodes, 'pattern'), function (results) {
        //console.timeEnd("END CHECK");
        self.logger.info("COMPLETE MATCHES: " + _.size(results));
        setImmediate(function () { self.read(); });
      });
    });
    _.each(self.clusters, function (cluster) {
      cluster.update(delayedCallback);
    });
  });
};

//ClusteringController.prototype.read2 = function () {
//  var minNode = _.min(this.nodes, function (node) { return node.cost(); });
//
//  if (minNode === Infinity)
//    return console.error('Finished, totally not a bug!');
//
//  var minCost = minNode.cost();
//
//  // TODO: count unique values on page download -> more is good (/total ?)
//  var self = this;
//  _.each(this.nodes, function (node) {
//    // TODO: problem: expensive streams, need to detect bound or not
//    node.read2(minCost, function (add, remove) {
//      // TODO: count each value?
//      var vars = ClusteringUtil.getVariables(node.pattern);
//      _.each(vars, function (v) {
//        self.clusters[v].removeBindings(_.filter(_.pluck(remove, v)));
//        if (node.supplies(v))
//          self.clusters[v].addBindings(_.filter(_.pluck(add, v)));
//        // TODO: bounds and stuff
//      });
//    });
//  });
//};

ClusteringController.prototype.getAllPaths = function (node1, node2, varsUsed, used) {
  if (node1 === node2 && !_.isEmpty(used))
    return [[node1]];
  used = used || [];
  used = used.concat([node1.pattern]);
  var self = this;
  var legalNeighbours = _.flatten(_.map(_.difference(ClusteringUtil.getVariables(node1.pattern), varsUsed), function (v) { return self.clusters[v].nodes; }));
  legalNeighbours = _.filter(legalNeighbours, function (node) { return !ClusteringUtil.containsObject(used, node.pattern); });
  legalNeighbours = _.uniq(legalNeighbours, function (node) { return rdf.toQuickString(node.pattern); });
  var paths = _.flatten(_.map(legalNeighbours, function (node) {
    // TODO: possibly incorrect if multiple vars match or one var occurs multiple times
    var v = _.first(_.difference(_.intersection(ClusteringUtil.getVariables(node1.pattern), ClusteringUtil.getVariables(node.pattern)), varsUsed));
    var neighbourPaths = self.getAllPaths(node, node2, varsUsed.concat([v]), used);
    return _.map(neighbourPaths, function (path) { return [node1].concat(path); });
  }), true);
  return paths;
};

//ClusteringController.prototype.validatePath = function (binding, path) {
//  // TODO: starting from both sides probably faster
//  var validBindings = [binding];
//  while (!_.isEmpty(path) && !_.isEmpty(validBindings)) {
//    var node = _.first(path);
//    path = _.rest(path);
//    validBindings = _.flatten(_.map(validBindings, function (binding) {
//      // TODO: lots of double work prolly
//      return _.filter(_.map(node.activeStream.triples, function (triple) {
//        try { return rdf.extendBindings(binding, node.pattern, triple);}
//        catch (bindingError) { return null; }
//      }));
//    }));
//  }
//  return !_.isEmpty(validBindings);
//};

module.exports = ClusteringController;