/**
 * Created by joachimvh on 11/09/2014.
 */
/* The main class that controls the algorithm run. Extends Iterator (but GraphIterator is necessary to fully fit in the entire program). */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  Logger = require('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil'),
  Cluster = require('./Cluster'),
  Node = require('./Node'),
  CachedTripleStore = require('./CachedTripleStore'),
  Iterator = require('../iterators/Iterator');

function ClusteringController(nodes, clusters, options) {
  Iterator.call(this, options);
  nodes = _.sortBy(nodes, function (node) { return rdf.toQuickString(node.pattern); }); // easier debugging if they are always in the same order here
  this.clusters = clusters;
  this.nodes = nodes;
  this.resultCount = 0;
  this.logger = new Logger("ClusteringController");
  this.logger.disable();
  this.store = new CachedTripleStore();
  this.running = true;
  for (var i = 0; i < nodes.length; ++i)
    nodes[i].controller = this;
}
Iterator.inherits(ClusteringController);

// Creates a ClusteringController object. Static function is required since we need to wait on the size metadata before the object can be created.
// We assume the patterns to be connected at this point!
ClusteringController.create = function (patterns, options, callback) {
  var clusters = {};
  var nodes = [];

  var delayedCallback = _.after(patterns.length, function () {
    var controller = new ClusteringController(nodes, clusters, options);
    setImmediate(callback(controller));
  });

  _.each(patterns, function (pattern) {
    var fragment = options.fragmentsClient.getFragmentByPattern(pattern);
    fragment.getProperty('metadata', function (metadata) {
      fragment.close();
      var node = new Node(pattern, metadata.totalTriples, options);
      nodes.push(node);
      var vars = rdf.getVariables(pattern);
      _.each(vars, function (v) {
        clusters[v] = clusters[v] || new Cluster(v);
        clusters[v].nodes.push(node);
      });
      delayedCallback();
    });
  });
};

// Initializes the ClusteringController by assigning roles to every node and starts the run.
ClusteringController.prototype.start = function () {
  // start with best node to make sure supply gets called at least once
  var minNode = _.min(this.nodes, function (node) { return node.fullStream.cost; });

  // only minNode starts as download stream, rest becomes bound first
  var varsToUpdate = rdf.getVariables(minNode.pattern);
  var varsDone = [];
  var parsedNodes = [minNode];
  while (varsToUpdate.length > 0) {
    var v = varsToUpdate.shift();
    varsDone.push(v);
    var newVars = [];
    _.each(_.sortBy(this.clusters[v].nodes, function (node) { return node.fullStream.count; }), function (node) {
      // haven't updated this node yet
      if (node.activeStream === node.fullStream && node !== minNode && !node.fixed) {
        node.switchStream(v);
        newVars = newVars.concat(rdf.getVariables(node.pattern));
        parsedNodes.push(node);
      }
    });
    newVars = _.uniq(newVars);
    newVars = _.difference(newVars, varsDone);
    varsToUpdate = _.union(varsToUpdate, newVars);
  }

  var self = this;
  var changed = true;
  while (changed) {
    changed = false;
    _.each(this.nodes, function (node) {
      var bindVar = node.waitingFor();
      if (!bindVar)
        return;
      var suppliers = self.clusters[bindVar].suppliers();
      // TODO: this 100 actually should not be changed by pagesize, the reasoning to come to this result was wrong and should be re-checked
      var full = _.every(suppliers, function (supplier) { return node.fullStream.count < supplier.fullStream.count / 100; });
      changed |= full;
      if (full) {
        node.switchStream(null);
      } else {
        var minVar = _.min(rdf.getVariables(node.pattern), function (v) {
          var suppliers = _.without(self.clusters[v].suppliers(), node);
          return _.min(_.map(suppliers, function (supplier) { return supplier.fullStream.count; }));
        });
        var switchBindVar = minVar !== bindVar;
        changed |= switchBindVar;
        if (switchBindVar)
          node.switchStream(minVar);
      }
    });
  }

  // order the nodes so dependencies are in the correct order
  parsedNodes = _.sortBy(_.filter(this.nodes, function (node) { return node.activeStream === node.fullStream; }), function (node) { return node.fullStream.count; });
  var vars = _.union.apply(null, _.map(parsedNodes, function (node) { return rdf.getVariables(node.pattern); }));
  while (parsedNodes.length < this.nodes.length) {
    var remaining = _.filter(this.nodes, function (node) { return !_.contains(parsedNodes, node) && _.intersection(vars, rdf.getVariables(node.pattern)).length > 0;  });
    var best = _.min(remaining, function (node) { return node.fullStream.count; });
    parsedNodes.push(best);
    vars = _.union(vars, rdf.getVariables(best.pattern));
  }

  this.nodes = parsedNodes;

  _.each(this.nodes, function (node) {
    node.updateDependency();
    node.logger.debug("initial bindVar: " + node.activeStream.bindVar + " (" + node.fullStream.count + ")");
  });

  this.running = true;
  this.readNode(minNode);
};

// Iterator overload. Starts the run (if it is not already running).
ClusteringController.prototype._read = function () {
  this.vote();
};

ClusteringController.prototype.vote = function () {
  // this will probably go badly if this gets called multiple times
  if (this.running)
    return;
  this.running = true;
  var votes = {};
  var self = this;
  var delayedCallback = _.after(Object.keys(this.clusters).length, function () {
    votes = _.omit(votes, _.isNull);
    // Remove nodes that are higher in the supply graph.
    var boundNodes = _.filter(votes, function (node) { return node.activeStream.feed; });
    var suppliers = _.flatten(_.map(boundNodes, function (node) {
      if (node.activeStream.bindVar)
        return node.activeStream.dependencies;
    }));
    var candidates = _.reject(votes, function (node) { return _.contains(suppliers, node); });
    //var minNode = ClusteringUtil.infiniMin(candidates, function (node) { return node.cost(); });
    var minNode = ClusteringUtil.infiniMin(candidates, function (node) { return node.triples.length; });
    //var minNode = ClusteringUtil.infiniMin(candidates, function (node) { return node.activeStream.count/node.activeStream.triples.length; });

    // logging
    var votingResults = _.map(votes, function (node, v) {
      if (!node)
        return null;
      var patternStr = v + ":" + rdf.toQuickString(node.pattern) + "(" + node.activeStream.bindVar + ")";
      if (node === minNode)
        patternStr = "*" + patternStr + "*";
      return patternStr;
    });
    self.logger.debug("votes: " + votingResults);

    // If minNode === Infinity there were no candidates remaining.
    if (minNode === Infinity) {
      self._end();

      return self.logger.debug('Finished, totally not a bug!');
    } else {
      self.readNode(minNode);
    }
  });
  // Requests the vote of every cluster.
  _.each(this.clusters, function (cluster) {
    cluster.vote(function (node) {
      votes[cluster.v] = node;
      delayedCallback();
    });
  });
};

// Reads new triples for the given node and updates all stored nodes.
ClusteringController.prototype.readNode = function (minNode) {
  var minCost = minNode.cost();
  var self = this;
  if (minCost > 0 && _.isFinite(minCost))
    _.each(self.nodes, function (node) { node.spend(minCost); });
  minNode.read(function () {
    var delayedCallback = _.after(self.nodes.length, function () {
      // only switch to download stream if we are sure it is for the best
      // start with the cheapest node and continue until we find an acceptable switch (or no nodes are left)
      _.some(_.sortBy(_.filter(self.nodes, function (node) { return node.activeStream.bindVar; }), function (node) { return node.fullStream.count; }), function (node) {
        var v = node.activeStream.bindVar;
        // value will be infinite if no values have been matched yet
        if (_.isFinite(node.activeStream.cost) && node.activeStream.cost > 1.1 * node.fullStream.cost) { // 1.1 to prevent unnecessary switching when the difference is small
          node.logger.debug("SWITCH STREAM " + v + " -> " + undefined + ", oldCost: " + node.activeStream.cost + ", cost: " + node.fullStream.cost);
          node.switchStream(null);
          self.store.reset(node);
          // move node to the front next to the other download streams
          var idx = _.indexOf(self.nodes, node);
          self.nodes.splice(idx, 1);
          var insertIdx = _.findIndex(self.nodes, function (node) { return !node.activeStream.bindVar; });
          if (insertIdx < 0)
            insertIdx = self.nodes.length;
          self.nodes.splice(insertIdx, 0, node);

          _.each(self.nodes, function (node) {
            node.updateDependency();
          });
          return true; // found a match, wait until next iteration to try again
        }
      });

      // Find all results for the complete query so far.
      self.store.matchBindings(self.nodes, function (results) {
        self.logger.debug("COMPLETE MATCHES: " + results.length);
        if (results.length < self.resultCount)
          self.logger.debug("RESULTS DECREASED!");
        // new results will always be at the end due to how the DB caching works
        var newResults = results.slice(self.resultCount);
        _.each(newResults, function (result) {
          self._push(result);
        });
        self.resultCount = Math.max(results.length, self.resultCount);
        self.running = false;
        if (newResults.length > 0) {
          self.emit('readable');
        } else {
          setImmediate(function () { self.vote(); });
        }
      });
    });

    // Update all nodes.
    _.each(self.nodes, function (node) {
      node.update(delayedCallback);
    });
  });
};

// All the nodes that are needed to supply the given nodes.
ClusteringController.prototype.supplyPath = function (nodes) {
  // Find all suppliers for the given nodes (directly or indirectly).
  var suppliedVars = _.filter(_.uniq(_.map(nodes, function (node) { return node.activeStream.bindVar; })));
  var results = [].concat(nodes);
  while (suppliedVars.length > 0) {
    var v = suppliedVars.shift();
    results = results.concat(this.clusters[v].suppliers());
    var newVars = this.clusters[v].dependsOn();
    suppliedVars = _.union(suppliedVars, newVars);
  }
  results =  _.uniq(results, function (node) { return ClusteringUtil.tripleID(node.pattern); });

  // Adds all connected download nodes. It is possible these are not included if one of the input nodes was also a download node.
  var downloadNodes = _.filter(this.nodes, function (node) { return !node.activeStream.feed; });
  var vars = _.union.apply(null, _.map(results, function (node) {
    return node.activeStream.bindVar ? [node.activeStream.bindVar] : rdf.getVariables(node.pattern);
  }));
  var filteredNodes = [];
  var size = 1;
  while (size != filteredNodes.length) {
    size = filteredNodes.length;
    filteredNodes = _.filter(downloadNodes, function (node) { return _.intersection(vars, rdf.getVariables(node.pattern)).length > 0; });
    var filteredVars = _.union.apply(null, _.map(filteredNodes, function (node) { return rdf.getVariables(node.pattern); }));
    vars = _.union(vars, filteredVars);
  }

  results = results.concat(filteredNodes);
  results = _.uniq(results, function (node) { return ClusteringUtil.tripleID(node.pattern); });
  return results;
};

// Find all unique values for the given variable in the given set of nodes.
ClusteringController.prototype.matchVariable = function (nodes, v, callback) {
  this.store.matchBindings(nodes, function (results) {
    // unique values are guaranteed due to v parameter
    var vals = results;

    var matchRates = _.map(nodes, function (node) { return vals.length / node.activeStream.tripleCount; });
    matchRates = _.map(matchRates, function (rate) { return _.isFinite(rate) ? rate : 0; });
    var estimates = _.map(nodes, function (node, idx) { return matchRates[idx] * node.activeStream.count; });
    estimates = _.map(estimates, function (estimate) { return _.isNaN(estimate) ? Infinity : estimate; });

    callback(vals, _.isEmpty(vals) ? Infinity : _.max(estimates), matchRates, estimates);
  }, v);
};

module.exports = ClusteringController;
