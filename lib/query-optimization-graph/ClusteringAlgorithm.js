/**
 * Created by joachimvh on 21/08/2014.
 */


var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  LDFClustering = require('./LDFClustering'),
  pageSize = 100; // TODO: load this from metadata

// TODO: find better name

function ClusteringAlgorithm (clustering) {
  this._clustering = clustering;
}

ClusteringAlgorithm.prototype.run = function (callback) {
  var root = this;
  var clustering = this._clustering;
  var delayedCallback = _.after(clustering.nodes.length, function () { step(); });
  _.each(clustering.nodes, function(node) {
    console.error("Init (1): Count " + rdf.toQuickString(node.pattern));
    clustering.countNode(node, delayedCallback);
  });
  function step(){
    //clustering.propagateData();
    clustering.propagatePathData();
    if (clustering.isFinished()) {
      var result = clustering.getResults();
      callback(result);
      return;
    }
    var calls = _.filter(
      [
        root._minimalFullDownload(),
        root._minimalSingleVarCount(),
        root._minimalSingleVarDownload(),
        root._minimalFullBindingDownload()
      ],
      _.identity
    );
    var minCall = _.min(
      calls,
      function (call) { return call.cost; }
    );
    console.error("Best (" + minCall.cost + "): " + minCall.description);
    minCall.call(step);
  }
};

ClusteringAlgorithm._createCall = function (cost, call, description) {
  return {
    cost: cost,
    call: call,
    description: description
  };
};

ClusteringAlgorithm._createCallsFromNodeList = function (nodes, costF, callF, descriptionF) {
  return _.map(nodes, function (node) {
    return ClusteringAlgorithm._createCall(
      costF(node),
      function (callback) { callF(node, callback); },
      descriptionF(node));
  });
};


ClusteringAlgorithm._filterIncompletes = function(nodes) {
  return _.filter(nodes, function(node){ return !node.complete && rdf.hasVariables(node.pattern); });
};

ClusteringAlgorithm.prototype._minimalFullDownload = function () {
  var clustering = this._clustering;
  var incompletes = ClusteringAlgorithm._filterIncompletes(clustering.nodes);
  return _.min(ClusteringAlgorithm._createCallsFromNodeList(
      incompletes,
      function(node) { return Math.ceil(node.count/pageSize)-1; }, // -1 because first page should already be cached
      function(node, callback) {
        clustering.downloadNode(node, function() {
          node.complete = true;
          node.count = node.triples.length;
          callback();
        });
      },
      function(node) { return "Full download: " + rdf.toQuickString(node.pattern); }
    ),
    function (call) {
      return call.cost;
    }
  );
};

ClusteringAlgorithm.prototype._minimalSingleVarCount = function () {
  var clustering = this._clustering;
  var boundIncompleteClusters = _.filter(clustering.clusters, function(cluster){
    return cluster.bindings !== null && _.some(cluster.nodes, function(node) {
      // nodes should be incomplete and not yet have fix values
      return !node.complete && _.some(["subject", "predicate", "object"], function(v) {
        return node.pattern[v] === cluster.key && _.isEmpty(node.fixCount);
      });
    });
  });

  // can't find a cluster that has bindings and incomplete nodes (with no fix cost for that cluster)
  if (boundIncompleteClusters.length <= 0)
    return null;

  // take the cluster with the lowest number of bindings, this minimizes the cost
  var minCluster = _.min(boundIncompleteClusters, function (cluster) { return cluster.bindings.length; } );

  var minIncompleteNode = _.min(ClusteringAlgorithm._filterIncompletes(minCluster.nodes), function (node) { return node.count; });
  return ClusteringAlgorithm._createCall(
    minCluster.bindings.length,
    function (callback) {
      clustering.countBindings(minIncompleteNode, minCluster.key, callback);
    },
    "Count single binding on var " + minCluster.key + ": " + rdf.toQuickString(minIncompleteNode.pattern)
  );
};

ClusteringAlgorithm.prototype._minimalSingleVarDownload = function () {
  var clustering = this._clustering;
  var incompletes = ClusteringAlgorithm._filterIncompletes(clustering.nodes);
  var fixedCountIncompletes = _.filter(incompletes, function(node) { return !_.isEmpty(node.fixCount); });

  if (fixedCountIncompletes.length <= 0) // no nodes with fix costs found
    return null;

  var nodeBindCosts = _.map(fixedCountIncompletes, function(node) {
    var result = { node: node };
    _.each(_.keys(node.fixCount), function (v) {
      result[v] = _.reduce(_.values(node.fixCount[v]), function (memo, bindCost) {
        return memo + Math.max(0, Math.ceil(bindCost/pageSize)-1); // -1 because first page should be cached already
      }, 0);
    });
    return result;
  });

  var minNodeBindCost = _.min(nodeBindCosts, function (nodeBindCost) {
    return Math.min(_.values(_.omit(nodeBindCost, 'node')));
  });
  var minVar = _.min(_.keys(_.omit(minNodeBindCost, 'node')), function (key) {
    return minNodeBindCost[key];
  });

  return ClusteringAlgorithm._createCall(
    minNodeBindCost[minVar],
    function (callback) {
      clustering.applyBindings(minNodeBindCost.node, [minVar], callback);
    },
    "Download single binding on var " + minVar + ": " + rdf.toQuickString(minNodeBindCost.node.pattern)
  );
};

ClusteringAlgorithm.prototype._minimalFullBindingDownload = function () {
  var clustering = this._clustering;
  var incompletes = ClusteringAlgorithm._filterIncompletes(clustering.nodes);
  var patternsWithBindings = _.filter(incompletes, function(node) {
    var vars = LDFClustering.getVariables(node.pattern);
    return _.every(vars, function(v){ return clustering.clusters[v].bindings !== null; });
  });
  if (patternsWithBindings.length <= 0)
    return null;

  return _.min(ClusteringAlgorithm._createCallsFromNodeList(
      patternsWithBindings,
      function (node) {
        // take the product of all the bindings for each variable in the pattern
        return _.reduce(LDFClustering.getVariables(node.pattern), function (memo, v) {
          return memo * clustering.clusters[v].bindings.length;
        }, 1);
      },
      function (node, callback) {
        clustering.applyBindings(node, true, true, true, callback);
      },
      function (node) { return "Download full binding: " + rdf.toQuickString(node.pattern); }
    ),
    function (call) {
      return call.cost;
    }
  );
};

module.exports = ClusteringAlgorithm;