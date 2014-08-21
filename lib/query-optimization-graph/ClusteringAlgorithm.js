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
      // TODO: remove fix values if bindings of a cluster get updated?
      return !node.complete && _.some(["subject", "predicate", "object"], function(v) {
        return node.pattern[v] === cluster.key && node.fixCount[v] < 0;
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
  var fixedCountIncompletes = _.filter(incompletes, function(node) { return _.some(_.values(node.fixCount), function(v) { return v >= 0; }); });

  if (fixedCountIncompletes.length <= 0) // no nodes with fix costs found
    return null;

  return _.min(ClusteringAlgorithm._createCallsFromNodeList(
      fixedCountIncompletes,
      function (node) {
        return Math.min(_.map(filterFixedPositions(node), function(v) { return node.fixCount[v]; })); // value stored in fixcount is already the exact number of calls that will be needed
      },
      function (node, callback) {
        var minPos = _.min(filterFixedPositions(node), function(v) { return node.fixCount[v]; });
        clustering.applyBindings(
          node,
          [node.pattern[minPos]],
          callback
        );
      },
      function (node) {
        var minPos = _.min(filterFixedPositions(node), function(v) { return node.fixCount[v]; });
        return "Download single binding on var " + node.pattern[minPos] + ": " + rdf.toQuickString(node.pattern);
      }
    ),
    function (call) {
      return call.cost;
    }
  );

  function filterFixedPositions (node) {
    return _.filter(["subject", "predicate", "object"], function(v) { return node.fixCount[v] >= 0; });
  }
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
          // TODO: this is less if we already have some fixCosts, try to incorporate those?
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