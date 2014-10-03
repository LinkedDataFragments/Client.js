/**
 * Created by joachimvh on 2/10/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  rdfstore = require('rdfstore'),
  N3 = require('N3'),
  Logger = require ('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil');

function HashStoreInterface (nodes) {
  this.nodes = nodes;
  this.nodeMap = _.object(_.map(nodes, function (node) { return rdf.toQuickString(node.pattern); }), nodes);
  this.DEBUGtime = 0;
}

// TODO: don't do this if this actually works
HashStoreInterface.prototype.addTriples = function (triples, callback) {
  callback(); // dummy function, data is added elsewhere
};

HashStoreInterface.prototype.matchBindings = function (patterns, callback) {
  var DEBUGdate = new Date();
  var self = this;
  var nodes = _.map(patterns, function (pattern) { return self.nodeMap[rdf.toQuickString(pattern)]; });
  var results = null;
  var vars = [];
  while (!_.isEmpty(nodes)) {
    var nodeIdx = _.findIndex(nodes, function (node) { return !_.isEmpty(_.intersection(vars, ClusteringUtil.getVariables(node.pattern))); });
    if (nodeIdx < 0)
      nodeIdx = 0;
    var node = nodes.splice(nodeIdx, 1)[0];
    var nodeVars = ClusteringUtil.getVariables(node.pattern);
    var matchVar = _.first(_.intersection(vars, nodeVars));
    if (!matchVar)
      matchVar = _.first(ClusteringUtil.getVariables(node.pattern));
    vars = _.union(vars, nodeVars);
    if (!results) {
      results = _.map(_.map(_.values(node.store[matchVar]), function (obj) { return _.values(obj)[0]; }), function (triple) { return rdf.extendBindings({}, node.pattern, triple); });
    } else {
      results = _.flatten(_.filter(_.map(results, function (binding) {
        var matches = node.store[matchVar][binding[matchVar]];
        if (!matches)
          return null;
        return _.filter(_.map(_.values(matches), function (triple) {
          try { return rdf.extendBindings(binding, node.pattern, triple); }
          catch (error) { return null; }
        }));
      })));
    }
  }
  this.DEBUGtime += new Date() - DEBUGdate;
  callback(results);
};

module.exports = HashStoreInterface;