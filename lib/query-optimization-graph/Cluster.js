/**
 * Created by joachimvh on 11/09/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  Logger = require ('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil');

function Cluster (v) {
  this.v = v;
  this.nodes = [];
  this.logger = new Logger("Cluster " + v);
  this.logger.disable();
  // TODO: which of these values are actually still updated?
  this.estimate = Infinity; // make sure to update this in time
  this.completeBindings = [];

  this.DEBUGcontroller = null; // TODO: this really should not be here
}

Cluster.prototype.vote = function (callback) {
  var self = this;
  var suppliers = this.suppliers();
  // hungry streams: bind streams with no bind values (i.e. can't be used to find new values until they receive new bindings)
  suppliers = _.filter(suppliers, function (node) { return !node.ended() && !(node.activeStream.isHungry && node.activeStream.isHungry()); });

  // prioritize filters
  var filters = _.filter(this.filterNodes(), function (node) { return !node.ended() && !(node.activeStream.isHungry && node.activeStream.isHungry()); });
  if (filters.length > 0)
    return callback(filters[0]);

  if (suppliers.length === 0)
    return callback(null);

  // TODO: put this somewhere else
  _.each(_.reject(suppliers, function (node) { return _.has(node.activeStream.matchRates, self.v); }), function (node) { node.activeStream.matchRates[self.v] = 0; });
  _.each(_.reject(suppliers, function (node) { return _.has(node.activeStream.estimates, self.v); }), function (node) { node.activeStream.estimates[self.v] = Infinity; });

  // TODO: check which of these values are still correct
  self.logger.debug("VOTE complete: " + _.size(self.completeBindings) + ", suppliers: " + _.map(_.pluck(suppliers, 'pattern'), rdf.toQuickString) + ", matchRates: " + _.map(suppliers,function(node){return node.activeStream.matchRates[self.v];}) + ", estimates: " + _.map(suppliers,function(node){return node.activeStream.estimates[self.v];}));

  var vote;
  // not enough data yet, need to use other heuristics
  var emptySuppliers = _.filter(suppliers, function (node) { return node.activeStream.triples.length === 0; });
  if (emptySuppliers.length > 0)
      vote = ClusteringUtil.infiniMin(emptySuppliers, function (node) { return node.activeStream.cost; });

  // no use reading from a low estimate, will probably decrease the estimate even more?
  if (!vote)
    //vote = ClusteringUtil.infiniMin(suppliers, function (node) { return _.size(node.activeStream.triples); });
    //vote = _.max(suppliers, function (node) { return node.activeStream.count/node.activeStream.triples.length; });
    vote = _.min(suppliers, function (node) { return node.triples.length; });

  callback(vote);
};

Cluster.prototype.filterNodes = function () {
  // supplies no vars but is in this cluster ==> filter node
  return _.filter(this.nodes, function (node) { return node.activeSupplyVars().length === 0; });
};

Cluster.prototype.suppliers = function () {
  var v = this.v;
  return _.filter(this.nodes, function (node) { return node.supplies(v); });
};

// all the nodes that are needed to supply the given nodes
// TODO: put this somewhere else (requires all existing nodes and clusters)
// TODO: not used that often, should check where
Cluster.prototype.supplyPath = function (nodes) {
  var suppliedVars = _.filter(_.uniq(_.map(nodes, function (node) { return node.activeStream.bindVar; })));
  var results = [].concat(nodes);
  while (suppliedVars.length > 0) {
    var v = suppliedVars.shift();
    results = results.concat(this.DEBUGcontroller.clusters[v].suppliers());
    var newVars = this.DEBUGcontroller.clusters[v].dependsOn();
    suppliedVars = _.union(suppliedVars, newVars);
  }
  results =  _.uniq(results, function (node) { return ClusteringUtil.tripleID(node.pattern); });

  // TODO: prettify
  var downloadNodes = _.filter(this.DEBUGcontroller.nodes, function (node) { return !node.activeStream.feed; });
  var vars = _.union.apply(null, _.map(results, function (node) {
    // we want all connected download nodes
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

Cluster.prototype.dependsOn = function () {
  return _.filter(_.flatten(_.uniq(_.map(this.suppliers(), function (node) { return node.activeStream.bindVar; }))));
};

// TODO: unrelated to clusters actually, need to move this
Cluster.prototype.matchSuppliers = function (suppliers, callback) {
  this.DEBUGcontroller.store.matchBindings(suppliers, function (results) {
    // unique values are guaranteed due to self.v parameter
    var vals = results;

    // TODO: actually we might also have to calculate statistical significance here
    // TODO: also check which of these values are still needed
    var matchRates = _.map(suppliers, function (node) { return _.size(vals) / node.activeStream.tripleCount; });
    matchRates = _.map(matchRates, function (rate) { return _.isFinite(rate) ? rate : 0; });
    var estimates = _.map(suppliers, function (node, idx) { return matchRates[idx]*node.activeStream.count; });
    estimates = _.map(estimates, function (estimate) { return _.isNaN(estimate) ? Infinity : estimate; });

    callback(vals, _.isEmpty(vals) ? Infinity : _.max(estimates), matchRates, estimates);
  }, this.v);
};

Cluster.prototype.update = function (updatedNode, callback) {
  var self = this;

  var delayedCallback = _.after(this.nodes.length, callback);

  updateNodes();
  function updateNodes () {
    _.each(self.nodes, function (node) {
      node.update(self.v, self.estimate, self.completeBindings, updatedNode, delayedCallback);
    });
  }
};

module.exports = Cluster;