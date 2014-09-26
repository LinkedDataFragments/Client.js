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
  RDFStoreInterface = require('./RDFStoreInterface');

function Cluster (v) {
  this.v = v;
  this.nodes = [];
  this.bounds = null;
  this.bindings = [];
  this.logger = new Logger("Cluster " + v);
  //this.logger.disable();
  this.estimate = Infinity; // make sure to update this in time
  this.completeBindings = [];

  this.add = [];
  this.remove = [];

  this.DEBUGcontroller = null; // TODO: this really should not be here
}

Cluster.prototype.addBindings = function (bindings) {
  // TODO: as soon as we have bounds it's useless to add data? (unless we want to count matches)
  if (_.isEmpty(bindings))
    return;
  if (this.bounds)
    bindings = _.intersection(this.bounds, bindings);
  bindings = _.difference(_.uniq(bindings), this.bindings);
  this.add = _.union(this.add, bindings);
  this.bindings = _.union(this.bindings, bindings);
};

Cluster.prototype.removeBindings = function (bindings) {
  if (_.isEmpty(bindings))
    return;
  this.bindings = _.difference(this.bindings, bindings);
  this.remove = _.union(this.remove, bindings);
};

Cluster.prototype.addBounds = function (bounds) {
  bounds = _.uniq(bounds);
  if (this.bounds)
    this.bounds = _.intersection(this.bounds, bounds);
  else
    this.bounds = bounds;

  var grouped = _.groupBy(this.bindings, function (binding) {
    return _.contains(bounds, binding) ? 'keep' : 'remove';
  });
  grouped.keep = grouped.keep || [];
  grouped.remove = grouped.remove || [];
  this.bindings = grouped.keep;
  this.remove = _.union(this.remove, grouped.remove);
  this.logger.info("BOUND " + this.v + " to " + _.size(this.bounds) + " values, removed " + _.size(grouped.remove));
};

// TODO: we also have verify nodes, what to do with them? (apply them to all the complete data?)
// TODO: sliding window with averages to get more correct result?
Cluster.prototype.vote = function (callback) {
  var self = this;
  // TODO: ok ignoring hungry streams? -> no, bad, other stream will be read when we dont need it
  var suppliers = this.suppliers();
  suppliers = _.filter(suppliers, function (node) { return !node.ended() && !(node.activeStream.isHungry && node.activeStream.isHungry()); });

  // TODO: really best to always prioritize filters?
  var filters = _.filter(this.filterNodes(), function (node) { return !node.ended() && !(node.activeStream.isHungry && node.activeStream.isHungry()); });
  if (!_.isEmpty(filters))
    return callback(_.first(filters));

  if (_.isEmpty(suppliers))
    return callback(null);

  _.each(_.reject(suppliers, function (node) { return _.has(node.activeStream.matchRates, self.v); }), function (node) { node.activeStream.matchRates[self.v] = 0; });
  _.each(_.reject(suppliers, function (node) { return _.has(node.activeStream.estimates, self.v); }), function (node) { node.activeStream.estimates[self.v] = Infinity; });

  self.logger.info("VOTE complete: " + _.size(self.completeBindings) + " suppliers: " + _.map(_.pluck(suppliers, 'pattern'), rdf.toQuickString) + " matchRates: " + _.map(suppliers,function(node){return node.activeStream.matchRates[self.v];}) + "" + " estimates: " + _.map(suppliers,function(node){return node.activeStream.estimates[self.v];}));

  // binding streams have priority (since the values they get should have been thoroughly checked already
  // TODO: binding streams have priority over their suppliers, not over all other nodes
//    var bindSuppliers = _.filter(suppliers, function (node) { return node.activeStream.feed; });
//    if (!_.isEmpty(bindSuppliers))
//      suppliers = bindSuppliers;

  // really need to call one of these
  var needySuppliers = _.filter(suppliers, function (node) { return node.cost() < 0; });
  if (!_.isEmpty(needySuppliers))
    suppliers = needySuppliers;

  var vote;
  // not enough data yet, need to use other heuristics
  if (_.isEmpty(this.completeBindings)) {
    // cold start
    // TODO: 3+ streams
    var emptySuppliers = _.filter(suppliers, function (node) { return _.size(node.activeStream.triples) === 0; });
    if (!_.isEmpty(emptySuppliers))
      vote = ClusteringUtil.infiniMin(emptySuppliers, function (node) { return node.cost(); });
    // TODO: if there are no matched values binding streams will return no values
    else
      vote = ClusteringUtil.infiniMin(suppliers, function (node) { return _.size(node.activeStream.triples); });
  }

  // no use reading from a low estimate, will probably decrease the estimate even more
  if (!vote)
    vote = _.max(suppliers, function (node) { return node.activeStream.estimates[self.v]; });

  callback(vote);
};

Cluster.prototype.filterNodes = function () {
  // supplies no vars but is in this cluster ==> filter node
  return _.filter(this.nodes, function (node) { return _.isEmpty(node.activeSupplyVars()); });
};

Cluster.prototype.suppliers = function () {
  var self = this;
  return _.filter(this.nodes, function (node) { return node.supplies(self.v); });
};

// all the nodes that are needed to supply the given nodes
// TODO: put this somewhere else
Cluster.prototype.supplyPath = function (nodes) {
  var suppliedVars = _.filter(_.flatten(_.uniq(_.map(nodes, function (node) { return node.activeStream.bindVar; }))));
  var results = [].concat(nodes);
  // TODO: I wonder if this can be an infinite loop
  while (!_.isEmpty(suppliedVars)) {
    var v = suppliedVars.shift();
    results = results.concat(this.DEBUGcontroller.clusters[v].suppliers());
    var newVars = this.DEBUGcontroller.clusters[v].dependsOn();
    suppliedVars = _.union(suppliedVars, newVars);
  }
  results =  _.uniq(results, function (node) { return rdf.toQuickString(node.pattern); });

  // TODO: prettify
  var downloadNodes = _.filter(this.DEBUGcontroller.nodes, function (node) { return !node.activeStream.feed; });
  var vars = _.union.apply(null, _.map(results, function (node) { return ClusteringUtil.getVariables(node.pattern); }));
  var filteredNodes = [];
  var size = 1;
  while (size != _.size(filteredNodes)) {
    size = _.size(filteredNodes);
    filteredNodes = _.filter(downloadNodes, function (node) { return _.size(_.intersection(vars, ClusteringUtil.getVariables(node.pattern))) > 0; });
    var filteredVars = _.union.apply(null, _.map(filteredNodes, function (node) { return ClusteringUtil.getVariables(node.pattern); }));
    vars = _.union(vars, filteredVars);
  }

  results = results.concat(filteredNodes);
  return _.uniq(results, function (node) { return rdf.toQuickString(node.pattern); });
};

Cluster.prototype.dependsOn = function () {
  return _.filter(_.flatten(_.uniq(_.map(this.suppliers(), function (node) { return node.activeStream.bindVar; }))));
};

Cluster.prototype.supply = function (callback) {
  //console.time("SUPPLY PRE-DB");
  var self = this;
  // TODO: should be ok since bounds should have been updated, maybe check this
  var suppliers = this.suppliers();
  if (_.isEmpty(suppliers)) {
    setImmediate(callback);
    return;
  }

  this.matchSuppliers(suppliers, function (completeBindings, estimate, matchRates, estimates){
    self.completeBindings = completeBindings;
    self.estimate = estimate;

    _.each(suppliers, function (node, idx) {
      node.activeStream.matchRates[self.v] = matchRates[idx];
      node.activeStream.estimates[self.v] = estimates[idx];
    });

    setImmediate(callback);
  });
};

// TODO: unrelated to clusters actually, could move this
Cluster.prototype.matchSuppliers = function (suppliers, callback) {
  var self = this;
  var storeInput = _.pluck(this.supplyPath(suppliers), 'pattern');
  // TODO: this could be cached as long as streams don't change
//  var paths = _.flatten(_.map(suppliers, function (node) {
//    var others = _.filter(suppliers, function (neighbour) { return rdf.toQuickString(node.pattern) < rdf.toQuickString(neighbour.pattern); }); // prevent double paths
//    return _.flatten(_.map(others, function (neighbour) {
//      // TODO: this will not return the short paths of only 2 long (but these are not necessary as we get these by just combining all suppliers)
//      return self.DEBUGcontroller.getAllPaths(node, neighbour, [self.v]);
//    }), true);
//  }), true);
//  var storeInput = _.uniq(_.pluck(_.flatten(paths).concat(suppliers), 'pattern'), rdf.toQuickString);
//  var downloadNodes = _.filter(this.DEBUGcontroller.nodes, function (node) { return !node.activeStream.feed; });
//  var vars = _.union.apply(null, _.map(suppliers, function (node) { return ClusteringUtil.getVariables(node.pattern); }));
//  // TODO: actually, we want all the suppliers back to the top I think
//  var filteredNodes = [];
//  var size = 1;
//  while (size != _.size(filteredNodes)) {
//    size = _.size(filteredNodes);
//    filteredNodes = _.filter(downloadNodes, function (node) { return _.size(_.intersection(vars, ClusteringUtil.getVariables(node.pattern))) > 0; });
//    var filteredVars = _.union.apply(null, _.map(filteredNodes, function (node) { return ClusteringUtil.getVariables(node.pattern); }));
//    vars = _.union(vars, filteredVars);
//  }
//  storeInput = _.uniq(storeInput.concat(_.pluck(filteredNodes, 'pattern')), rdf.toQuickString);
  // TODO: how to include filters? (just adding them not good: they haven't always checked all the vals yet)
  // TODO: somehow put filters between suppliers and hungry nodes?
  //this.logger.info(_.map(storeInput, function (pattern) { return rdf.toQuickString(pattern); }));
  //this.logger.info(_.map(storeInputDEBUG, function (pattern) { return rdf.toQuickString(pattern); }));
  this.DEBUGcontroller.store.matchBindings(storeInput, function (results) {
    var vals = _.uniq(_.pluck(results, self.v));
    self.logger.info("MATCHED: " + _.size(vals));

    var matchRates = _.map(suppliers, function (node) { return _.size(vals) / node.activeStream.tripleCount; });
    //var matchRates = _.map(suppliers, function (node) { return _.size(vals) / _.size(node.activeStream.triples); });
    matchRates = _.map(matchRates, function (rate) { return _.isFinite(rate) ? rate : 0; });
    var estimates = _.map(suppliers, function (node, idx) { return matchRates[idx]*node.activeStream.count; });
    estimates = _.map(estimates, function (estimate) { return _.isNaN(estimate) ? Infinity : estimate; });

    callback(vals, _.isEmpty(vals) ? Infinity : _.max(estimates), matchRates, estimates);
  });
};

Cluster.prototype.update = function (callback) {
  var self = this;
  var count = _.min(_.invoke(this.nodes, 'count'));
  var remaining = _.min(_.invoke(this.nodes, 'remaining'));

  var delayedCallback = _.after(_.size(this.nodes), function () {
    self.add = [];
    self.remove = [];
    callback();
  });

  // TODO: this totally should not be done here and should be split in separate functions
  this.supply(function () {
    _.each(self.nodes, function (node) {
      node.update(self.v, count, remaining, self.bindings, self.bounds, self.estimate, self.completeBindings, self.add, self.remove, [], delayedCallback);
    });
  });
};

module.exports = Cluster;