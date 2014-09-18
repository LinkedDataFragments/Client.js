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
  var suppliers = _.filter(this.nodes, function (node) { return node.supplies(self.v) && !node.ended() && !(node.activeStream.isHungry && node.activeStream.isHungry()); });

  // TODO: I don't want to call supply again here
  this.supply(function() {
    if (_.isEmpty(suppliers))
      return callback(null);

    var initRate = {};
    initRate[self.v] = 0;
    var initEstimate = {};
    initEstimate[self.v] = Infinity;

    _.each(suppliers, function (node) {
      _.defaults(node.activeStream.matchRates[self.v], initRate);
      _.defaults(node.activeStream.estimates[self.v], initEstimate);
    });

    self.logger.info("VOTE complete: " + _.size(self.completeBindings) + " suppliers: " + _.map(_.pluck(suppliers, 'pattern'), rdf.toQuickString) + " matchRates: " + _.map(suppliers,function(node){return node.activeStream.matchRates[self.v];}) + "" + " estimates: " + _.map(suppliers,function(node){return node.activeStream.estimates[self.v];}));

    // binding streams have priority (since the values they get should have been thoroughly checked already
    var bindSuppliers = _.filter(suppliers, function (node) { return node.activeStream.feed; });
    if (!_.isEmpty(bindSuppliers))
      suppliers = bindSuppliers;

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
  });
};

Cluster.prototype.supply = function (callback) {
  console.time("SUPPLY PRE-DB");
  var self = this;
  // TODO: should be ok since bounds should have been updated, maybe check this
  var suppliers = _.filter(this.nodes, function (node) { return node.supplies(self.v); });
  if (_.isEmpty(suppliers)) {
    setImmediate(callback);
    return;
  }
  // TODO: this could be cached as long as streams don't change
  var paths = _.flatten(_.map(suppliers, function (node) {
    var others = _.filter(suppliers, function (neighbour) { return rdf.toQuickString(node.pattern) < rdf.toQuickString(neighbour.pattern); }); // prevent double paths
    return _.flatten(_.map(others, function (neighbour) {
      // TODO: this will not return the short paths of only 2 long (but these are not necessary as we get these by just combining all suppliers)
      return self.DEBUGcontroller.getAllPaths(node, neighbour, [self.v]);
    }), true);
  }), true);
  var storeInput = _.uniq(_.pluck(_.flatten(paths).concat(suppliers), 'pattern'), rdf.toQuickString);
  // TODO: obviously only use connected streams, but in test case this is fine
  var downloadNodes = _.filter(this.DEBUGcontroller.nodes, function (node) { return !node.activeStream.feed; });
  storeInput = _.uniq(storeInput.concat(_.pluck(downloadNodes, 'pattern')), rdf.toQuickString);
  console.timeEnd("SUPPLY PRE-DB");
  console.time("SUPPLY DB");
  this.DEBUGcontroller.store.matchBindings(storeInput, function (results) {
    console.timeEnd("SUPPLY DB");
    var vals = _.uniq(_.pluck(results, self.v));
    self.logger.info("MATCHED: " + _.size(vals));
    self.completeBindings = vals;

    // TODO: also use db
    var matchRates = _.map(suppliers, function (node) { return _.size(vals) / _.size(node.activeStream.triples); });
    matchRates = _.map(matchRates, function (rate) { return _.isFinite(rate) ? rate : 0; });
    var estimates = _.map(suppliers, function (node, idx) { return matchRates[idx]*node.activeStream.count; });
    _.each(suppliers, function (node, idx) {
      node.activeStream.matchRates[self.v] = matchRates[idx];
      node.activeStream.estimates[self.v] = estimates[idx];
    });
    self.estimate = _.isEmpty(vals) ? Infinity : _.max(estimates);
    setImmediate(callback);
  });
};

Cluster.prototype.update = function (callback) {
  console.time("PRE-UPDATE");
  var self = this;
  var count = _.min(_.invoke(this.nodes, 'count'));
  var remaining = _.min(_.invoke(this.nodes, 'remaining'));

  var delayedCallback = _.after(_.size(this.nodes), function () {
    console.timeEnd("UPDATE NODES");
    self.add = [];
    self.remove = [];
    callback();
  });

  // TODO: this totally should not be done here and should be split in separate functions
  this.supply(function () {
    console.timeEnd("PRE-UPDATE");
    console.time("UPDATE NODES");
    _.each(self.nodes, function (node) {
      node.update(self.v, count, remaining, self.bindings, self.bounds, self.estimate, self.completeBindings, self.add, self.remove, [], delayedCallback);
    });
  });
};

module.exports = Cluster;