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
Cluster.prototype.vote = function () {
  var self = this;
  // TODO: should be ok since bounds should have been updated, maybe check this
  var suppliers = _.filter(this.nodes, function (node) { return node.supplies(self.v) && !node.ended(); });
  if (_.isEmpty(suppliers)) {
    this.completeBindings = this.bounds;
    this.estimate = _.size(this.bounds);
    this.logger.info("VOTE finished cluster, size:" + _.size(this.completeBindings));
    return null; // all suppliers finished
  }
  // TODO: feels wrong if a bad stream is selected
  if (_.size(suppliers) === 1) {
    var pos = suppliers[0].getVariablePosition(this.v);
    // TODO: this shouldn't be here
    this.completeBindings = _.uniq(_.pluck(suppliers[0].activeStream.triples, pos));
    if (this.bounds)
      this.completeBindings = _.intersection(this.completeBindings, this.bounds);
    this.estimate = suppliers[0].count(); // TODO: this will only work if this value gets updated when bounds are used
    this.logger.info("VOTE single cluster, size:" + _.size(this.completeBindings) + ", estimate:" + this.estimate);
    return suppliers[0];
  }
  var bindingsCount = {};
  _.each(suppliers, function (node) {
    var bindings = _.uniq(_.pluck(node.activeStream.triples, node.getVariablePosition(self.v)));
    _.each(bindings, function (binding) {
      bindingsCount[binding] = _.has(bindingsCount, binding) ? bindingsCount[binding]+1 : 1;
    });
  });
  this.completeBindings = _.filter(_.keys(bindingsCount), function (binding) { return bindingsCount[binding] === _.size(suppliers); });
  // TODO: all non-download streams: find all paths with all other nodes
  var bindSuppliers = _.filter(suppliers, function (node) { return node.activeStream.feed; });
  var paths = _.flatten(_.map(bindSuppliers, function (node) {
    var others = _.filter(suppliers, function (neighbour) { return rdf.toQuickString(node.pattern) < rdf.toQuickString(neighbour.pattern); }); // prevent double paths
    return _.flatten(_.map(others, function (neighbour) {
      return self.DEBUGcontroller.getAllPaths(node, neighbour);
    }), true);
  }), true);
  this.logger.info("PATHS (" + _.size(paths) + "): " + _.map(paths, function (path) { return _.map(path, function (node) { return rdf.toQuickString(node.pattern); }); }));
  var storeInput = _.uniq(_.pluck(_.flatten(paths).concat(_.difference(suppliers, bindSuppliers)), 'pattern'), rdf.toQuickString);
  this.DEBUGcontroller.store.matchBindings(storeInput, function (results) {
    self.logger.info("MATCHED: " + _.size(_.uniq(_.pluck(results, self.v))));
  });
//  this.logger.info("VALIDS: " + _.map(paths, function (path) {
//    return _.size(_.filter(_.map(self.completeBindings, function (val) {
//      var binding = _.object([[self.v, val]]);
//      return self.DEBUGcontroller.validatePath(binding, path);
//    })));
//  }));

  var matchRates = _.map(suppliers, function (node) { return _.size(self.completeBindings) / _.size(node.activeStream.triples); });
  matchRates = _.map(matchRates, function (rate) { return _.isFinite(rate) ? rate : 0; });
  var estimates = _.map(suppliers, function (node, idx) { return matchRates[idx]*node.activeStream.count; });

  this.logger.info("VOTE complete: " + _.size(this.completeBindings) + " matchRates: " + matchRates + "" + " suppliers: " + _.size(suppliers) + " estimates: " + estimates);

  // not enough data yet, need to use other heuristics
  if (_.isEmpty(this.completeBindings)) {
    // cold start
    // TODO: 3+ streams
    if (ClusteringUtil.sum(_.map(suppliers, function (node) { return _.size(node.activeStream.triples); })) === 0)
      return ClusteringUtil.infiniMin(suppliers, function (node) { return node.cost(); });
    // TODO: if there are no matched values binding streams will return no values
    return ClusteringUtil.infiniMin(suppliers, function (node) { return _.size(node.activeStream.triples); });
  }

  // no use reading from a low estimate, will probably decrease the estimate even more
  this.estimate = _.max(estimates);
  return _.max(suppliers, function (node, idx) { return estimates[idx]; });
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
  this.vote();
  _.each(this.nodes, function (node) {
    node.update(self.v, count, remaining, self.bindings, self.bounds, self.estimate, self.completeBindings, self.add, self.remove, [], delayedCallback);
  });
};

module.exports = Cluster;