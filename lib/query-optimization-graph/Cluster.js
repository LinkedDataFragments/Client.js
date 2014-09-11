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

  this.add = [];
  this.remove = [];
}

Cluster.prototype.addBindings = function (bindings) {
  if (_.isEmpty(bindings))
    return;
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

  _.each(this.nodes, function (node) {
    node.update(self.v, count, self.bindings, self.add, self.remove, remaining, delayedCallback);
  });
};

module.exports = Cluster;