/**
 * Created by joachimvh on 11/09/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  Logger = require ('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil'),
  Stream = require('./Stream');

function Node (pattern, count, options) {
  this.pattern = pattern;
  this._options = options;
  this.logger = new Logger("Node " + rdf.toQuickString(pattern));
  this.logger.disable();
  this.fullStream = new Stream.DownloadStream(pattern, count, options);
  this.activeStream = this.fullStream;
  this.bindStreams = {};
  this.triples = [];
  this.dependencies = [];

  this.DEBUGclusters = {}; // TODO: too much interlinking
}

Node.prototype.getVariablePosition = function (variable) {
  if (this.pattern.subject === variable) return 'subject';
  if (this.pattern.predicate === variable) return 'predicate';
  if (this.pattern.object === variable) return 'object';
  return null;
};

Node.prototype.updateDependency = function () {
  this.activeStream.dependencies = this.generateStreamDependencies(this.activeStream.bindVar);
};

Node.prototype.generateStreamDependencies = function (v) {
  if (!v)
    return [];
  var idx = this.DEBUGclusters[v].DEBUGcontroller.nodes.indexOf(this);
  var nodes = _.without(_.union(this.DEBUGclusters[v].DEBUGcontroller.nodes.slice(0, idx), this.DEBUGclusters[v].supplyPath([this])), this);
  var filtered = _.intersection(nodes, this.DEBUGclusters[v].suppliers());
  var vars = _.union.apply(null, _.map(filtered, function (node) { return ClusteringUtil.getVariables(node.pattern); }));
  var changed = true;
  while (changed) {
    var valid = _.filter(nodes, function (node) {
      return !_.contains(filtered, node) && _.intersection(ClusteringUtil.getVariables(node.pattern), vars).length > 0;
    });
    vars = _.union(vars, _.flatten(_.map(valid, function (node) { return ClusteringUtil.getVariables(node.pattern); })));
    filtered = filtered.concat(valid);
    changed = valid.length > 0;
  }
  return filtered;
};

// TODO: the v parameter is actually archaic since we only update on the active stream, a single call would suffice, would also simplify some of the code
Node.prototype.update = function (v, estimate, completeBindings, updatedNode, callback) {
  var pos = this.getVariablePosition(v);
  if (!pos || this.ended())
    return callback();
  var bindStream = this.bindStreams[v];
  var self = this;

  if (bindStream && (bindStream === this.activeStream)) {
    this.DEBUGclusters[v].matchSuppliers(bindStream.dependencies, function (specificBindings, ownEstimate, ownMatchRates, ownEstimates) {
      completeBindings = specificBindings;
      self.logger.info('MATCHED: ' + completeBindings.length);
      estimate = ownEstimate;
      // take wrong (server-side) estimations into account
      if (estimate < completeBindings.length && _.some(bindStream.dependencies, function (dependency) { return !dependency.activeStream.ended; }))
        estimate = completeBindings.length+1;
      updateBindStream();
    });
  } else {
    updateBindStream();
  }

  //updateBindStream();
  function updateBindStream () {
    if (bindStream)
      bindStream.feed(completeBindings);

    if (self.activeStream === self.fullStream)
      self.activeStream.logger.info("UPDATE ended:" + self.activeStream.ended + ", remaining:" + self.activeStream.remaining + ", cost:" + self.activeStream.cost + ", count:" + self.activeStream.count + ", costRemaining:" + self.activeStream.costRemaining);

    // stabilize stream
    if (bindStream) {
      if (bindStream === self.activeStream) {
        bindStream.stabilize(function () {
          bindStream.updateRemaining(estimate - _.size(completeBindings));
          callback();
        });
      } else {
        bindStream.updateRemaining(estimate - _.size(completeBindings));
        callback();
      }
    } else {
      callback();
    }
  }
};

Node.prototype.cost = function () {
  if (this.ended())
    return Infinity;

  return this.activeStream.costRemaining;
};

Node.prototype.spend = function (cost) {
  var self = this;
  if (!self.ended())
    this.activeStream.logger.info("costs " + this.activeStream.cost + " remaining " + this.activeStream.costRemaining + " spend " + cost);
  this.activeStream.spend(cost);
};

Node.prototype.read = function (callback) {
  var self = this;
  this.activeStream.read(function (buffer) {
    self.triples = self.activeStream.triples;
    setImmediate(callback(buffer));
  });
};

Node.prototype.ended = function () {
  return this.activeStream.ended;
};

Node.prototype.supplies = function (v) {
  return this.activeStream.bindVar !== v;
};

Node.prototype.activeSupplyVars = function () {
  return _.without(ClusteringUtil.getVariables(this.pattern), this.activeStream.bindVar);
};

Node.prototype.waitingFor = function () {
  if (!this.activeStream.feed)
    return null;
  return this.activeStream.isHungry() ? this.activeStream.bindVar : null;
};

module.exports = Node;
