/**
 * Created by joachimvh on 11/09/2014.
 */
/* Class corresponding to a single pattern in the BGP. */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  Logger = require('../util/Logger'),
  Stream = require('./Stream');

// Count is the estimate returned by the server.
function Node(pattern, count, options) {
  this.pattern = pattern;
  this._options = options;
  this.logger = new Logger("Node " + rdf.toQuickString(pattern));
  this.logger.disable();
  this.fullStream = new Stream.DownloadStream(pattern, count, options);
  this.activeStream = this.fullStream;
  this.triples = [];
  this.dependencies = [];

  this.controller = null;
}

// Changes the role of the Node. v should be null if the change is to download.
Node.prototype.switchStream = function (v) {
  if (v)
    this.activeStream = new Stream.BindingStream(this.fullStream.count, this.pattern, v, this._options);
  else
    this.activeStream = this.fullStream;
  this.triples = this.activeStream.triples;
};

// Updates the dependency values for the active stream. This needs to be called on role change or when the supply graph changes.
Node.prototype.updateDependency = function () {
  this.activeStream.dependencies = this.generateStreamDependencies(this.activeStream.bindVar);
};

Node.prototype.generateStreamDependencies = function (v) {
  if (!v)
    return [];
  var idx = this.controller.nodes.indexOf(this);
  var nodes = _.without(_.union(this.controller.nodes.slice(0, idx), this.controller.supplyPath([this])), this);
  var filtered = _.filter(nodes, function (node) { return node.supplies(v); });
  var vars = _.union.apply(null, _.map(filtered, function (node) { return rdf.getVariables(node.pattern); }));
  var changed = true;
  while (changed) {
    var valid = _.filter(nodes, function (node) {
      return !_.contains(filtered, node) && _.intersection(rdf.getVariables(node.pattern), vars).length > 0;
    });
    vars = _.union(vars, _.flatten(_.map(valid, function (node) { return rdf.getVariables(node.pattern); })));
    filtered = filtered.concat(valid);
    changed = valid.length > 0;
  }
  return filtered;
};

// Updates the node's stored values (if it is a bind node) based on the triples we downloaded so far.
Node.prototype.update = function (callback) {
  if (this.ended())
    return callback();
  var self = this;
  var stream = this.activeStream;

  if (stream.bindVar) {
    this.controller.matchVariable(stream.dependencies, stream.bindVar, function (bindings, estimate) {
      self.logger.debug('MATCHED: ' + bindings.length);
      // take wrong (server-side) estimations into account
      if (estimate < bindings.length && _.some(stream.dependencies, function (dependency) { return !dependency.activeStream.ended; }))
        estimate = bindings.length + 1;

      stream.feed(bindings);

      stream.stabilize(function () {
        stream.updateRemaining(estimate - bindings.length);
        callback();
      });
    });
  } else {
    stream.logger.debug("UPDATE ended:" + stream.ended + ", remaining:" + stream.remaining + ", cost:" + stream.cost + ", count:" + stream.count + ", costRemaining:" + stream.costRemaining);
    callback();
  }
};

Node.prototype.cost = function () {
  if (this.ended())
    return Infinity;

  return this.activeStream.costRemaining;
};

Node.prototype.spend = function (cost) {
  if (!this.ended())
    this.activeStream.logger.debug("costs: " + this.activeStream.cost + ", remaining: " + this.activeStream.costRemaining + ", spend: " + cost);
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

// The variables supplied by this pattern.
Node.prototype.activeSupplyVars = function () {
  return _.without(rdf.getVariables(this.pattern), this.activeStream.bindVar);
};

// For which variable new values are required before this node can continue.
Node.prototype.waitingFor = function () {
  if (!this.activeStream.feed)
    return null;
  return this.activeStream.isHungry() ? this.activeStream.bindVar : null;
};

module.exports = Node;
