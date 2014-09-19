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
  RDFStoreInterface = require('./RDFStoreInterface'),
  Stream = require('./Stream');

function Node (pattern, count, options) {
  this.pattern = pattern;
  this._options = options;
  this.logger = new Logger("Node " + rdf.toQuickString(pattern));
  //this.logger.disable();
  this.fullStream = new Stream.DownloadStream(pattern, count, options);
  this.activeStream = this.fullStream;
  this.bindStreams = {};
  this.triples = []; // TODO: store triples per stream?
}

Node.prototype.fixBindVar = function (v) {
  // TODO: DEBUG
  this.fixed = true;
  if (!v) {
    this.activeStream = this.fullStream;
  } else {
    if (this.bindStreams[v]) {
      this.activeStream = this.bindStreams[v];
    } else {
      this.activeStream = new Stream.BindingStream(this.fullStream.count, this.pattern, v, this._options);
      this.bindStreams[v] = this.activeStream; // this is necessary for the feeding process
    }
  }
};

Node.prototype.streams = function () {
  if (this.fixed)
    return [this.activeStream];
  return _.values(this.bindStreams).concat([this.fullStream]);
};

Node.prototype.getVariablePosition = function (variable) {
  if (this.pattern.subject === variable) return 'subject';
  if (this.pattern.predicate === variable) return 'predicate';
  if (this.pattern.object === variable) return 'object';
  return null;
};

Node.prototype.update = function (v, count, remaining, vals, bounds, estimate, completeBindings, added, removed, complete, callback) {
  // TODO: problem if added is by fulldownload stream of this node (and then gets added to other streams)
  // TODO: direct fix checking on source of added is fine, but can potentially still be a problem if we have a cycle
  // TODO: just checking if we got that value from full download stream is not enough (because there might still be mappings left we don't have!)
  var pos = this.getVariablePosition(v);
  if (!pos || this.ended())
    return callback();
  // TODO: first attempt to fix this double data problem
  //console.time("UPDATE FILTER INCOMING");
  var localVals = _.pluck(this.triples, pos);
  added = _.difference(added, localVals);
  vals = _.difference(vals, localVals);
  //console.timeEnd("UPDATE FILTER INCOMING");
//  //console.time("UPDATE BOUNDS");
//  // TODO: sort of really inefficient
//  _.each(this.streams(), function (stream) {
//    // TODO: need to update count/cost/etc.?
//    if (bounds)
//      stream.triples = _.filter(stream.triples, function (triple) { return _.contains(bounds, triple[pos]); } );
//    if (removed && !_.isEmpty(removed))
//      stream.triples = _.reject(stream.triples, function (triple) { return _.contains(removed, triple[pos]); } );
//  });
//  //console.timeEnd("UPDATE BOUNDS");
  var bindStream = this.bindStreams[v];

  //console.time("UPDATE FEED");
  // TODO: complete binding test
  if (bindStream) {
    //bindStream.feed(added, removed);
    bindStream.feed(completeBindings, removed);
  } else if (!this.fixed && count < this.fullStream.remaining && count < _.min(_.pluck(this.bindStreams, 'cost'))) {
    bindStream = new Stream.BindingStream(count, this.pattern, v, this._options);
    // bindStream.feed(vals); // need to add everything so far since we are starting a new stream
    bindStream.feed(completeBindings);
    this.bindStreams[v] = bindStream;
  }
  //console.timeEnd("UPDATE FEED");

  // TODO: download stream debug data (since we don't have a feed call for these)
  if (this.activeStream === this.fullStream)
    this.activeStream.logger.info("UPDATE ended:" + this.activeStream.ended + ", remaining:" + this.activeStream.remaining + ", cost:" + this.activeStream.cost + ", count:" + this.activeStream.count + ", costRemaining:" + this.activeStream.costRemaining);

  // stabilize stream
  // TODO: maybe addStabilize?
  if (bindStream) {
    //console.time("UPDATE STABILIZE");
    // TODO: this might start getting really expensive if we can't stabilize
    bindStream.stabilize(function () {
      // bindStream.updateRemaining(remaining);
      bindStream.updateRemaining(estimate - _.size(completeBindings));
      //console.timeEnd("UPDATE STABILIZE");
      callback();
    });
  } else {
    callback();
  }
};

Node.prototype._costModifier = function (stream) {
  if (1)
    return 1; // TODO: not sure if I want to keep this function
  if (!_.isFinite(stream.cost))
    return 1;

  var finiteStreams = _.filter(this.streams(), function (stream) { return _.isFinite(stream.cost); });
  if (_.size(finiteStreams) === 1)
    return 1;

  var allCosts = ClusteringUtil.sum(_.filter(_.pluck(finiteStreams, 'cost'), _.isFinite));
  // goes from 1 to Infinity
  //return Math.exp(1/(1-(_.size(finiteStreams)-1)*stream.cost/allCosts) - 1);
  return Math.pow(1/(1-(_.size(finiteStreams)-1)*stream.cost/allCosts), 2);
};

Node.prototype.cost = function () {
  if (this.ended())
    return Infinity;

  var self = this;
  return _.min(_.map(this.streams(), function (stream) { return stream.costRemaining * self._costModifier(stream); }));
};

Node.prototype.spend = function (cost) {
  var self = this;
  // TODO: good idea to only spend on active stream?
  var mod = self._costModifier(this.activeStream);
  if (!self.ended())
    this.activeStream.logger.info("modifier " + mod + " costs " + this.activeStream.cost + " remaining " + this.activeStream.costRemaining + " spend " + cost/mod);
  this.activeStream.spend(cost/mod);
//  _.each(this.streams(), function (stream) {
//    var mod = self._costModifier(stream);
//    if (!self.ended())
//      stream.logger.info("modifier " + mod + " costs " + stream.cost + " remaining " + stream.costRemaining + " spend " + cost/mod);
//    stream.spend(cost/mod);
//  });
};

Node.prototype.read = function (callback) {
  var self = this;
  // TODO: using one stream increases cost of others to make sure we don't have simultaneous streams?
  //var minStream = _.min(this.streams(), 'costRemaining');
  var minStream = _.min(this.streams(), 'cost');
  var switchAdd = [];
  var switchRemove = [];
  // TODO: problem, should actually be changed during 'update'
  if (minStream !== this.activeStream) {
    // we switched streams, update add/remove
    // TODO: is this actually a good idea?
    // TODO: reset costs?
    switchAdd = _.reject(minStream.triples, _.partial(ClusteringUtil.containsObject, this.activeStream.triples));
    switchRemove = _.filter(this.activeStream.triples, _.partial(ClusteringUtil.containsObject, this.activeStream.triples));
    this.logger.info("SWITCH STREAM");
    this.activeStream = minStream;
  }
  minStream.read(function (buffer, remove) {
    remove = remove || [];
    // TODO: can have doubles, should sort
    self.triples = self.triples.concat(buffer);
    // TODO: still don't know whether I should return triples or bindings
    callback(buffer.concat(switchAdd), remove.concat(switchRemove));
  });
};

Node.prototype.count = function (v) {
  // TODO: use stream triples
  if (this.ended())
    return _.size(this.triples);
  // TODO: nr of unique values is lower if there are multiple variables...
  return Math.min(this.fullStream.count, this.bindStreams[v] ? this.bindStreams[v].count : Infinity);
};

Node.prototype.remaining = function (v) {
  if (this.ended())
    return 0;
  return Math.min(this.fullStream.remaining, this.bindStreams[v] ? this.bindStreams[v].remaining : Infinity);
};

Node.prototype.ended = function () {
  return _.any(this.streams(), 'ended');
};

Node.prototype.supplies = function (v) {
  if (!_.contains(ClusteringUtil.getVariables(this.pattern), v))
    return false;
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
