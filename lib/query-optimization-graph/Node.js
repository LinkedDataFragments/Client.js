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
  this.removes = []; // TODO: I'm not sure I'll actually need this
  this.adds = []; // TODO: ^

  this.DEBUGclusters = {}; // TODO: too much interlinking
  this.switchAttempts = {};
  this.switchCountdown = {};
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
  // TODO: first attempt to fix this double data problem <- still a problem?
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
  var self = this;

  // switch activestream if needed
  var minStream = _.min(this.streams(), 'cost');
  var switchAdd = [];
  var switchRemove = [];
  // TODO: not switching if we are a supplier is really wrong, should check what the effect is if we remove this stream (removing and calling supply?)
  // TODO: atm this makes switching impossible
  // TODO: how to determine impact of switch?
  // can't have opinions if the cost is infinite (not enough results yet to change)
  if (_.isFinite(this.activeStream.cost) && minStream !== this.activeStream) {
    // we switched streams, update add/remove
    // TODO: is this actually a good idea?
    // TODO: reset costs?
//    switchAdd = _.reject(minStream.triples, _.partial(ClusteringUtil.containsObject, this.activeStream.triples));
//    switchRemove = _.filter(this.activeStream.triples, _.partial(ClusteringUtil.containsObject, this.activeStream.triples));
//    this.logger.info("SWITCH STREAM " + this.activeStream.bindVar + " -> " + minStream.bindVar);
//    this.activeStream = minStream;
//    this.adds = this.adds.concat(switchAdd);
//    this.removes = this.removes.concat(switchRemove);
    // TODO: try to find which cluster provides the best solution
    // TODO: problem if active stream is download stream since those are also used in paths
    // TODO: this is also quite expensive
    var attempts = this.switchAttempts[minStream.bindVar];
    if (attempts === undefined) {
      this.switchAttempts[minStream.bindVar] = 0;
      this.switchCountdown[minStream.bindVar] = 1;
    }
    if (this.switchCountdown[minStream.bindVar] > 0) {
      --this.switchCountdown[minStream.bindVar];
    } else {
      ++this.switchAttempts[minStream.bindVar];
      this.switchCountdown[minStream.bindVar] = this.switchAttempts[minStream.bindVar]*this.switchAttempts[minStream.bindVar];
      // TODO: need to supply to update values, but this is not really the right place, also timing
      _.each(this.activeSupplyVars(), function (v) { self.DEBUGclusters[v].supply(function (){}); });
      var filteredClusters = _.filter(_.values(this.DEBUGclusters), function (cluster) { return self.bindStreams[cluster.v] && self.bindStreams[cluster.v] !== self.activeStream; });
      _.each(filteredClusters, function (cluster) {
        cluster.nodes = _.without(cluster.nodes, self);
      });
      var bindCosts = {};
      var realStream = this.activeStream;
      var delayedCallback = _.after(_.size(filteredClusters), function () {
        var minVar = _.min(_.keys(bindCosts), function (v) { return bindCosts[v]; });
        if (minVar !== self.activeStream.bindVar && _.isFinite(bindCosts[minVar]) && bindCosts[minVar] < realStream.cost) {
          self.logger.info("SWITCH STREAM " + self.activeStream.bindVar + " -> " + minStream.bindVar);
          self.logger.info(bindCosts);
          self.activeStream = self.bindStreams[minVar];
        } else {
          self.logger.info("FAILED " + minStream.bindVar);
          self.activeStream = realStream;
        }
      });
      _.each(filteredClusters, function (cluster) {
        self.activeStream = self.bindStreams[cluster.v];
        cluster.matchSuppliers(cluster.suppliers(), function (completeBindings, estimate, matchRates, estimates) {
          cluster.nodes.push(self);
          bindCosts[cluster.v] = estimate * Math.ceil(self.bindStreams[cluster.v].resultsPerBinding() / 100); // TODO: pagesize
          delayedCallback();
        });
      });
    }
  }

  // TODO: new idea: calculate bindings per node, and use all nodes that come before it in ordering
  // TODO: don't need sparql queries if we only compare streams supplying the same var
  // TODO: lets start with only activestream and check the result
  if (bindStream === this.activeStream) {
    var idx = this.DEBUGclusters[v].DEBUGcontroller.nodes.indexOf(this);
    // TODO: should make sure suppliers are always before consumers (might already be the case tbh)
    var nodes = this.DEBUGclusters[v].DEBUGcontroller.nodes.slice(0, idx);
    // TODO: only take connected part?
    this.DEBUGclusters[v].matchSuppliers(nodes, function (specificBindings){
      completeBindings = specificBindings;
      updateBindStream();
    });
  } else {
    updateBindStream();
  }

  //debugCallback();
  function updateBindStream () {
    //console.time("UPDATE FEED");
    // TODO: use normal values if there are no completeBindings?
    if (bindStream) {
      //bindStream.feed(added, removed);
      // TODO: this is not the correct place to do this, should happen in stream itself
      var allStreamBindings = _.map(bindStream.results, function (result) { return _.first(_.keys(result)); }).concat(bindStream._bindings);
      var removedBindings = _.difference(allStreamBindings, completeBindings);
      bindStream.feed(completeBindings, removed.concat(removedBindings));
      // TODO: not sure if this is a good idea, let's find out
    } else { // if (!this.fixed && count < this.fullStream.remaining && count < _.min(_.pluck(this.bindStreams, 'cost'))) {
      bindStream = new Stream.BindingStream(count, self.pattern, v, self._options);
      // bindStream.feed(vals); // need to add everything so far since we are starting a new stream
      bindStream.feed(completeBindings);
      self.bindStreams[v] = bindStream;
    }
    //console.timeEnd("UPDATE FEED");

    // TODO: download stream debug data (since we don't have a feed call for these)
    if (self.activeStream === self.fullStream)
      self.activeStream.logger.info("UPDATE ended:" + self.activeStream.ended + ", remaining:" + self.activeStream.remaining + ", cost:" + self.activeStream.cost + ", count:" + self.activeStream.count + ", costRemaining:" + self.activeStream.costRemaining);

    // stabilize stream
    // TODO: maybe addStabilize?
    // TODO: problem stabilizing with values that were downloaded with the downloadstream of this node?
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
  this.activeStream.read(function (buffer, remove) {
    remove = remove || [];
    // TODO: can have doubles, should sort
    self.triples = self.triples.concat(buffer);
    // TODO: still don't know whether I should return triples or bindings
    setImmediate(callback(buffer.concat(self.adds), remove.concat(self.removes)));
    self.adds = [];
    self.removes = [];
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
