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
  this.logger.disable();
  this.fullStream = new Stream.DownloadStream(pattern, count, options);
  this.activeStream = this.fullStream;
  this.bindStreams = {};
  this.triples = []; // TODO: store triples per stream?
  this.store = _.object(ClusteringUtil.getVariables(pattern), _.map(ClusteringUtil.getVariables(pattern), function (v) { return {}; }));

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

Node.prototype.update = function (v, estimate, completeBindings, updatedNode, callback) {
  var pos = this.getVariablePosition(v);
  if (!pos || this.ended())
    return callback();
  var bindStream = this.bindStreams[v];
  var self = this;

  // switch activestream if needed
  var minStream = _.min(this.streams(), 'cost');
  // can't have opinions if the cost is infinite (not enough results yet to change)
//  if (_.isFinite(this.activeStream.cost) && minStream !== this.activeStream) {
//    // we switched streams, update add/remove
//    // TODO: is this actually a good idea?
//    // TODO: reset costs?
////    switchAdd = _.reject(minStream.triples, _.partial(ClusteringUtil.containsObject, this.activeStream.triples));
////    switchRemove = _.filter(this.activeStream.triples, _.partial(ClusteringUtil.containsObject, this.activeStream.triples));
////    this.logger.info("SWITCH STREAM " + this.activeStream.bindVar + " -> " + minStream.bindVar);
////    this.activeStream = minStream;
////    this.adds = this.adds.concat(switchAdd);
////    this.removes = this.removes.concat(switchRemove);
//    // TODO: try to find which cluster provides the best solution
//    // TODO: problem if active stream is download stream since those are also used in paths
//    // TODO: this is also quite expensive
//    var attempts = this.switchAttempts[minStream.bindVar];
//    if (attempts === undefined) {
//      this.switchAttempts[minStream.bindVar] = 0;
//      this.switchCountdown[minStream.bindVar] = 1;
//    }
//    if (this.switchCountdown[minStream.bindVar] > 0) {
//      --this.switchCountdown[minStream.bindVar];
//    } else {
//      ++this.switchAttempts[minStream.bindVar];
//      this.switchCountdown[minStream.bindVar] = this.switchAttempts[minStream.bindVar]*this.switchAttempts[minStream.bindVar];
//      // TODO: need to supply to update values, but this is not really the right place, also timing
//      _.each(this.activeSupplyVars(), function (v) { self.DEBUGclusters[v].supply(function (){}); });
//      var filteredClusters = _.filter(_.values(this.DEBUGclusters), function (cluster) { return self.bindStreams[cluster.v] && self.bindStreams[cluster.v] !== self.activeStream; });
//      _.each(filteredClusters, function (cluster) {
//        cluster.nodes = _.without(cluster.nodes, self);
//      });
//      var bindCosts = {};
//      var realStream = this.activeStream;
//      var delayedCallback = _.after(_.size(filteredClusters), function () {
//        var minVar = _.min(_.keys(bindCosts), function (v) { return bindCosts[v]; });
//        if (minVar !== self.activeStream.bindVar && _.isFinite(bindCosts[minVar]) && bindCosts[minVar] < realStream.cost) {
//          self.logger.info("SWITCH STREAM " + self.activeStream.bindVar + " -> " + minStream.bindVar);
//          self.logger.info(bindCosts);
//          self.activeStream = self.bindStreams[minVar];
//        } else {
//          self.logger.info("FAILED " + minStream.bindVar);
//          self.activeStream = realStream;
//        }
//      });
//      _.each(filteredClusters, function (cluster) {
//        self.activeStream = self.bindStreams[cluster.v];
//        cluster.matchSuppliers(cluster.suppliers(), function (completeBindings, estimate, matchRates, estimates) {
//          cluster.nodes.push(self);
//          bindCosts[cluster.v] = estimate * Math.ceil(self.bindStreams[cluster.v].resultsPerBinding() / 100); // TODO: pagesize
//          delayedCallback();
//        });
//      });
//    }
//  }
  // TODO: need way to check bound streams even if activestream is download stream (without disconnecting parts)
  // TODO: ^ can be done by switching activeStream, but executing this block on all streams gets expensive, should have way to check if it is necessary (prev block?)
  if (bindStream && (bindStream === this.activeStream)) {
    var idx = this.DEBUGclusters[v].DEBUGcontroller.nodes.indexOf(this);
    // TODO: should make sure suppliers are always before consumers (might already be the case tbh)
    var nodes = this.DEBUGclusters[v].DEBUGcontroller.nodes.slice(0, idx);
    if (_.contains(nodes, updatedNode)) {
      this.DEBUGclusters[v].matchSuppliers(nodes, function (specificBindings, ownEstimate, ownMatchRates, ownEstimates) {
        completeBindings = specificBindings;
        self.logger.info('MATCHED: ' + completeBindings.length);
        estimate = ownEstimate;
        updateBindStream();
      });
    } else {
      updateBindStream();
    }
  } else {
    updateBindStream();
  }

  //updateBindStream();
  function updateBindStream () {
    var DEBUGtimer = self.DEBUGclusters[ClusteringUtil.getVariables(self.pattern)[0]].DEBUGcontroller.DEBUGtimer;
    var start = new Date();
    if (bindStream) {
      bindStream.feed(completeBindings);
      // TODO: not sure if this is a good idea, let's find out
    } else { // if (!this.fixed && count < this.fullStream.remaining && count < _.min(_.pluck(this.bindStreams, 'cost'))) {
      bindStream = new Stream.BindingStream(estimate, self.pattern, v, self._options);
      bindStream.feed(completeBindings);
      self.bindStreams[v] = bindStream;
    }
    DEBUGtimer.postread_feed += new Date() - start;

    // TODO: download stream debug data (since we don't have a feed call for these)
    if (self.activeStream === self.fullStream)
      self.activeStream.logger.info("UPDATE ended:" + self.activeStream.ended + ", remaining:" + self.activeStream.remaining + ", cost:" + self.activeStream.cost + ", count:" + self.activeStream.count + ", costRemaining:" + self.activeStream.costRemaining);

    // stabilize stream
    // TODO: maybe addStabilize?
    // TODO: problem stabilizing with values that were downloaded with the downloadstream of this node?
    if (bindStream) {
      start = new Date();
      // TODO: this might start getting really expensive if we can't stabilize -> DING DING DING
      // TODO: DEBUG: lot of unnecessary http calls, see what happens if we only stabilize main stream
      if (bindStream === self.activeStream) {
        bindStream.stabilize(function () {
          bindStream.updateRemaining(estimate - _.size(completeBindings));
          DEBUGtimer.postread_stabilize += new Date() - start;
          callback();
        });
      } else {
        bindStream.updateRemaining(estimate - _.size(completeBindings));
        DEBUGtimer.postread_stabilize += new Date() - start;
        callback();
      }
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
  this.activeStream.read(function (buffer, remove) {
    remove = remove || [];
    self.triples = self.activeStream.triples;
    setImmediate(callback(buffer));
  }, function (preReadTime, readTime, postReadTime, addPre, addRead, addPost) {
    var DEBUGtimer = self.DEBUGclusters[ClusteringUtil.getVariables(self.pattern)[0]].DEBUGcontroller.DEBUGtimer;
    DEBUGtimer.read_pre += preReadTime;
    DEBUGtimer.read_read += readTime;
    DEBUGtimer.read_post += postReadTime;
    DEBUGtimer.read_add_pre += addPre;
    DEBUGtimer.read_add_read += addRead;
    DEBUGtimer.read_add_post += addPost;
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
