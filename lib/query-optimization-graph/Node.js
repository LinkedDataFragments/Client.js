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
  this.fullStream = new Stream.DownloadStream(pattern, count, options);
  this.bindStreams = {};
  this.triples = [];
  this.logger = new Logger("Node " + rdf.toQuickString(pattern));
}

Node.prototype.streams = function () {
  return _.values(this.bindStreams).concat([this.fullStream]);
};

Node.prototype.getVariablePosition = function (variable) {
  if (this.pattern.subject === variable) return 'subject';
  if (this.pattern.predicate === variable) return 'predicate';
  if (this.pattern.object === variable) return 'object';
  return null;
};

Node.prototype.update = function (v, count, vals, added, removed, remaining, callback) {
  // TODO: problem if added is by fulldownload stream of this node (and then gets added to other streams)
  // TODO: direct fix checking on source of added is fine, but can potentially still be a problem if we have a cycle
  // TODO: just checking if we got that value from full download stream is not enough (because there might still be mappings left we don't have!)
  var pos = this.getVariablePosition(v);
  if (!pos || this.ended())
    return callback();
  // TODO: first attempt to fix this double data problem
  var localVals = _.pluck(this.triples, pos);
  added = _.difference(added, localVals);
  vals = _.difference(vals, localVals);
  var bindStream = this.bindStreams[v];
  if (bindStream) {
    bindStream.feed(added, removed);
  } else if (count < this.fullStream.remaining && count < _.min(_.pluck(this.bindStreams, 'cost'))) {
    bindStream = new Stream.BindingStream(count, this.pattern, v, this._options);
    bindStream.feed(vals); // need to add everything so far since we are starting a new stream
    this.bindStreams[v] = bindStream;
  }

  // stabilize stream
  if (bindStream) {
    // TODO: this might start getting really expensive if we can't stabilize
    bindStream.stabilize(function () {
      bindStream.updateRemaining(remaining);
      callback();
    });
  } else {
    callback();
  }
};

Node.prototype._costModifier = function (stream) {
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
  _.each(this.streams(), function (stream) {
    var mod = self._costModifier(stream);

    if (!self.ended())
      stream.logger.info("modifier " + mod + " costs " + stream.cost + " remaining " + stream.costRemaining + " spend " + cost/mod);
    stream.spend(cost/mod);
  });
};

Node.prototype.read = function (callback) {
  var self = this;
  // TODO: using one stream increases cost of others to make sure we don't have simultaneous streams?
  var minStream = _.min(this.streams(), 'costRemaining');
  minStream.read(function (buffer, remove, bindVal) {
    // TODO: can have doubles, should sort
    var appliedBindings = _.map(buffer, function (binding) {
      if (minStream.bindVar && bindVal) {
        binding = _.clone(binding); // don't want to overwrite object
        binding[minStream.bindVar] = bindVal;
      }
      return rdf.applyBindings(binding, self.pattern);
    });
    self.triples = self.triples.concat(appliedBindings);
    // TODO: still don't know whether I should return triples or bindings
    callback(buffer, remove);
  });
};

Node.prototype.count = function (v) {
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

module.exports = Node;
