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
}

Node.prototype.getVariablePosition = function (variable) {
  if (this.pattern.subject === variable) return 'subject';
  if (this.pattern.predicate === variable) return 'predicate';
  if (this.pattern.object === variable) return 'object';
  return null;
};

Node.prototype.update = function (v, count, vals, added, removed, remaining, callback) {
  var pos = this.getVariablePosition(v);
  if (!pos || this.ended())
    return callback();
  var bindStream = this.bindStreams[v];
  if (bindStream) {
    bindStream.feed(added, removed);
    bindStream.updateRemaining(remaining);
  } else if (count < this.fullStream.count && count < _.min(_.pluck(this.bindStreams, 'cost'))) {
    bindStream = new Stream.BindingStream(count, this.pattern, v, this._options);
    bindStream.feed(vals); // need to add everything so far since we are starting a new stream
    bindStream.stabilize(function () {
      bindStream.updateRemaining(remaining);
      callback();
    });
    this.bindStreams[v] = bindStream;
    return;
  }

  callback();
};

Node.prototype.cost = function () {
  if (this.ended())
    return Infinity;

  var minNode = _.min(this.bindStreams, 'costRemaining');
  if(minNode && _.isObject(minNode) && minNode.cost > this.fullStream.cost) {
    console.error('uh oh');
  }

  return Math.min(this.fullStream.costRemaining, _.min(_.pluck(this.bindStreams, 'costRemaining')));
};

Node.prototype.spend = function (cost) {
  this.fullStream.spend(cost);
  _.each(this.bindStreams, function (stream) { stream.spend(cost); });
};

Node.prototype.read = function (callback) {
  var self = this;
  // TODO: using one stream increases cost of others to make sure we don't have simultaneous streams?
  var minStream = _.min(_.values(this.bindStreams).concat([this.fullStream]), 'costRemaining');
  _.each(_.values(this.bindStreams).concat([this.fullStream]), function (stream) {
    if (stream !== minStream) stream.costRemaining += minStream.cost; // TODO: this is obviously not the best way but I'm tired
  });
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
  var count = this.fullStream.count;
  var bindCount = this.bindStreams[v] ? this.bindStreams[v].count : Infinity;
  return Math.min(count, bindCount);
};

Node.prototype.remaining = function (v) {
  if (this.ended())
    return 0;
  var remaining = this.fullStream.remaining;
  var bindRemaining = this.bindStreams[v] ? this.bindStreams[v].remaining : Infinity;
  return Math.min(remaining, bindRemaining);
};

Node.prototype.ended = function () {
  return _.any(_.values(this.bindStreams).concat([this.fullStream]), 'ended');
};

module.exports = Node;
