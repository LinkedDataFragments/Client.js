/**
 * Created by joachimvh on 11/09/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
  Iterator = require('../iterators/Iterator'),
  Logger = require('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil'),
  util = require('util');

function Stream(cost, pattern, loggername)
{
  this.logger = new Logger(loggername);
  this.logger.disable();

  this.cost = cost;
  this.costRemaining = cost;
  this.pattern = pattern;
  this.vars = rdf.getVariables(pattern);
  this.ended = false;
  this.triples = [];
  this.tripleCount = 0;
}

Stream.prototype.read = function (callback) {
  throw new Error('Not implemented yet.');
};

Stream.prototype.spend = function (cost) {
  this.costRemaining -= cost;
};

///////////////////////////// DownloadStream /////////////////////////////
function DownloadStream(pattern, count, options) {
  Stream.call(this, count / 100, pattern, "Stream " + rdf.toQuickString(pattern)); // TODO: pagesize

  this._iterator = new TriplePatternIterator(Iterator.single({}), pattern, options);
  this._iterator.setMaxListeners(1000);
  this.remaining = count;
  this.count = count;
  this._buffer = [];

  var self = this;
  this._iterator.on('end', function () {
    self.count = self.tripleCount;
    self.ended = true;
    self.remaining = 0;
  });
}
util.inherits(DownloadStream, Stream);

DownloadStream.prototype.read = function (callback, timingCallback) {
  if (this.ended)
    return setImmediate(function () { callback([]); });

  var self = this;
  var pageSize = 100; // TODO: real pagesize
  var buffer = [];
  var iterator = this._iterator;
  var start = new Date();
  iterator.on('data', addTriple);
  iterator.on('end', end);
  function addTriple(val) {
    buffer.push(rdf.applyBindings(val, self.pattern));
    if (buffer.length >= pageSize || iterator.ended) {
      iterator.removeListener('data', addTriple);
      iterator.removeListener('end', end);
      addBuffer(buffer);
    }
  }
  function end() {
    addBuffer(buffer);
  }
  var added = false;
  function addBuffer(buffer) {
    if (added)
      return;
    added = true;
    self.triples = self.triples.concat(buffer);
    self.tripleCount += _.size(buffer);
    // TODO: find out why the algorithm didn't continue before I made this change
    if (self.tripleCount > self.count)
      self.count = self.tripleCount + (self.ended ? 0 : 1); // wrong server estimation
    self.remaining = self.count - self.tripleCount;
    setImmediate(function () { callback(buffer); });
  }

  this.cost = Math.max(0, this.remaining - pageSize) / pageSize;
  this.costRemaining = this.cost; // reset since we did a read
};

///////////////////////////// BindingStream /////////////////////////////
function BindingStream(cost, pattern, bindVar, options) {
  Stream.call(this, cost, pattern, "Stream " + rdf.toQuickString(pattern) + " (" + bindVar + ")");

  this.bindVar = bindVar;
  this._options = options;
  this._bindings = [];
  this.results = [];
  this.resultVals = []; // need this for feeding
  this._streams = [];
  this._gotAllData = false;
  this.ended = false; // it is important updateRemaining gets called at least once to make sure this value is correct for empty streams!
  this.remaining = Infinity;
  this.cost = Infinity;
  this.costRemaining = Infinity;
  this.count = Infinity;
  this.matchRate = 1;
}
util.inherits(BindingStream, Stream);

BindingStream.prototype.resultsPerBinding = function (results) {
  results = results || this.results;
  if (results.length === 0)
    return this._gotAllData ? 0 : Infinity;
  var sum = 0;
  for (var i = 0; i < results.length; ++i)
    sum += Math.max(1, results[i].count);
  return sum / results.length;
};

BindingStream.prototype.isStable = function () {
  if (this._gotAllData && _.isEmpty(this._bindings))
    return true;
  if (this.results.length < 4)
    return false;
  var prev = this.results[0];
  var prevAvg = this.resultsPerBinding(prev);
  var prevMargin = 0.98 / Math.sqrt(this.results.length) * prevAvg;
  var avg = this.resultsPerBinding();
  return prevMargin * prevAvg > Math.abs(prevAvg - avg);
};

BindingStream.prototype.addBinding = function (callback) {
  var start = new Date();
  var self = this;
  var bindingVal = this._bindings.shift();
  var binding = _.object([[this.bindVar, bindingVal]]);
  var boundPattern = rdf.applyBindings(binding, this.pattern);
  var fragment = this._options.fragmentsClient.getFragmentByPattern(boundPattern);
  fragment.getProperty('metadata', function (metadata) {
    fragment.close();
    var stream = new DownloadStream(boundPattern, metadata.totalTriples, self._options);
    stream.bindVal = bindingVal;
    self._streams.push(stream);
    self.results.push({binding: bindingVal, count: metadata.totalTriples});
    self.resultVals.push(bindingVal);
    setImmediate(callback);
  });
};

BindingStream.prototype.stabilize = function (callback) {
  if (this.isStable())
    return callback(true);
  if (this._bindings.length <= 0)
    return callback(false);

  var self = this;
  this.addBinding(function () { self.stabilize(callback); });
};

BindingStream.prototype.read = function (callback, _recursive) {
  if (this.ended || this._bindings.length === 0 && this._streams.length === 0)
    return setImmediate(function () { callback([]); });

  var self = this;
  // always add at least 1 new binding if possible to update the stability
  if ((!_recursive || !this.isStable() || this._streams.length === 0) && this._bindings.length > 0) {
    this.addBinding(function () { self.read(callback, true); });
  } else if (this._streams.length > 0) {
    var stream = this._streams[0];
    stream.read(function (buffer) {
      if (stream.ended)
        self._streams.shift();

      self.cost -= buffer.length;
      self.costRemaining = self.cost;
      if (self.remaining <= 0 && self._streams.length === 0 && self._bindings.length === 0)
        self.ended = true;

      self.triples = self.triples.concat(buffer);
      self.tripleCount += buffer.length;
      setImmediate(function () { callback(buffer); });
    });
  }
};

BindingStream.prototype.feed = function (bindings) {
  // don't add elements we already added before
  var cacheExclude = {};
  var cache = {};
  var i;
  for (i = 0; i < this.resultVals.length; ++i)
    cacheExclude[this.resultVals[i]] = 1;
  for (i = 0; i < bindings.length; ++i)
    if (!cacheExclude[bindings[i]])
      cache[bindings[i]] = 1;
  for (i = 0; i < this._bindings.length; ++i)
    cache[this._bindings[i]] = 1;
  this._bindings = Object.keys(cache);
  this.logger.debug("FEED results: " + this.results.length + ", streams: " + this._streams.length + ", bindings: " + this._bindings.length + ", triples: " + this.tripleCount);
};

BindingStream.prototype.isHungry = function () {
  return this._streams.length === 0 && this._bindings.length === 0 && !this.ended;
};

BindingStream.prototype.updateRemaining = function (remaining) {
  this.ended = this._bindings.length === 0 && _.every(this._streams, 'ended') && remaining === 0;
  this._gotAllData = remaining <= 0;

  if (!this.isStable()) {
    this.remaining = Infinity;
    this.cost = Infinity;
    this.costRemaining = Infinity;
    this.count = Infinity;
    return;
  }

  this.remaining = ClusteringUtil.sum(this._streams, 'remaining');
  this.remaining += (remaining + _.size(this._bindings)) * this.resultsPerBinding();

  var oldCost = this.cost;
  this.cost = ClusteringUtil.sum(_.map(this._streams, function (stream) { return Math.ceil(stream.remaining / 100); })); // TODO: pagesize
  this.cost += (remaining + _.size(this._bindings)) * Math.ceil(this.resultsPerBinding() / 100); // TODO: pageSize
  var diff = oldCost < Infinity ? this.cost - oldCost : 0;
  this.costRemaining = Math.min(this.cost, this.costRemaining + diff); // if cost suddenly increases, so should costRemaining (or lowers)

  this.count = ClusteringUtil.sum(_.pluck(this.results, 'count'));
  this.count += (remaining + _.size(this._bindings)) * this.resultsPerBinding(); // _streams are already included in results

  this.matchRate = _.size(_.filter(this.results, function (result) { return result.count > 0; })) / _.size(this.results);

  this.logger.debug("UPDATE remaining input:" + remaining + ", ended:" + this.ended + ", remaining:" + this.remaining + ", cost:" + this.cost + ", count:" + this.count + ", costRemaining:" + this.costRemaining + ", matchRate:" + this.matchRate);
};


module.exports = Stream;
Stream.DownloadStream = DownloadStream;
Stream.BindingStream = BindingStream;