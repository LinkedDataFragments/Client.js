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
  util = require('util');

function Stream (cost, pattern)
{
  this.cost = cost;
  this.costRemaining = cost;
  this.pattern = pattern; // we (usually) don't want multiple streams on the same pattern, this allows for identification
  this.vars = ClusteringUtil.getVariables(pattern);
  this.ended = false;
  this.triples = [];
  this.tripleCount = 0;
  this.matchRates = {};
  this.estimates = {};
}

Stream.prototype.read = function (callback) {
  throw new Error('Not implemented yet.');
};

Stream.prototype.spend = function (cost) {
  this.costRemaining -= cost;
};

///////////////////////////// DownloadStream /////////////////////////////
function DownloadStream (pattern, count, options) {
  Stream.call(this, count/100, pattern); // TODO: pagesize

  this.logger = new Logger("Stream " + rdf.toQuickString(pattern));
  //this.logger.disable();

  this._iterator = new TriplePatternIterator(Iterator.single({}), pattern, options);
  this.remaining = count;
  this.count = count;
}
util.inherits(DownloadStream, Stream);

DownloadStream.prototype.read = function (callback) {
  if (this._iterator.ended)
    return setImmediate(function () { callback([]); });

  var self = this;
  var pageSize = 100; // TODO: real pagesize
  var buffer = [];
  var iterator = this._iterator;iterator.setMaxListeners(1000); // TODO: listeners
  iterator.on('data', addTriple);
  iterator.on('end', end);
  function addTriple (val) {
    buffer.push(rdf.applyBindings(val, self.pattern));
    if (buffer.length >= pageSize || iterator.ended) {
      iterator.removeListener('data', addTriple);
      iterator.removeListener('end', end);
      self.ended |= iterator.ended;
      self.remaining -= buffer.length;
      self.triples = self.triples.concat(buffer);
      self.tripleCount += _.size(buffer);
      setImmediate(function (){ callback(buffer); });
    }
  }
  // TODO: this doesn't always get called?
  function end () {
    self.remaining = 0;
    self.ended = true;
    self.triples = self.triples.concat(buffer);
    self.tripleCount += _.size(buffer);
    setImmediate(function (){ callback(buffer); });
  }

  this.cost = Math.max(0, this.remaining - pageSize)/pageSize;
  this.costRemaining = this.cost; // reset since we did a read
};

DownloadStream.prototype.close = function () {
  this._iterator.close();
};

///////////////////////////// BindingStream /////////////////////////////
function BindingStream (cost, pattern, bindVar, options) {
  Stream.call(this, cost, pattern);

  this.logger = new Logger("Stream " + rdf.toQuickString(pattern) + " (" + bindVar + ")");
  //this.logger.disable();

  this.bindVar = bindVar;
  this._options = options;
  this._bindings = [];
  this.results = [];
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
  if (_.isEmpty(results))
    return Infinity;
  // TODO: this is not really correct since we don't take pages into account, already add 1 to take into account empty pages still being a call
  return ClusteringUtil.sum(_.map(results, function (result) { return Math.max(1, _.first(_.values(result))); } )) / _.size(results);
};

BindingStream.prototype.isStable = function () {
  if (_.isEmpty(this._bindings)) // TODO: this._gotAllData &&
    return true;
  if (_.size(this.results) < 4)  // TODO: 4 is kind of random
    return false;
  var prev = _.initial(this.results);
  var prevAvg = this.resultsPerBinding(prev);
  var prevMargin = 1.96/(2*Math.sqrt(_.size(this.results)))*prevAvg;
  var avg = this.resultsPerBinding();
  return prevMargin*prevAvg > Math.abs(prevAvg-avg);
};

// TODO: it actually becomes harder to stabilize if we have more values ...
BindingStream.prototype.addBinding = function (callback) {
  var self = this;
  // we use a random index to try and reduce the effect of sorting on the data (if any)
  // TODO: not using random to debug more easily
  var idx = 0; //_.random(_.size(this._bindings)-1);
  var bindingVal = _.first(this._bindings.splice(idx, 1));
  var binding = _.object([[this.bindVar, bindingVal]]);
  var boundPattern = rdf.applyBindings(binding, this.pattern);
  var fragment = this._options.fragmentsClient.getFragmentByPattern(boundPattern);
  fragment.getProperty('metadata', function(metadata) {
    fragment.close();
    var stream = new DownloadStream(boundPattern, metadata.totalTriples, self._options);
    stream.bindVal = binding;
    self._streams.push(stream);
    self.results.push(_.object([[bindingVal, metadata.totalTriples]]));
    // advantage: quickly have a stable stream
    // disadvantage: only using data on the first page
    setImmediate(callback);
  });
};

BindingStream.prototype.stabilize = function (callback) {
  if (this.isStable())
    return callback(true);
  if (_.isEmpty(this._bindings))
    return callback(false);

  var self = this;
  this.addBinding(function () { self.stabilize(callback); });
};

// TODO: if streams have < 100 results, the results are actually already cached, should make use of this (also for >100 streams if we edit the ldf parser?)
BindingStream.prototype.read = function (callback, _recursive) {
  if (this.ended || _.isEmpty(this._bindings) && _.isEmpty(this._streams))
    return setImmediate(function () { callback([]); });

  var self = this;
  // always add at least 1 new binding if possible to update the stability
  if ((!_recursive || !this.isStable()) && !_.isEmpty(this._bindings)) {
    this.addBinding(function () { self.read(callback, true); });
  } else if (!_.isEmpty(this._streams)){
    var stream = _.first(this._streams);
    stream.read(function (buffer) {
      if (stream.ended)
        self._streams.shift();
      var remove = [];
      // there were no matches for this binding so it should be removed
      if (stream.count === 0)
        remove.push(_.object([[self.bindVar, stream.bindVal]]));

      self.cost -= _.size(buffer);
      self.costRemaining = self.cost;
      if (self.remaining <= 0 && _.isEmpty(self._streams) && _.isEmpty(self._bindings))
        self.ended = true;

      self.triples = self.triples.concat(buffer);
      self.tripleCount += _.size(buffer);
      setImmediate(function () { callback(buffer, remove); });
    });
  }
};

BindingStream.prototype.feed = function (add, remove) {
  add = add || [];
  remove = remove || [];
  var self = this;
  var resultBindings = _.map(this.results, function (result) { return _.first(_.keys(result)); }) ;
  // don't add elements we already added before
  add = _.difference(add, resultBindings);
  this._bindings = _.union(this._bindings, add);
  var groupedResults = _.groupBy(this.results, function (count, binding) {
    return _.contains(remove, binding) ? "remove" : "keep";
  });
  groupedResults.remove = groupedResults.remove || [];
  groupedResults.keep = groupedResults.keep || [];
  this.results = groupedResults.keep;
  // remove unneeded streams
  this._streams = _.filter(this._streams, function (stream) {
    var remove = _.contains(remove, stream.bindVal);
    if (remove) stream.close();
    return !remove;
  });
  if (!_.isEmpty(remove)) {
    var bindPos = _.first(["subject", "predicate", "object"], function (pos) { return self.pattern[pos] === self.bindVar; });
    this.triples = _.filter(this.triples, function (triple) { return !_.contains(remove, triple[bindPos]); });
    this.tripleCount = _.size(this.triples); // TODO: how to not be dependent on triple size (use results and stream counts), although, if we don't remove anything from the DB this is not necessary
  }
  this.logger.info("FEED add: " + _.size(add), ", remove: " + _.size(remove) + ", results: " + _.size(this.results) + ", streams: " + _.size(this._streams) + ", bindings: " + _.size(this._bindings) + ", triples: " + _.size(this.triples));
};

BindingStream.prototype.isHungry = function () {
  return _.isEmpty(this._streams) && _.isEmpty(this._bindings) && !this.ended;
};

BindingStream.prototype.updateRemaining = function (remaining) {
  // TODO: maybe let ended streams restart again
  this.ended = _.isEmpty(this._bindings) && _.every(this._streams, 'ended') && remaining === 0;
  this._gotAllData = remaining <= 0;

  if (!this.isStable()) {
    this.remaining = Infinity;
    this.cost = Infinity;
    this.costRemaining = Infinity;
    this.count = Infinity;
    return;
  }

  this.remaining = ClusteringUtil.sum(this._streams, 'remaining');
  // TODO: problem if 0 * infinity
  this.remaining += (remaining + _.size(this._bindings)) * this.resultsPerBinding();

  var oldCost = this.cost;
  // TODO: ceiling the entire block will give wrong results for small results per binding, ceiling the final block can potentially double the value
  this.cost = ClusteringUtil.sum(_.map(this._streams, function (stream) { return Math.ceil(stream.remaining/100); })); // TODO: pagesize
  this.cost += (remaining + _.size(this._bindings)) * Math.ceil(this.resultsPerBinding()/100); // TODO: pageSize
  var diff = oldCost < Infinity ? this.cost - oldCost : 0;
  this.costRemaining = Math.min(this.cost, this.costRemaining + diff); // if cost suddenly increases, so should costRemaining (or lowers)

  this.count = ClusteringUtil.sum(_.map(this.results, function (result) { return _.first(_.values(result)); }));
  this.count += (remaining + _.size(this._bindings)) * this.resultsPerBinding(); // _streams are already included in results

  this.matchRate = _.size(_.filter(this.results, function (result) { return _.first(_.values(result)) > 0; })) / _.size(this.results);

  this.logger.info("UPDATE remaining input:" + remaining + ", ended:" + this.ended + ", remaining:" + this.remaining + ", cost:" + this.cost + ", count:" + this.count + ", costRemaining:" + this.costRemaining + ", matchRate:" + this.matchRate);
};


module.exports = Stream;
Stream.DownloadStream = DownloadStream;
Stream.BindingStream = BindingStream;