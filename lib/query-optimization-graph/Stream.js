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

function Stream (cost, pattern, loggername)
{
  this.logger = new Logger(loggername);
  this.logger.disable();

  this.cost = cost;
  this.costRemaining = cost;
  this.pattern = pattern; // we (usually) don't want multiple streams on the same pattern, this allows for identification
  this.vars = ClusteringUtil.getVariables(pattern);
  this.ended = false;
  this.triples = [];
  this.tripleCount = 0;
  this.matchRates = {};
  this.estimates = {};
  var self = this;
  _.each(this.vars, function (v) {
    self.matchRates[v] = 0;
    self.estimates[v] = Infinity;
  });
}

Stream.prototype.read = function (callback) {
  throw new Error('Not implemented yet.');
};

Stream.prototype.spend = function (cost) {
  this.costRemaining -= cost;
};

///////////////////////////// DownloadStream /////////////////////////////
function DownloadStream (pattern, count, options) {
  Stream.call(this, count/100, pattern, "Stream " + rdf.toQuickString(pattern)); // TODO: pagesize

  this._iterator = new TriplePatternIterator(Iterator.single({}), pattern, options);
  this._iterator.setMaxListeners(1000); // TODO: listeners
  this.remaining = count;
  this.count = count;
  this._buffer = [];

  var self = this;
  this._iterator.on('end', function () {
    self.ended = true;
    self.remaining = 0;
  });
}
util.inherits(DownloadStream, Stream);

DownloadStream.prototype.read = function (callback) {
  if (this.ended)
    return setImmediate(function () { callback([]); });

  var self = this;
  var pageSize = 100; // TODO: real pagesize
  var buffer = [];
  var iterator = this._iterator;
  iterator.on('data', addTriple);
  iterator.on('end', end);
  function addTriple (val) {
    buffer.push(rdf.applyBindings(val, self.pattern));
    if (buffer.length >= pageSize || iterator.ended) {
      iterator.removeListener('data', addTriple);
      iterator.removeListener('end', end);
      addBuffer(buffer);
    }
  }
  function end () {
    addBuffer(buffer);
  }
  var added = false;
  function addBuffer (buffer) {
    if (added)
      return;
    added = true;
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
  Stream.call(this, cost, pattern, "Stream " + rdf.toQuickString(pattern) + " (" + bindVar + ")");

  this.bindVar = bindVar;
  this.bindPos = _.filter(['subject', 'predicate', 'object'], function (pos) { return pattern[pos] === bindVar; })[0];
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
    return this._gotAllData ? 0 : Infinity;
  // TODO: this is not really correct since we don't take pages into account, already add 1 to take into account empty pages still being a call
  return ClusteringUtil.sum(_.map(results, function (result) { return Math.max(1, result.count); } )) / _.size(results);
};

BindingStream.prototype.isStable = function () {
  if (this._gotAllData && _.isEmpty(this._bindings)) // TODO: this._gotAllData &&
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
BindingStream.prototype.addBinding = function (callback, timingCallback) {
  var start = new Date();
  var self = this;
  // we use a random index to try and reduce the effect of sorting on the data (if any)
  // TODO: not using random to debug more easily
  var idx = 0; //_.random(_.size(this._bindings)-1);
  var bindingVal = _.first(this._bindings.splice(idx, 1));
  var binding = _.object([[this.bindVar, bindingVal]]);
  var boundPattern = rdf.applyBindings(binding, this.pattern);
  var fragment = this._options.fragmentsClient.getFragmentByPattern(boundPattern);
  var pre = new Date() - start;
  start = new Date();
  fragment.getProperty('metadata', function(metadata) {
    var read = new Date() - start;
    start = new Date();
    fragment.close();
    var stream = new DownloadStream(boundPattern, metadata.totalTriples, self._options);
    stream.bindVal = bindingVal;
    self._streams.push(stream);
    self.results.push({binding: bindingVal, count: metadata.totalTriples});
    // advantage: quickly have a stable stream
    // disadvantage: only using data on the first page
    var post = new Date() - start;
    if (timingCallback)
      timingCallback(pre, read, post);
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

BindingStream.prototype.read = function (callback, timingCallback, _recursive) {
  if (this.ended || _.isEmpty(this._bindings) && _.isEmpty(this._streams))
    return setImmediate(function () { callback([]); });

  if (!_recursive) {
    this.startRead = new Date();
    this.addPre = 0;
    this.addRead = 0;
    this.addPost = 0;
  }

  var self = this;
  // always add at least 1 new binding if possible to update the stability
  if ((!_recursive || !this.isStable()) && !_.isEmpty(this._bindings)) {
    this.addBinding(function () { self.read(callback, timingCallback, true); }, function (pre, read, post) {
      self.addPre += pre;
      self.addRead += read;
      self.addPost += post;
    });
  } else if (!_.isEmpty(this._streams)){
    var stream = _.first(this._streams);
    var preReadTime = new Date() - this.startRead;
    var start = new Date();
    stream.read(function (buffer) {
      var readTime = new Date() - start;
      start = new Date();
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
      var postReadTime = new Date() - start;
      if (timingCallback)
        timingCallback(preReadTime, readTime, postReadTime, self.addPre, self.addRead, self.addPost);
      setImmediate(function () { callback(buffer, remove); });
    });
  }
};

BindingStream.prototype.feed = function (bindings) {
  var self = this;
  var resultBindings = _.pluck(this.results, 'binding') ;
  // don't add elements we already added before
  var DEBUGoldSize = _.size(this._bindings);
  this._bindings = _.union(this._bindings,  _.difference(bindings, resultBindings));
  var groupedResults = _.groupBy(this.results, function (result) {
    return _.contains(bindings, result.binding) ? "keep" : "remove";
  });
  groupedResults.keep = groupedResults.keep || [];
  groupedResults.remove = groupedResults.remove || [];
  //this.results = groupedResults.keep; // TODO: do this in a way we don't get duplicate results
  // remove unneeded streams
  var removedBindings = _.pluck(groupedResults.remove, 'binding');
  if (removedBindings.length > 0) {
    //this.triples = _.reject(this.triples, function (triple) { return _.contains(removedBindings, triple[self.bindPos]); });
    _.each(removedBindings, function (binding) {
      var stream = _.find(self._streams, {bindVal: binding});
      // if we don't have a stream, that binding was already completely downloaded
      // TODO: these values are still in the triple store...
//    if (stream)
//      self.tripleCount -= stream.tripleCount;
//    else
//      self.tripleCount -= result.count;
    });
    // TODO: if we don't remove the results we also shouldn't remove the streams, else we might miss some bindings
    //this._streams = _.reject(this._streams, function (stream) {
    //  var shouldRemove = _.contains(removedBindings, stream.bindVal);
    //  if (shouldRemove) stream.close();
    //  return shouldRemove;
    //});
  }
  this.logger.info("FEED add: " + (_.size(this._bindings)-DEBUGoldSize), ", remove: " + _.size(groupedResults.remove) + ", results: " + _.size(this.results) + ", streams: " + _.size(this._streams) + ", bindings: " + _.size(this._bindings) + ", triples: " + this.tripleCount);
};

BindingStream.prototype.isHungry = function () {
  return _.isEmpty(this._streams) && _.isEmpty(this._bindings) && !this.ended;
};

BindingStream.prototype.updateRemaining = function (remaining) {
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
  this.remaining += (remaining + _.size(this._bindings)) * this.resultsPerBinding();

  var oldCost = this.cost;
  // TODO: ceiling the entire block will give wrong results for small results per binding, ceiling the final block can potentially double the value (100 <-> 101)
  this.cost = ClusteringUtil.sum(_.map(this._streams, function (stream) { return Math.ceil(stream.remaining/100); })); // TODO: pagesize
  this.cost += (remaining + _.size(this._bindings)) * Math.ceil(this.resultsPerBinding()/100); // TODO: pageSize
  var diff = oldCost < Infinity ? this.cost - oldCost : 0;
  this.costRemaining = Math.min(this.cost, this.costRemaining + diff); // if cost suddenly increases, so should costRemaining (or lowers)

  this.count = ClusteringUtil.sum(_.pluck(this.results, 'count'));
  this.count += (remaining + _.size(this._bindings)) * this.resultsPerBinding(); // _streams are already included in results

  this.matchRate = _.size(_.filter(this.results, function (result) { return result.count > 0; })) / _.size(this.results);

  this.logger.info("UPDATE remaining input:" + remaining + ", ended:" + this.ended + ", remaining:" + this.remaining + ", cost:" + this.cost + ", count:" + this.count + ", costRemaining:" + this.costRemaining + ", matchRate:" + this.matchRate);
};


module.exports = Stream;
Stream.DownloadStream = DownloadStream;
Stream.BindingStream = BindingStream;