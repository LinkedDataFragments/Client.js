/**
 * Created by joachimvh on 28/08/2014.
 */

var rdf = require('../util/RdfUtil'),
    _ = require('lodash'),
    TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
    Iterator = require('../iterators/Iterator'),
    MultiTransformIterator = require('../iterators/MultiTransformIterator'),
    Logger = require ('../util/Logger'),
    ClusteringUtil = require('./ClusteringUtil'),
    RDFStoreInterface = require('./RDFStoreInterface'),
    EventEmitter = require('events').EventEmitter; // TODO: debug inputs

// TODO: create cached iterator that can easily be reset and repeated? (immediately cloning iterator has the same effect, root iterator is needed everytime though (unless you want a giant stack of clones))
// TODO: ^ also problem if stream was ended and you want to replay?

// keep track of 'validsSoFar' in a stream? (or just use separate object for that, will need to be checked everytime though)

function LDFClusteringStream (options) {
  this._options = options;
}

LDFClusteringStream.prototype.getFullDownloadIterator = function (pattern) {
  return new TriplePatternIterator(Iterator.single({}), pattern, this._options);
};

// assume working read fucntion ...
// read in blocks to reduce number of queries?
function mergeStreams (varStreams) {
  // checks if ended, total remaining size, etc.
  // can all influence results
  var matches = _.map(varStreams, function () { return {}; });
  var reads = [];
  _.each(varStreams, function (stream, idx) {
    var read = stream.read();
    reads.push(read);
    matches[idx][read] = true;
  });
  var completeMatches = _.filter(_.uniq(reads), function (read) {
    return _.every(matches, function (match) {
      return match[read];
    });
  });
  this._buffer.concat(completeMatches);
  // keep doing this
}

function bindingStream (subject, predicate, object) {
  // all three can either be stream or string?
  // more than 1 binding: need to combine all results (probably better to switch streams to increase chances of results?)
  // if multiple: combine all results of one iterator with new result of other
  // should this iterator return results or counts? 2 different iterators prolly (e.g. countiterator && resultsiterator)
  // can keep asking counts until it becomes clear the results will be too high (still cached some results -> will be hard to keep track of?)
  // countstream.read returns binding value + count (average several of these results to predict final result?)
  // valstream.read returns triples (switch between requested bindings or finish 1 binding at a time? -> problem with infinite data, just one page at a time?)
}


///////////////////////////// BindingIterator /////////////////////////////
// TODO: this is for a single var binding
function BindingIterator (source, pattern, bindVar, options) {
  if (!this instanceof BindingIterator)
    return new BindingIterator(source, pattern, bindVar, options);
  // if we give bindings as a source, we are forced to iterate through all the results of a single binding before being able to go to the next
  MultiTransformIterator.call(this, Iterator.single({}), options);

  this._pattern = pattern;
  this._client = this._options.fragmentsClient;
  this._bindVar = bindVar;
  this._source = source;
  this._fragments = {};
}
MultiTransformIterator.inherits(BindingIterator);

BindingIterator.prototype._getFragment = function (binding) {
  var self = this;
  var bindObj = {};
  bindObj[this._bindVar] = binding;
  var appliedPattern = rdf.applyBindings(bindObj, this._pattern);
  var fragment = this._client.getFragmentByPattern(appliedPattern);
  this._fragments[binding] = {fragment: fragment, matchRate: 1.0, read: 0, total: -1};
  fragment.getProperty('metadata', function(metadata) {
    self._fragments[binding].total = metadata.totalTriples;
  });
};

// responsibility of whoever is calling this iterator to update this value
// TODO: how to make this work?
BindingIterator.prototype.setMatchRate = function (binding, matchRate) {
  this._fragments[binding] = matchRate;
};

BindingIterator.prototype._createTransformer = function (bindings, options) {
  return null; // we will need a fragment for every binding
};

BindingIterator.prototype._readTransformer = function (fragment, fragmentBindings) {
  // check match rate of every stored iterator and take highest
  // if none of them reach a specific threshold: get a new binding from the source and use that fragment
  // TODO: dynamically update threshold as more results get stored?
  // TODO: should be cheaper if no new page needs to be downloaded
  // TODO: technically, if there is an infinite amount of bindings this is not good enough
  var threshold = Math.min(0.5, 1-_.size(this._fragments)/this._source.sizeBound());
  if (this._source.ended && _.every(this._fragments, function (fragment) { return fragment.ended; } )) {
    this.close();
    return null;
  }

  var best = _.max(_.filter(this._fragments), function (fragment) { return !fragment.ended; }, function (fragment) { return fragment.matchRate;});
  if (!best || best.matchRate < threshold && !this._source.ended) {
    var binding = this._source.read();
    if (!binding)
      return null;
    best = this._getFragment(binding);
  }
  if (best.total < 0)
    return null;
  var triple;
  while (triple = best.fragment.read()) {
    try { return rdf.findBindings(this._pattern, triple); }
    catch (bindingError) { }
  }
  return null;
};

BindingIterator.prototype.sizeEstimate = function () {
  var self = this;
  var valids = _.filter(this._fragments, function (fragment) { return fragment.total >= 0; });
  var sourceSize = this._source.sizeEstimate();
  if (valids.length <= 0)
    return { size: sourceSize.size, certainty: 0 };
  var avg = Math.sum(_.map(valids, function (fragment) { return fragment.total; }))/valids.length;
  return { size: avg*sourceSize.size, certainty: valids.length/sourceSize.size*sourceSize.certainty };
};

///////////////////////////// MergeIterator /////////////////////////////
function MergeIterator (sources, options) {
  if (!this instanceof BindingIterator)
    return new MergeIterator(sources, options);
  // if we give bindings as a source, we are forced to iterate through all the results of a single binding before being able to go to the next
  MultiTransformIterator.call(this, Iterator.single({}), options);

  this._sources = sources;
}
MultiTransformIterator.inherits(MergeIterator);

MergeIterator.prototype._createTransformer = function () {
  return null; // we will need a fragment for every binding
};

MergeIterator.prototype._readTransformer = function () {
  // TODO: ok, so I have a bunch of sources, what do I do with them?
  // for each source, do a single call read (which can give multiple results (pagesize)) to have a start sample
  // if there are matches: use that to set a match ratio
  // randomly? choose next source to do a call read, chance based on match ratio and remaining size? maybe also how much we can get in 1 call read?
};

///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
//////////////////////////// ATTEMPT 2 ////////////////////////////
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////

function getFullStream (pattern, options) {
  return new TriplePatternIterator(Iterator.single({}), pattern, options);
}

function getBindingCountStream (bindingIterator, pattern, options) {
  return null;
}

function getBindingStream (bindingIterator, pattern, options) {
  return new TriplePatternIterator(bindingIterator, pattern, options);
}

function createCall (cost, call, description) {
  return {
    cost: cost,
    call: call,
    description: description
  };
}

LDFClusteringStream.algorithm = function (options, patterns, nodes, clusters, store) {
  if (!nodes) {
    nodes = _.map(patterns, function (pattern) { return { pattern: pattern, countStreams: {}, stream: null}; });
    var delayedCallback = _.after(nodes.length, function () { LDFClusteringStream.algorithm(options, patterns, nodes, clusters, store); });
    _.each(nodes, function (node) {
      var fragment = options.fragmentsClient.getFragmentByPattern(node.pattern);
      fragment.getProperty('metadata', function(metadata) {
        fragment.close();
        node.count = metadata.totalTriples;
        delayedCallback();
      });
    });
    return;
  }
  if (!clusters) {
    clusters = _.object(_.map(_.union.apply(null, _.map(patterns, function (pattern) {
      return ClusteringUtil.getVariables(pattern);
    })), function (v) {
      return [v, { nodes: _.filter(nodes, function (node) {
        return _.contains(ClusteringUtil.getVariables(node.pattern), v);
      }), key: v
      }];
    }));
  }
  if (!store)
    store = new RDFStoreInterface(); // TODO: really should make an object ...

  var logger = new Logger("Stream"); // TODO: yes yes, should create a single object, bite me

  // iterating function that optimizes iterator for every node?

  // check if all nodes have an iterator, if yes: download page on each and add to db
  // even better: just download a page on every node that has an iterator, that way the db slowly gets filled while more iterators get added
  var grouped = _.groupBy(nodes, function (node) { return node.stream ? "complete" : "incomplete"; });
  // TODO: do this
  // _.each(grouped.complete, function (node) { node.stream.getPage("storeTriplesInDBCallback"); });

  // TODO: how to determine we should probably wait for some more data?
  var fullStreamCalls = _.map(grouped.incomplete, function (node) {
    var stream = new FullDownloadStream (node.pattern, node.count, options);
    return createCall(
      stream.estimateRemaining(),
      function (callback) { node.stream = stream; callback(); },
      "Created full download stream for " + rdf.toQuickString(node.pattern)
    );
  });

  // TODO: create cluster streams
  var countStreamCalls = _.flatten(_.map(grouped.incomplete, function (node) {
    return _.filter(_.map(["subject", "predicate", "object"], function (pos) {
      var v = node.pattern[pos];
      if (!rdf.isVariable(v) || !clusters[v].bindings)
        return null;
      var stream = new BindingCountStream (node.pattern, v, clusters[v].bindings, options);
      return createCall(
        stream.estimateRemaining(),
        function (callback) { node.countStreams[pos] = stream; callback(); },
        "Created count stream on variable " + v + " for " + rdf.toQuickString(node.pattern)
      );
    }));
  }));

  var bindStreamCalls = _.flatten(_.map(grouped.incomplete, function (node) {
    return _.filter(_.map(["subject", "predicate", "object"], function (pos) {
      if (!node.countStreams[pos])
        return null;
      var stream = new BindingStream(node.pattern, node.countStreams[pos], options);
      return createCall(
        stream.estimateRemaining(),
        function (callback) {},
        "Created binding stream on variable " + node.pattern[pos] + " for " + rdf.toQuickString(node.pattern)
      );
    }));
  }));

  var best = _.min(_.map([fullStreamCalls, countStreamCalls, bindStreamCalls], function (calls) {
    return _.min(calls, function (call) { return call.cost; } );
  }), function (call) { return call.cost; });

  logger.info(best.description);

  best.call(function () {
    var valids = _.filter(nodes, function (node) { return node.stream; });

    var delayedClusterCallback = _.after(_.size(clusters), function () {
      LDFClusteringStream.algorithm(options, patterns, nodes, clusters, store);
    });

    var delayedCallback = _.after(valids.length, function () {
      _.each(clusters, function (cluster, key) {
        var patterns = _.filter(_.map(cluster.nodes, function (node) { return node.stream ? node.pattern : null; }));
        if (patterns.length > 0) {
          store.matchBindings(patterns, function (results) {
            cluster.bindings = _.map(results, function (result) { return result[cluster.key]; });
            delayedClusterCallback();
          });
        } else {
          delayedClusterCallback();
        }
      });
    });

    _.each(valids, function (node) {
      node.stream.readCall(function (buffer) {
        store.addTriples(buffer, delayedCallback);
      });
    });
  });

//  clusters.updateFromDatabase();
//  nodes.iterator.filterInvalidBindings();
  // GOTO START
};

///////////////////////////// FullDownloadStream /////////////////////////////
function FullDownloadStream (pattern, count, options) {
  if (!this instanceof FullDownloadStream)
    return new FullDownloadStream(pattern, options);

  this._iterator = new TriplePatternIterator(Iterator.single({}), pattern, options);
  this._pattern = pattern;
  this._remaining = count;

  var self = this;
}

FullDownloadStream.prototype.readCall = function (callback) {
  if (this.ended()) {
    setImmediate(function () { callback([]); });
    return;
  }
  var self = this;
  var pageSize = 100; // TODO: real pagesize
  var buffer = [];
  var iterator = this._iterator;
  iterator.on('readable', addTriple);
  iterator.on('end', end);
  function addTriple (binding) {
    buffer.push(rdf.applyBindings(iterator.read(), self._pattern));
    if (buffer.length >= pageSize || iterator.ended) {
      iterator.removeListener('readable', addTriple);
      iterator.removeListener('end', end);
      self._remaining -= buffer.length;
      setImmediate(function (){ callback(buffer); });
    }
  }
  function end () {
    self._remaining = 0;
    setImmediate(function (){ callback(buffer); });
  }
};

FullDownloadStream.prototype.estimateRemaining = function () {
  return this._remaining;
};

FullDownloadStream.prototype.estimateRemainingCalls = function () {
  return Math.ceil(this._remaining/100); // TODO: pagesize
};

FullDownloadStream.prototype.ended = function () {
  return this._iterator.ended;
};

// TODO: count streams zijn mss niet echt logisch bij streams?
// TODO: gewoon direct beginnen met stream en checken wat de eerste paar hits als resultaten geven...
// TODO: toelaten bindings te feeden, gebruikte bindings opslaan
///////////////////////////// SingleBindingCountStream /////////////////////////////
function BindingCountStream (pattern, bindVar, bindings, options) {
  if (!this instanceof BindingCountStream)
    return new BindingCountStream(pattern, bindVar, bindings, options);

  this._client = options.fragmentsClient;
  this._pattern = pattern;
  this._var = bindVar;
  this._bindings = bindings;
}

BindingCountStream.prototype.readCall = function (callback) {
  var self = this;
//  if (this._bindBuffer.length <= 0) {
//    this._stream.readCall(function (buffer) {
//      self._bindBuffer = buffer;
//      setImmediate(function () { callback([]); });
//    });
//  } else {
  var binding = {};
  //binding[this._var] = this._bindBuffer.shift();
  binding[this._var] = this._bindings.shift();
  var boundPattern = rdf.applyBindings(binding, this._pattern);
  var fragment = this._client.getFragmentByPattern(boundPattern);
  fragment.getProperty('metadata', function(metadata) {
    fragment.close();
    var count = metadata.totalTriples;
    var result = {count: count, binding: binding};
    setImmediate(function () { callback(result); });
  });
//  }
};

BindingCountStream.prototype.estimateRemaining = function() {
  //return this._bindBuffer.length + this._stream.estimateRemaining();
  return this._bindings.length;
};

BindingCountStream.prototype.estimateRemainingCalls = function() {
  //return this._bindBuffer.length + this._stream.estimateRemaining() + this._stream.estimateRemainingCalls();
  return this._bindings.length;
};

BindingCountStream.prototype.ended = function () {
  return this._bindings <= 0;
};

///////////////////////////// SingleBindingStream /////////////////////////////
function BindingStream (pattern, bindings, options) {
  if (!this instanceof BindingStream)
    return new BindingStream(pattern, bindings, options);

  this._options = options;
  this._pattern = pattern;
  this._bindings = bindings;

  this._storedCounts = []; // used for calculating avg
  this._boundStream = { ended: function() { return true; }, estimateRemaining: function() { return 0; }}; // allows me to avoid some null checks
}

// TODO: switch between bindings instead of staying on the same one the entire time?
BindingStream.prototype.readCall = function (callback) {
  if (this.ended()) {
    setImmediate(function () { callback([]); });
    return;
  }
  var self = this;
//  if (this._boundStream.ended() && this._bindBuffer.length <= 0) {
//    this.fillBuffer(1, function() { callback([]); });
//  } else {
  if (this._boundStream.ended()) {
    var binding = this._bindings.shift();
    var boundPattern = rdf.applyBindings(binding, this._pattern);
    var fragment = this._options.fragmentsClient.getFragmentByPattern(boundPattern);
    fragment.getProperty('metadata', function(metadata) {
      fragment.close();
      self._boundStream = new FullDownloadStream(boundPattern, metadata.totalTriples, self._options);
      self._storedCounts.push({binding: binding, count: metadata.totalTriples});
      callback([]);
    });
  } else {
    this._boundStream.readCall(callback);
  }
};

// TODO: maybe separate field for new values?
BindingStream.prototype.feed = function (bindings) {
  var self = this;
  var storedBindings = _.pluck(this._storedCounts, 'binding');
  // only add bindings that we didn't calculate yet
  this._bindings = _.filter(bindings, function (binding) {
    return !ClusteringUtil.containsObject(storedBindings, binding);
  });
  // remove unneeded bindings
  this._storedCounts = _.filter(this._storedCounts, function (stored) {
    return ClusteringUtil.containsObject(bindings, stored.binding);
  });
};

//BindingStream.prototype.fillBuffer = function (bufferSize, callback) {
//  var additional = bufferSize - this._bindBuffer.length;
//  if (additional <= 0 || this._stream.ended()) {
//    callback();
//    return;
//  }
//  var self = this;
//  this._stream.readCall(function (buffer) {
//    self._bindBuffer = buffer;
//    self._storedCounts = self._storedCounts.concat(buffer);
//    setImmediate(function () { self.fillBuffer(bufferSize, callback); });
//  });
//};

// TODO: underestimate remaining at the start so they don't get replaced quickly
BindingStream.prototype.estimateRemaining = function() {
  // wait until we at least have 10 values before trying to check the average
  // TODO: maybe better way to choose value instead of 10
  if (this._storedCounts.length <= 10)
    return this._bindings.length*100;
  var avg = _.reduce(this._storedCounts, function (memo, val) { return memo + val.count; }, 0)/this._storedCounts.length;
  return avg * this._bindings.length + this._boundStream.estimateRemaining();
};

BindingStream.prototype.estimateRemainingCalls = function() {
  return Math.ceil(this.estimateRemaining()/100);
};

BindingStream.prototype.ended = function () {
  return this._bindings.length === 0 && this._boundStream.ended();
};

///////////////////////////// PageStream Interface /////////////////////////////

//function PageStream () {
//  this._buffer = [];
//}
//
//PageStream.prototype.ended = function () { return true; };
//PageStream.prototype.estimateRemaining = function () { return 0; };
//PageStream.prototype.estimateRemainingCalls = function () { return 0; };
//
//PageStream.prototype.readCall = function (callback) {
//  var self = this;
//  this._singleCall(function (buffer) {
//    if (self._buffer.length > 0) {
//      buffer = self._buffer.concat(buffer);
//      self._buffer = [];
//    }
//    setImmediate(function () { callback(buffer); });
//  });
//};
//
//PageStream.prototype.readCalls = function (calls, callback) {
//  if (calls <= 0)
//    return;
//  var self = this;
//
//  // TODO: read up on node.js synchronization to see how much of this is necessary
//  var delayedcallback = _.after(calls, function () { callback(self._buffer); });
//  var called = false;
//  var customCallback = function (buffer) {
//    self._buffer = self._buffer.concat(buffer);
//    if (!called) {
//      if (self.ended()) {
//        called = true;
//        callback(self._buffer);
//      } else {
//        delayedcallback();
//      }
//    }
//  };
//
//  while (calls-- > 0 && !this.ended())
//    this.readCall(customCallback);
//};
//
//PageStream.prototype._singleCall = function (callback) {
//  throw new Error('The _singleCall method has not been implemented.');
//};

///////////////////////////// ClusterStream /////////////////////////////
//function ClusterStream (streams) {
//  this._streams = streams;
//  this._readPos = 0;
//}
//
//ClusterStream.prototype.ended = function () {
//  return _.every(this._streams, function (stream) { return stream.ended(); });
//};
//
//PageStream.prototype.estimateRemaining = function () {
//  return _.reduce(this._streams, function (memo, stream) { return memo + stream.estimateRemaining(); }, 0);
//};
//
//PageStream.prototype.estimateRemainingCalls = function () {
//  return _.reduce(this._streams, function (memo, stream) { return memo + stream.estimateRemainingCalls(); }, 0);
//};
//
//PageStream.prototype.readCall = function (callback) {
//  if (this.ended())
//    callback([]);
//  if (this._readPos >= this._streams.length)
//    this._readPos = 0;
//  while (this._streams[this._readPos].ended())
//    this._readPos = (this._readPos + 1) % this._streams.length;
//  this._streams[this._readPos].readCall(callback);
//  this._readPos = (this._readPos + 1) % this._streams.length;
//};

LDFClusteringStream.init2 = function (patterns, options) {
  var nodes = _.map(patterns, function (pattern) { return { pattern: pattern, stream: null, triples: []}; });

  var clusters = _.object(_.map(_.union.apply(null, _.map(patterns, function (pattern) {
    return ClusteringUtil.getVariables(pattern);
  })), function (v) {
    return [v, { nodes: _.filter(nodes, function (node) {
      return _.contains(ClusteringUtil.getVariables(node.pattern), v);
    }), key: v
    }];
  }));
  var store = new RDFStoreInterface();

  var logger = new Logger("Stream");

  var delayedCall = _.after(nodes.length, function () { LDFClusteringStream.calls(nodes, clusters, store, logger, options); } );
  _.each(nodes, function (node) {
    var fragment = options.fragmentsClient.getFragmentByPattern(node.pattern);
    fragment.getProperty('metadata', function(metadata) {
      fragment.close();
      node.count = metadata.totalTriples;
      node.stream = new FullDownloadStream (node.pattern, node.count, options);
      delayedCall();
    });
  });
};

LDFClusteringStream.calls = function (nodes, clusters, store, logger, options) {
  var incompletes = _.filter(nodes, function (node) { return !node.stream.ended(); });
  var delayedCall = _.after(incompletes.length, function () { LDFClusteringStream.updateStreams(nodes, clusters, store, logger, options); });
  _.each(incompletes, function (node) {
    node.stream.readCall(function (buffer) {
      store.addTriples(buffer, function () {
        // callback
      });
      node.triples = node.triples.concat(buffer);
      delayedCall();
    });
  });
};

LDFClusteringStream.updateStreams = function (nodes, clusters, store, logger, options) {
  var bindNodes = [];


  // TODO: if one node is complete those values can be used as bounds
  _.each(clusters, function (cluster) {
    // TODO: should only be called once per ended stream
    var ended = _.filter(cluster.nodes, function (node) { return node.stream.ended(); });
    var bounds = null;
    if (ended.length > 0) {
      bounds = _.intersection.apply(null, _.map(ended, function (node) {
        var pos = _.filter(["subject", "predicate", "object"], function (pos) { return node.pattern[pos] === cluster.key; })[0];
        return _.pluck(node.triples, pos);
      }));
      bounds = _.map(bounds, function (binding) { return _.object([[cluster.key, binding]]); }); // need array of arrays

      // filter triples that don't match
      _.each(cluster.nodes, function (node) {
        node.triples = _.filter(node.triples, function (triple) {
          return _.some(bounds, function (binding) {
            try { rdf.extendBindings(binding, node.pattern, triple); return true; }
            catch (bindingError) { return false; }
          });
        });
      });
    }

    // TODO: should only check new values
    var bindings = _.uniq(_.union.apply(null, _.map(cluster.nodes, function (node) {
      var pos = _.filter(["subject", "predicate", "object"], function (pos) { return node.pattern[pos] === cluster.key; })[0];
      return _.pluck(node.triples, pos);
    })));
    bindings = _.map(bindings, function (binding) { return _.object([[cluster.key, binding]]); }); // need array of arrays

    if (bounds)
      bindings = _.union(bounds, _.intersection(bindings, bounds));
    cluster.bindings = bindings;
    _.each (cluster.nodes, function (node) {
      if (node.stream.feed && node.bindVar === cluster.key)
        node.stream.feed(bindings);
    });

    // TODO: check all clusters if a node is in multiple clusters
    _.each (cluster.nodes, function (node) {
      if (node.stream.estimateRemainingCalls() > bindings.length) {
        bindNodes.push(node);
      }
    });
  });

  _.each(nodes, function (node) {
    var minVar = _.min(ClusteringUtil.getVariables(node.pattern), function (v) { return clusters[v].bindings.length; });
    if (clusters[minVar].bindings.length < node.stream.estimateRemainingCalls()) {
      logger.info("Updating " + rdf.toQuickString(node.pattern) + " to bind stream on var " + minVar);
      node.stream = new BindingStream(node.pattern, clusters[minVar].bindings, options);
      node.bindVar = minVar;
    }
  });

  // TODO: check if the bindingstreams shouldn't be reverted (probably a good idea to also store old used streams then)

  LDFClusteringStream.calls(nodes, clusters, store, logger, options);
};



module.exports = LDFClusteringStream;