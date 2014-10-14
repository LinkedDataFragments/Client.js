/**
 * Created by joachimvh on 3/10/2014.
 */

var rdf = require('../util/RdfUtil'),
    _ = require('lodash'),
    N3 = require('N3'),
    Logger = require ('../util/Logger'),
    ClusteringUtil = require('./ClusteringUtil'),
    Iterator = require('../iterators/Iterator'),
    ReorderingGraphPatternIterator = require('../triple-pattern-fragments/ReorderingGraphPatternIterator'),
    TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator');

function N3StoreInterface (nodes) {
  this.store = N3.Store();
  this.nodes = nodes;
  this.nodeMap = _.object(_.map(nodes, function (node) { return rdf.toQuickString(node.pattern); }), nodes);
  this.cache = [];
  this.lastCounts = {};
  this.DEBUGtime = 0;
  this.logger = new Logger("N3StoreInterface");
  //this.logger.disable();
}

N3StoreInterface.prototype.addTriples = function (triples, callback) {
  this.store.addTriples(triples);
  callback();
};

N3StoreInterface.prototype.cacheResult = function (nodes) {
  var idx = _.findIndex(this.cache, function (cacheEntry) {
    if (nodes.length !== cacheEntry.nodes.length)
      return false;
    return _.intersection(cacheEntry.nodes, nodes).length === nodes.length;
  });
  return idx < 0 ? null : this.cache[idx];
};

N3StoreInterface.prototype.maxMatchingResult = function (nodes) {
  var bestMatch = {nodes:[], bindings:[Object.create(null)]};
  // TODO: possible optimization by knowing how many more we could find
  for (var i = 0; i < this.cache.length; ++i) {
    var entry = this.cache[i];
    if (nodes.length < entry.nodes.length)
      continue;
    var intersection = _.intersection(entry.nodes, nodes);
    if (intersection.length < entry.nodes.length)
      continue;
    if (entry.nodes.length > bestMatch.nodes.length && this.cacheMatchesNodes(entry, intersection))
      bestMatch = entry;
  }
  return bestMatch;
};

N3StoreInterface.prototype.cacheMatchesNodes = function (entry, nodes) {
  return _.every(nodes, function (node) {
    var str = rdf.toQuickString(node.pattern);
    if (!_.has(entry.counts, str)) return true;
    return entry.counts[rdf.toQuickString(node.pattern)] === node.activeStream.tripleCount;
  });
};

N3StoreInterface.prototype.updateCacheResult = function (entry, nodes, bindings) {
  nodes = _.sortBy(nodes, function (node) { return rdf.toQuickString(node.pattern); });
  entry.counts = _.object(entry.strings, _.map(nodes, function (node) { return node.activeStream.tripleCount; }));
  entry.bindings = bindings;
};

N3StoreInterface.prototype.pushCacheResult = function (nodes, bindings) {
  nodes = _.sortBy(nodes, function (node) { return rdf.toQuickString(node.pattern); });
  var strings = _.map(nodes, function (node) { return rdf.toQuickString(node.pattern); });
  var counts = _.object(strings, _.map(nodes, function (node) { return node.activeStream.tripleCount; }));
  var entry = {strings:strings, counts:counts, nodes:nodes, bindings:bindings};
  this.cache.push(entry);
  return entry;
};

// TODO: giving patterns to keep interface, should accept nodes if used eventually
N3StoreInterface.prototype.matchBindings = function (patterns, callback) {
  var DEBUGdate = new Date();
  var self = this;
//  var it = new ReorderingGraphPatternIterator(Iterator.single({}), patterns, {fragmentsClient: new N3FragmentsClientWrapper(this.store)});

//  var parameters = _.map(['subject', 'predicate', 'object'], function (pos) { return rdf.isVariable(patterns[0][pos]) ? null : patterns[0][pos]; });
//  var data = this.store.find.apply(this.store, parameters);
//  var it = Iterator.ArrayIterator(data);

//  var count = 0;
//  it.toArray(function (error, data) {
//    self.DEBUGtime += new Date() - DEBUGdate;
//    callback(data);
//  });

  var nodes = _.map(patterns, function (pattern) { return self.nodeMap[rdf.toQuickString(pattern)]; });
  var bestCache = this.maxMatchingResult(nodes);
  nodes = _.difference(nodes, bestCache.nodes);
  if (nodes.length <= 0)
    return callback(bestCache.bindings);
  // TODO: not sure about best sort, usually want same order to preserve cache?
  nodes = _.sortBy(nodes, function (node) { return node.activeStream.cost; });
  _.each(nodes, function (node) { if (!_.has(self.lastCounts, rdf.toQuickString(node.pattern))) self.lastCounts[rdf.toQuickString(node.pattern)] = 0; });
  var grouped = _.groupBy(nodes, function (node) {
    return node.activeStream.tripleCount === self.lastCounts[rdf.toQuickString(node.pattern)] ? 'same' : 'changed';
  });
  _.each(nodes, function (node) { self.lastCounts[rdf.toQuickString(node.pattern)] = node.activeStream.tripleCount; });
  grouped.same = grouped.same || [];
  grouped.changed = grouped.changed || [];
  nodes = grouped.same.concat(grouped.changed);
  var orderedNodes, vars;
  if (bestCache.nodes.length === 0) {
    orderedNodes = [nodes.shift()];
    vars = ClusteringUtil.getVariables(orderedNodes[0].pattern);
  } else {
    orderedNodes = [];
    vars = _.union.apply(null, _.map(bestCache.nodes, function (node) { return ClusteringUtil.getVariables(node); }));
  }
  while (!_.isEmpty(nodes)) {
    // TODO: unconnected nodes problem
    var nodeIdx = _.findIndex(nodes, function (node) { return !_.isEmpty(_.intersection(vars, ClusteringUtil.getVariables(node.pattern))); });
    var nextNode = nodes.splice(nodeIdx, 1)[0];
    orderedNodes.push(nextNode);
    vars = _.union(vars, ClusteringUtil.getVariables(nextNode.pattern));
  }

  _.each(_.keys(DEBUGTIMER), function (key) { DEBUGTIMER[key] = 0; });

  var start;
  var bindings = bestCache.bindings;
  var newBindings = [];
  orderedNodes = bestCache.nodes.concat(orderedNodes);
  var prevEntry = bestCache;
  for (var i = bestCache.nodes.length; i < orderedNodes.length; ++i) {
    if (_.isEmpty(bindings))
      break;
    var node = orderedNodes[i];
    var cacheEntry = this.cacheResult(orderedNodes.slice(0, i+1));
    if (cacheEntry && this.cacheMatchesNodes(cacheEntry, orderedNodes.slice(0, i+1))) {
      // TODO: this should never trigger anymore
      bindings = cacheEntry.bindings;
      newBindings = [];
      //self.logger.info('CACHED ' + i + ' ' + cacheEntry.strings);
      ++DEBUGTIMER.cached;
    } else {
      // TODO: what if not all of the previous counts in the cacheEntry are correct?
      if (cacheEntry && this.cacheMatchesNodes(cacheEntry, orderedNodes.slice(0, i))) {
        start = new Date();
        var newTriples = node.triples.slice(cacheEntry.counts[rdf.toQuickString(node.pattern)]);
        DEBUGTIMER.slice += new Date() - start;
        start = new Date();
        var newTripleBindings = this.extendBindings2(bindings, node.pattern, newTriples);
        DEBUGTIMER.newTriples += new Date() - start;
        start = new Date();
        var updatedNewBindings = _.isEmpty(newBindings) ? [] : this.extendBindings2(newBindings, node.pattern, node.triples); //this.patternFromStore(node.pattern)
        DEBUGTIMER.updatedTriples += new Date() - start;
        newBindings = newTripleBindings.concat(updatedNewBindings);
        bindings = newBindings.concat(cacheEntry.bindings);
      } else {
        start = new Date();
        bindings = this.extendBindings2(bindings, node.pattern, node.triples);
        newBindings = bindings;
        DEBUGTIMER.basic += new Date() - start;
      }

      start = new Date();
      // TODO: way to make sure this wouldn't be necessary
//      var uniq = _.uniq(bindings, function (binding) { return _.map(_.sortBy(_.keys(binding)), function (key) { return key+''+binding[key]; }).join(''); } );
//      bindings = uniq;
      //node.newTriples = [];
      var DEBUGARR = _.map(orderedNodes.slice(0, i+1), function (node) { return node.activeStream.tripleCount; });
      if (cacheEntry)
        this.updateCacheResult(cacheEntry, orderedNodes.slice(0, i+1), bindings);
      else
        cacheEntry = this.pushCacheResult(orderedNodes.slice(0, i+1), bindings);
      DEBUGTIMER.rest += new Date() - start;
    }
    prevEntry = cacheEntry;
  }
  this.DEBUGtime += new Date() - DEBUGdate;
  DEBUGTIMER.total = new Date() - DEBUGdate;
  DEBUGTIMER.apply = Math.ceil(DEBUGTIMER.apply/1000000);
  DEBUGTIMER.match = Math.ceil(DEBUGTIMER.match/1000000);
  DEBUGTIMER.bind = Math.ceil(DEBUGTIMER.bind/1000000);
  DEBUGTIMER.matchFilter = Math.ceil(DEBUGTIMER.matchFilter/1000000);
  DEBUGTIMER.matchStore = Math.ceil(DEBUGTIMER.matchStore/1000000);
  DEBUGTIMER.bestCache = bestCache.nodes.length;
  DEBUGTIMER.bindings = bindings.length;
  if (DEBUGTIMER.total > 10) {
    this.logger.info('query: ' + _.map(patterns, rdf.toQuickString).join(" "));
    this.logger.info(_.map(_.keys(DEBUGTIMER), function (key) { return key + ': ' + DEBUGTIMER[key]; }).join(', '));
  }
  callback(bindings);
};
var DEBUGTIMER = {slice:0, newTriples:0, updatedTriples:0, basic:0, rest:0, extend:0, extend2:0, apply:0, match:0, bind:0, matchFilter:0, matchStore:0, cached:0, bestCache:0};

N3StoreInterface.prototype.extendBindings = function (bindings, pattern, triples) {
  var self = this;
  var totalStart = new Date();
  var result = _.flatten(_.map(bindings, function (binding) {
    var start = process.hrtime();
    var appliedBinding = rdf.applyBindings(binding, pattern);
    DEBUGTIMER.apply += process.hrtime(start)[1];
    start = process.hrtime();
    var matches = triples ? self.filterPatternTriples(triples, appliedBinding) : self.patternFromStore(appliedBinding);
    DEBUGTIMER.match += process.hrtime(start)[1];
    start = process.hrtime();
    var result = self.triplesToBindings(_.clone(binding), appliedBinding, matches);
    DEBUGTIMER.bind += process.hrtime(start)[1];
    return result;
  }));
  DEBUGTIMER.extend += new Date() - totalStart;

  return result;
};

N3StoreInterface.prototype.filterPatternTriples = function (triples, pattern) {
  var start = process.hrtime();
  var result = _.filter(triples, function (triple) {
    return (rdf.isVariable(pattern.subject) || pattern.subject === triple.subject) &&
      (rdf.isVariable(pattern.object) || pattern.object === triple.object) &&
      (rdf.isVariable(pattern.predicate) || pattern.predicate === triple.predicate);
  });
  DEBUGTIMER.matchFilter += process.hrtime(start)[1];
  return result;
};

N3StoreInterface.prototype.patternFromStore = function (pattern) {
  var start = process.hrtime();
  var matches = this.store.find(
    rdf.isVariable(pattern.subject) ? null : pattern.subject,
    rdf.isVariable(pattern.predicate) ? null : pattern.predicate,
    rdf.isVariable(pattern.object) ? null : pattern.object
  );
  DEBUGTIMER.matchStore += process.hrtime(start)[1];
  return matches;
};

N3StoreInterface.prototype.triplesToBindings = function (binding, pattern, verifiedTriples) {
  var newBindings = _.map(verifiedTriples, function (triple) {
    var newBinding = _.clone(binding);
    if (rdf.isVariable(pattern.subject)) newBinding[pattern.subject] = triple.subject;
    if (rdf.isVariable(pattern.predicate)) newBinding[pattern.predicate] = triple.predicate;
    if (rdf.isVariable(pattern.object)) newBinding[pattern.object] = triple.object;
    return newBinding;
  });
  return newBindings;
};

N3StoreInterface.prototype.extendBindings2 = function (leftBindings, pattern, triples) {
  var start = new Date();
  triples = triples || this.patternFromStore(pattern);
  var rightBindings = this.triplesToBindings2(pattern, triples);
  DEBUGTIMER.match += new Date() - start;
  DEBUGTIMER.extend2 += new Date() - start;

  // TODO: workaround
  if (leftBindings.length === 1 && _.size(leftBindings[0]) === 0)
    return rightBindings;

  if (_.isEmpty(leftBindings) || _.isEmpty(rightBindings))
    return [];

  start = new Date();

  var keys = _.intersection(_.keys(leftBindings[0]), ClusteringUtil.getVariables(pattern));

  var minBindings = leftBindings.length < rightBindings.length ? leftBindings : rightBindings;
  var maxBindings = leftBindings.length >= rightBindings.length ? leftBindings : rightBindings;

  // merging
  var tree = {}, branch, binding, val, i, j, valid;
  for(i = 0; i < minBindings.length; ++i) {
    binding = minBindings[i];
    branch = tree;
    for(j = 0; j < keys.length-1; j++) {
      val = binding[keys[j]];
      branch[val] = branch[val] || Object.create(null);
      branch = branch[val];
    }
    val = binding[keys[keys.length-1]];
    branch[val] = branch[val] || [];
    branch[val].push(binding);
  }

  if (keys.length === 0)
    tree = tree[undefined];

  var joined = [];
  for(i=0; i < maxBindings.length; i++) {
    binding = maxBindings[i];
    branch = tree;
    valid = true;
    for(j = 0; j < keys.length; j++) {
      val = binding[keys[j]];
      if (branch[val]) {
        branch = branch[val];
      } else {
        valid = false;
        break;
      }
    }
    if (valid) {
      // branch will be the leaf at this point
      for (j = 0; j < branch.length; j++) {
        joined.push(this.mergeBindings2(branch[j], binding));
      }
    }
  }

  DEBUGTIMER.extend2 += new Date() - start;

  return joined;
};

N3StoreInterface.prototype.mergeBindings2 = function (left, right) {
  var merged = {}, variable;
  for(variable in left)
    merged[variable] = left[variable];

  for(variable in right)
    merged[variable] = right[variable];

  return merged;
};

N3StoreInterface.prototype.triplesToBindings2 = function (pattern, triples) {
  var varPos = {};
  for (var pos in pattern) {
    if (rdf.isVariable(pattern[pos]))
      varPos[pos] = pattern[pos];
  }

  var results = [];
  for (var i = 0; i < triples.length; ++i) {
    var result = {};
    var triple = triples[i];
    // TODO: invalid if triple has duplicate vars
    for (pos in varPos) {
      result[varPos[pos]] = triple[pos];
    }
    results.push(result);
  }
  return results;
};

function N3FragmentsClientWrapper (store) {
  this.store = store;
}

N3FragmentsClientWrapper.prototype.getFragmentByPattern = function (pattern) {
  var parameters = _.map(['subject', 'predicate', 'object'], function (pos) { return rdf.isVariable(pattern[pos]) ? null : pattern[pos]; });
  var data = this.store.find.apply(this.store, parameters);
  var iterator = Iterator.ArrayIterator(data);
  iterator.setProperty('metadata', {totalTriples: _.size(data)});
  return iterator;
};

module.exports = N3StoreInterface;