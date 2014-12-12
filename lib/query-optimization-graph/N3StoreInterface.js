/**
 * Created by joachimvh on 3/10/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  N3 = require('n3'),
  Logger = require ('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil'),
  Iterator = require('../iterators/Iterator'),
  ReorderingGraphPatternIterator = require('../triple-pattern-fragments/ReorderingGraphPatternIterator'),
  TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator');

function N3StoreInterface () {
  this.store = N3.Store();
  this.cache = {};
  this.lastCounts = {};
  this.DEBUGtime = 0;
  this.logger = new Logger("N3StoreInterface");
  this.DEBUGTIMERTOTAL = _.clone(DEBUGTIMER);
  //this.logger.disable();
}

N3StoreInterface.prototype.addTriples = function (triples, callback) {
  this.store.addTriples(triples);
  callback();
};

N3StoreInterface.prototype.reset = function (node) {
  this.cache = _.omit(this.cache, function (entry) { return _.contains(entry.nodes, node); });
  this.lastCounts[ClusteringUtil.tripleID(node.pattern)] = 0;
};

N3StoreInterface.prototype.cacheID = function (nodes) {
  return _.map(_.pluck(nodes, 'pattern'), ClusteringUtil.tripleID).sort().join('');
};

N3StoreInterface.prototype.cacheResult = function (nodes) {
  return this.cache[this.cacheID(nodes)];
};

N3StoreInterface.prototype.cacheCost = function (node, prevEntry) {
  var nextEntry = this.cacheResult(prevEntry.nodes.concat([node]));
  if (!nextEntry || !nextEntry.counts) {
    if (prevEntry && prevEntry.bindings)
      return node.triples.length*prevEntry.bindings.length;
    else
      return node.triples.length * node.triples.length;
  }
  var oldCount = nextEntry.counts[ClusteringUtil.tripleID(node.pattern)];
  var newCount = node.triples.length - oldCount;
  var diffs = _.map(prevEntry.nodes, function (prevNode) {
    var count = nextEntry.counts[ClusteringUtil.tripleID(prevNode.pattern)];
    return prevNode.triples.length-count;
  });
  // this looks more complicated than the other version, but gives worse results...
  //return newCount*(1+ (prevEntry.bindings ? prevEntry.bindings.length : oldCount)) + (diffs.length > 0 ? Math.max.apply(null, diffs) : 0)*oldCount;
  return newCount + (diffs.length > 0 ? Math.max.apply(null, diffs) : 0);
};

N3StoreInterface.prototype.cachePath = function (nodes, entry, current, best) {
  current = current || {path:[], cost:0};
  best = best || {path:[], cost:Infinity};
  entry = entry || {nodes:[]};

  if (nodes.length <= 0) {
    if (current.cost < best.cost) {
      best.cost = current.cost;
      best.path = current.path;
    }
    return best;
  }

  var vars = _.union.apply(null, _.map(entry.nodes, function (node) { return ClusteringUtil.getVariables(node.pattern); }));
  for (var i = 0; i < nodes.length; ++i) {
    var node = nodes[i];
    var nodeVars = ClusteringUtil.getVariables(node.pattern);
    if (vars.length > 0 && _.intersection(vars, nodeVars).length <= 0)
      continue;

    var cost = this.cacheCost(node, entry);
    var nextCost = current.cost*current.cost + cost; // add more importance to early costs since these new bindings will be used more
    var minCost = Math.pow(nextCost, nodes.length);
    if (minCost < best.cost) {
      var nextEntry = this.cacheResult(entry.nodes.concat(node));
      if (!nextEntry)
        nextEntry = {nodes: entry.nodes.concat([node])};
      this.cachePath(_.without(nodes, node), nextEntry, {path: current.path.concat([node]), cost: nextCost}, best);
    }
  }
  return best;
};

// greedy path prediction, worse results than total prediction
N3StoreInterface.prototype.cacheStep = function (entry, nodes) {
  if (!entry || !entry.nodes || entry.nodes.length === 0)
    return _.min(nodes, function (node) { return node.triples.length; });
  var vars = Object.keys(entry.uniques);
  var filtered = _.filter(nodes, function (node) { return _.intersection(ClusteringUtil.getVariables(node.pattern), vars).length > 0; });
  if (filtered.length <= 0)
    return this.bestStep(null, nodes); // just take the smallest node if we can't make a path (but this is really bad and shouldn't happen)
  var best = {node:null, cost:Infinity};
  for (var i = 0; i < filtered.length; ++i) {
    var node = filtered[i];
    var nextEntry = this.cacheResult(entry.nodes.concat(node));
    var cost;
    if (!nextEntry) {
      cost = entry.bindings.length * node.triples.length;
    } else {
      var oldCount = nextEntry.counts[ClusteringUtil.tripleID(node.pattern)];
      var newCount = node.triples.length - oldCount;
      cost = this.unusedBindings(entry, nextEntry).length * oldCount;
      cost += entry.bindings.length * newCount;
    }
    if (cost < best.cost) {
      best.cost = cost;
      best.node = node;
    }
  }
  return best.node;
};

N3StoreInterface.prototype.unusedBindings = function (oldEntry, newEntry) {
  if (!oldEntry || !oldEntry.bindings)
    return [];
  if (!newEntry || !newEntry.counts)
    return oldEntry.bindings;
  var incompleteStrings = _.filter(oldEntry.strings, function (str) { return newEntry.counts[str] < oldEntry.counts[str]; });
  var filtered = [];
  if (incompleteStrings.length > 0) {
    for (var i = oldEntry.bindings.length-1; i >= 0; --i) {
      var match = false;
      var binding = oldEntry.bindings[i];
      for (var j = 0; j < incompleteStrings.length; ++j) {
        var str = incompleteStrings[j];
        if (binding.indices[str] >= newEntry.counts[str]) {
          filtered.push(binding);
          match = true;
          break;
        }
      }
      // new bindings always get added to the end (since we re-use the rest from the cache)
      if (!match)
        break;
    }
  }
  return filtered;
};

N3StoreInterface.prototype.maxMatchingResult = function (nodes) {
  // we use null to indicate that there should be no join and we should just use all the values of the other list of bindings
  var bestMatch = {nodes:[], bindings:null, counts:{}};
  // TODO: possible optimization by knowing how many more we could find
  for (var str in this.cache) {
    var entry = this.cache[str];
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
    var str = ClusteringUtil.tripleID(node.pattern);
    if (!_.has(entry.counts, str)) return true;
    return entry.counts[str] === node.activeStream.tripleCount;
  });
};

N3StoreInterface.prototype.updateCacheResult = function (entry, nodes, bindings) {
  nodes = _.sortBy(nodes, function (node) { return ClusteringUtil.tripleID(node.pattern); });
  entry.counts = _.object(entry.strings, _.map(nodes, function (node) { return node.activeStream.tripleCount; }));
  entry.bindings = bindings;
};

N3StoreInterface.prototype.pushCacheResult = function (nodes, bindings) {
  nodes = _.sortBy(nodes, function (node) { return ClusteringUtil.tripleID(node.pattern); });
  var strings = _.map(nodes, function (node) { return ClusteringUtil.tripleID(node.pattern); });
  var counts = _.object(strings, _.map(nodes, function (node) { return node.activeStream.tripleCount; }));
  var entry = {strings:strings, counts:counts, nodes:nodes, bindings:bindings, uniques:{}};
   _.each(nodes, function (node) { _.each(ClusteringUtil.getVariables(node.pattern), function (v) { entry.uniques[v] = []; }); } );
  this.cache[this.cacheID(nodes)] = entry;
  return entry;
};

// TODO: giving patterns to keep interface, should accept nodes if used eventually
N3StoreInterface.prototype.matchBindings = function (nodes, callback, v) {
  var DEBUGdate = new Date();
  var self = this;
  _.each(Object.keys(DEBUGTIMER), function (key) { DEBUGTIMER[key] = 0; });

  var start = new Date();
  var bestCache = this.maxMatchingResult(nodes);
  var bindings = bestCache.bindings;
  var uniqs = [];
  nodes = _.difference(nodes, bestCache.nodes);
  var bestPath = this.cachePath(nodes, bestCache);
  var orderedNodes = [];
  DEBUGTIMER.pre += new Date() - start;
  var prevEntry = bestCache;
  if (nodes.length > 0) {
    var blockStart = new Date();
    //start = new Date();
    ////orderedNodes = bestPath.path;
    //// TODO: not sure about best sort, usually want same order to preserve cache?
    //nodes = _.sortBy(nodes, function (node) { return node.activeStream.cost; });
    //_.each(nodes, function (node) { if (!_.has(self.lastCounts, ClusteringUtil.tripleID(node.pattern))) self.lastCounts[ClusteringUtil.tripleID(node.pattern)] = 0; });
    //var grouped = _.groupBy(nodes, function (node) {
    //  return node.activeStream.tripleCount === self.lastCounts[ClusteringUtil.tripleID(node.pattern)] ? 'same' : 'changed';
    //});
    //_.each(nodes, function (node) { self.lastCounts[ClusteringUtil.tripleID(node.pattern)] = node.activeStream.tripleCount; });
    //grouped.same = grouped.same || [];
    //grouped.changed = grouped.changed || [];
    //nodes = grouped.same.concat(grouped.changed);
    //var vars;
    //if (bestCache.nodes.length === 0) {
    //  orderedNodes = [nodes.shift()];
    //  vars = ClusteringUtil.getVariables(orderedNodes[0].pattern);
    //} else {
    //  orderedNodes = [];
    //  vars = _.union.apply(null, _.map(bestCache.nodes, function (node) { return ClusteringUtil.getVariables(node.pattern); }));
    //}
    //while (!_.isEmpty(nodes)) {
    //  // TODO: unconnected nodes problem
    //  var nodeIdx = _.findIndex(nodes, function (node) { return !_.isEmpty(_.intersection(vars, ClusteringUtil.getVariables(node.pattern))); });
    //  var nextNode = nodes.splice(nodeIdx, 1)[0];
    //  orderedNodes.push(nextNode);
    //  vars = _.union(vars, ClusteringUtil.getVariables(nextNode.pattern));
    //}
    //DEBUGTIMER.pre += new Date() - start;

    // TODO: check which is better
    orderedNodes = bestPath.path;

    // TODO: actually, we know which patterns supplied which nodes, these bindings don't need to be rechecked
    orderedNodes = bestCache.nodes.concat(orderedNodes);
    for (var i = bestCache.nodes.length; i < orderedNodes.length; ++i) {
      if (bindings && _.isEmpty(bindings))
        break;
      var node = orderedNodes[i];
      //var node = this.cacheStep(prevEntry, nodes);
      nodes = _.without(nodes, node);
      start = new Date();
      var cacheNodes = prevEntry.nodes.concat([node]);
      var cacheEntry = this.cacheResult(cacheNodes);
      DEBUGTIMER.find += new Date() - start;

      //var last = i === orderedNodes.length - 1;
      start = new Date();
      var uniques = null, vv;
      if (cacheEntry) {
        var filtered = self.unusedBindings(prevEntry, cacheEntry);
        DEBUGTIMER.filter += new Date() - start;
        start = new Date();

        var oldTriples = node.triples.slice(0, cacheEntry.counts[ClusteringUtil.tripleID(node.pattern)]);
        var oldBindings = this.extendBindings(filtered, node.pattern, oldTriples, 0);
        DEBUGTIMER.updatedTriples += new Date() - start;

        start = new Date();
        var newTriples = node.triples.slice(cacheEntry.counts[ClusteringUtil.tripleID(node.pattern)]);
        var newBindings = this.extendBindings(bindings, node.pattern, newTriples, cacheEntry.counts[ClusteringUtil.tripleID(node.pattern)]);
        DEBUGTIMER.newTriples += new Date() - start;

        start = new Date();
        for (vv in cacheEntry.uniques)
          cacheEntry.uniques[vv] = self.union(cacheEntry.uniques[vv], oldBindings[1][vv] || [], newBindings[1][vv] || []);
        DEBUGTIMER.uniq += new Date() - start;

        bindings = cacheEntry.bindings.concat(oldBindings[0].concat(newBindings[0]));
      } else {
        start = new Date();
        bindings = this.extendBindings(bindings, node.pattern, node.triples, 0);
        uniques = bindings[1];
        bindings = bindings[0];
        DEBUGTIMER.basic += new Date() - start;
      }

      start = new Date();
      if (cacheEntry)
        this.updateCacheResult(cacheEntry, cacheNodes, bindings);
      else
        cacheEntry = this.pushCacheResult(cacheNodes, bindings);
      if (uniques)
        for (vv in uniques)
          cacheEntry.uniques[vv] = self.union(null, uniques[vv]); // create cache map
      DEBUGTIMER.rest += new Date() - start;
      prevEntry = cacheEntry;
    }
    DEBUGTIMER.block += new Date() - blockStart;
  } else {
    orderedNodes = bestCache.nodes; // only needed for debug printing
  }

  // TODO: also store this?
  start = new Date();
  if (v) {
    //if (uniqs.length > 0)
    //  bindings = uniqs;
    //else
    // faster than _.uniq
    //bindings = this.uniq(bindings, v);

    if (orderedNodes.length === bestCache.nodes.length) {
      //if (bestCache.uniques[v])
      //  uniques = bestCache.uniques[v];
      //else {
      //  uniques = this.uniq(bestCache.bindings, v);
      //  bestCache.uniques[v] = uniques;
      //}
      //uniques = bestCache.uniques;
    }
    bindings = Object.keys(prevEntry.uniques[v] || {}); // || {} can happen if we break early out of the loop
  } else {
    bindings = _.pluck(bindings, 'binding');
  }

  DEBUGTIMER.uniq_final += new Date() - start;

  this.DEBUGtime += new Date() - DEBUGdate;
  DEBUGTIMER.total = new Date() - DEBUGdate;
  DEBUGTIMER.bestCache = bestCache.nodes.length;
  DEBUGTIMER.bindings = bindings.length;
  if (DEBUGTIMER.total > 10) {
    this.logger.info('query: ' + _.map(_.pluck(orderedNodes, 'pattern'), rdf.toQuickString).join(" "));
    this.logger.info(_.map(Object.keys(DEBUGTIMER), function (key) { return key + ': ' + DEBUGTIMER[key]; }).join(', '));
  }
  _.each(Object.keys(DEBUGTIMER), function (key) { self.DEBUGTIMERTOTAL[key] += DEBUGTIMER[key]; });

  callback(bindings);
};
var DEBUGTIMER = {pre:0, find:0, newTriples:0, updatedTriples:0, filter:0, basic:0, rest:0, extend:0, extend_triples:0, extend_tree:0, extend_merge:0, bestCache:0, uniq:0, uniq_final:0, block:0, total:0, joins:0};

N3StoreInterface.prototype.uniq = function (bindings, v) {
  var cache = {};
  for (var i = 0; i < bindings.length; ++i)
    cache[bindings[i].binding[v]] = 1;
  return Object.keys(cache);
};

N3StoreInterface.prototype.union = function (cache/*, ...*/) {
  cache = cache || {};
  var args = _.filter(Array.prototype.slice.call(arguments, 1), 'length'); // funky slice because arguments is an object
  for (var i = 0; i < args.length; ++i) {
    var vals = args[i];
    for (var j = 0; j < vals.length; ++j)
      cache[vals[j]] = 1;
  }
  return cache;
};

N3StoreInterface.prototype.extendBindings = function (leftBindings, pattern, triples, offset) {
  var branch, binding, val, i, j, valid;
  if ((leftBindings && leftBindings.length <= 0) || triples.length <= 0)
    return [[], {}];

  var start = new Date();
  var rightBindings = this.triplesToBindings(pattern, triples, offset);
  DEBUGTIMER.extend_triples += new Date() - start;
  DEBUGTIMER.extend += new Date() - start;

  if (!leftBindings) {
    var uniqs = {};
    //if (v) {
      var vars = ClusteringUtil.getVariables(pattern);
      for (i = 0; i < vars.length; ++i)
        uniqs[vars[i]] = this.uniq(rightBindings, vars[i]);
    //}
    return [rightBindings, uniqs];
  }

  var extendStart = new Date();
  start = new Date();

  var keys = _.intersection(Object.keys(leftBindings[0].binding), ClusteringUtil.getVariables(pattern));

  var minBindings = leftBindings.length < rightBindings.length ? leftBindings : rightBindings;
  var maxBindings = leftBindings.length >= rightBindings.length ? leftBindings : rightBindings;

  // merging
  var tree = this.createBindingTree(minBindings, keys);
  DEBUGTIMER.extend_tree += new Date() - start;
  start = new Date();

  var vResults = {};
  var unionKeys = _.union(Object.keys(leftBindings[0].binding), ClusteringUtil.getVariables(pattern));
  for (i = 0; i < unionKeys.length; ++i)
    vResults[unionKeys[i]] = {};
  var joined = [];
  for(i = 0; i < maxBindings.length; i++) {
    binding = maxBindings[i];
    branch = tree;
    valid = true;
    for(j = 0; j < keys.length; j++) {
      val = binding.binding[keys[j]];
      if (branch[val]) {
        branch = branch[val];
      } else {
        valid = false;
        break;
      }
    }
    if (valid) {
      // branch will be the leaf at this point
      DEBUGTIMER.joins += branch.length;
      for (j = 0; j < branch.length; j++) {
        //if (v)
        //  vResults[binding.binding[v] ? binding.binding[v] : branch[j].binding[v]] = true;
        //else
        var merge = this.mergeBindings(binding, branch[j]);

        for (var k = 0; k < unionKeys.length; ++k) {
          var key = unionKeys[k];
          vResults[key][merge[0].binding[key]] = true;
        }
        joined.push(merge[0]);
        //if (v)
        //  vResults[merge[1]] = true;
      }
    }
  }

  DEBUGTIMER.extend_merge += new Date() - start;
  DEBUGTIMER.extend += new Date() - extendStart;

  return [joined, _.mapValues(vResults, function (cache) {
    return Object.keys(cache);
  })];
};

N3StoreInterface.prototype.createBindingTree = function (bindings, keys) {
  var tree = {};
  var i, j, binding, branch, val;
  for (i = 0; i < bindings.length; ++i) {
    binding = bindings[i];
    branch = tree;
    for (j = 0; j < keys.length - 1; j++) {
      val = binding.binding[keys[j]];
      branch[val] = branch[val] || {};
      branch = branch[val];
    }
    val = binding.binding[keys[keys.length - 1]];
    branch[val] = branch[val] || [];
    branch[val].push(binding);
  }

  if (keys.length === 0)
    tree = tree[undefined];
  return tree;
};

// TODO: we only need the bindings that are necessary for the next patterns (might need all of them for the results and caching though)
N3StoreInterface.prototype.mergeBindings = function (left, right) {
  var merged = {indices:{}, binding:{}}, variable, binding;
  var val;
  //merged = Object.create(left);
  //merged.indices = Object.create(left.indices);
  //merged.binding = Object.create(left.binding);
  for (variable in left.binding)
    merged.binding[variable] = left.binding[variable];
  for (variable in right.binding)
    merged.binding[variable] = right.binding[variable];

  for (variable in left.indices)
    merged.indices[variable] = left.indices[variable];
  for (variable in right.indices)
    merged.indices[variable] = right.indices[variable];

  return [merged, val];
};

// TODO: faster if we don't actually create new objects here but just use the triple objects somehow during the merge process?
N3StoreInterface.prototype.triplesToBindings = function (pattern, triples, offset) {
  offset = offset || 0;
  var varPos = {};
  for (var pos in pattern)
    if (rdf.isVariable(pattern[pos]))
      varPos[pos] = pattern[pos];

  var patternStr = ClusteringUtil.tripleID(pattern);
  var results = new Array(triples.length);
  for (var i = 0; i < triples.length; ++i) {
    var triple = triples[i];
    var result;
    if (triple.binding) {
      result = triple.binding;
    } else {
      result = {indices: {}, binding: {}};
      result.indices[patternStr] = i + offset;
      for (pos in varPos)
        result.binding[varPos[pos]] = triple[pos];
      triple.binding = result; // store the binding in the triple object for easy re-use
    }
    results[i] = result;
  }
  return results;
};

module.exports = N3StoreInterface;