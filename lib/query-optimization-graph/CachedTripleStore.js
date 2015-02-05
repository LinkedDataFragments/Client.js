/**
 * Created by joachimvh on 3/10/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  Logger = require('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil');

function N3StoreInterface() {
  this.cache = {};
  this.lastCounts = {};
  this.logger = new Logger("N3StoreInterface");
  this.logger.disable();
}

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
      return node.triples.length * prevEntry.bindings.length;
    else
      return node.triples.length * node.triples.length;
  }
  var oldCount = nextEntry.counts[ClusteringUtil.tripleID(node.pattern)];
  var newCount = node.triples.length - oldCount;
  var diffs = _.map(prevEntry.nodes, function (prevNode) {
    var count = nextEntry.counts[ClusteringUtil.tripleID(prevNode.pattern)];
    return prevNode.triples.length - count;
  });
  // this looks more complicated than the other version, but gives worse results...
  //return newCount*(1+ (prevEntry.bindings ? prevEntry.bindings.length : oldCount)) + (diffs.length > 0 ? Math.max.apply(null, diffs) : 0)*oldCount;
  return newCount + (diffs.length > 0 ? Math.max.apply(null, diffs) : 0);
};

N3StoreInterface.prototype.cachePath = function (nodes, entry, current, best) {
  current = current || {path: [], cost: 0};
  best = best || {path: [], cost: Infinity};
  entry = entry || {nodes: []};

  if (nodes.length <= 0) {
    if (current.cost < best.cost) {
      best.cost = current.cost;
      best.path = current.path;
    }
    return best;
  }

  var vars = _.union.apply(null, _.map(entry.nodes, function (node) { return rdf.getVariables(node.pattern); }));
  for (var i = 0; i < nodes.length; ++i) {
    var node = nodes[i];
    var nodeVars = rdf.getVariables(node.pattern);
    if (vars.length > 0 && _.intersection(vars, nodeVars).length <= 0)
      continue;

    var cost = this.cacheCost(node, entry);
    var nextCost = current.cost * current.cost + cost; // add more importance to early costs since these new bindings will be used more
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

N3StoreInterface.prototype.unusedBindings = function (oldEntry, newEntry) {
  if (!oldEntry || !oldEntry.bindings)
    return [];
  if (!newEntry || !newEntry.counts)
    return oldEntry.bindings;
  var incompleteStrings = _.filter(oldEntry.strings, function (str) { return newEntry.counts[str] < oldEntry.counts[str]; });
  var filtered = [];
  if (incompleteStrings.length > 0) {
    for (var i = oldEntry.bindings.length - 1; i >= 0; --i) {
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
  var bestMatch = {nodes: [], bindings: null, counts: {}};
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
    if (!entry.counts.hasOwnProperty(str)) return true;
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
  var strings = _.map(nodes, function (node) { return ClusteringUtil.tripleID(node.pattern); }); // gets used when filtering old/new previous bindings
  var counts = _.object(strings, _.map(nodes, function (node) { return node.activeStream.tripleCount; }));
  var entry = {strings: strings, counts: counts, nodes: nodes, bindings: bindings, uniques: {}};
  _.each(nodes, function (node) { _.each(rdf.getVariables(node.pattern), function (v) { entry.uniques[v] = []; }); });
  this.cache[this.cacheID(nodes)] = entry;
  return entry;
};

N3StoreInterface.prototype.matchBindings = function (nodes, callback, v) {
  var bestCache = this.maxMatchingResult(nodes);
  var bindings = bestCache.bindings;
  nodes = _.difference(nodes, bestCache.nodes);
  nodes = this.cachePath(nodes, bestCache).path;
  var prevEntry = bestCache;
  if (nodes.length > 0) {
    while (nodes.length > 0) {
      if (bindings && bindings.length === 0)
        break;
      var node = nodes.shift();
      var cacheNodes = prevEntry.nodes.concat([node]);
      var cacheEntry = this.cacheResult(cacheNodes);

      var uniques = null, vv;
      if (cacheEntry) {
        var filtered = this.unusedBindings(prevEntry, cacheEntry);

        var oldTriples = node.triples.slice(0, cacheEntry.counts[ClusteringUtil.tripleID(node.pattern)]);
        var oldBindings = this.extendBindings(filtered, node.pattern, oldTriples, 0);

        var newTriples = node.triples.slice(cacheEntry.counts[ClusteringUtil.tripleID(node.pattern)]);
        var newBindings = this.extendBindings(bindings, node.pattern, newTriples, cacheEntry.counts[ClusteringUtil.tripleID(node.pattern)]);

        for (vv in cacheEntry.uniques)
          cacheEntry.uniques[vv] = this.union(cacheEntry.uniques[vv], oldBindings.uniques[vv] || [], newBindings.uniques[vv] || []);

        bindings = cacheEntry.bindings.concat(oldBindings.bindings.concat(newBindings.bindings));
      } else {
        // no matching cache entries, usually only happens at the start of the algorithm
        bindings = this.extendBindings(bindings, node.pattern, node.triples, 0);
        uniques = bindings.uniques;
        bindings = bindings.bindings;
      }

      if (cacheEntry)
        this.updateCacheResult(cacheEntry, cacheNodes, bindings);
      else
        cacheEntry = this.pushCacheResult(cacheNodes, bindings);
      if (uniques)
        for (vv in uniques)
          cacheEntry.uniques[vv] = this.union(null, uniques[vv]); // create cache map
      prevEntry = cacheEntry;
    }
  }

  if (v)
    bindings = Object.keys(prevEntry.uniques[v] || {}); // || {} can happen if we break early out of the loop
  else
    bindings = _.pluck(bindings, 'binding');

  callback(bindings);
};

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
    return {bindings: [], uniques: {}};

  var rightBindings = this.triplesToBindings(pattern, triples, offset);

  if (!leftBindings) {
    var uniqs = {};
    var vars = rdf.getVariables(pattern);
    for (i = 0; i < vars.length; ++i)
      uniqs[vars[i]] = this.uniq(rightBindings, vars[i]);
    return {bindings: rightBindings, uniques: uniqs};
  }

  var keys = _.intersection(Object.keys(leftBindings[0].binding), rdf.getVariables(pattern));

  var minBindings = leftBindings.length < rightBindings.length ? leftBindings : rightBindings;
  var maxBindings = leftBindings.length >= rightBindings.length ? leftBindings : rightBindings;

  // merging
  var tree = this.createBindingTree(minBindings, keys);

  var vResults = {};
  var unionKeys = _.union(Object.keys(leftBindings[0].binding), rdf.getVariables(pattern));
  for (i = 0; i < unionKeys.length; ++i)
    vResults[unionKeys[i]] = {};
  var joined = [];
  for (i = 0; i < maxBindings.length; i++) {
    binding = maxBindings[i];
    branch = tree;
    valid = true;
    for (j = 0; j < keys.length; j++) {
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
      for (j = 0; j < branch.length; j++) {
        var merge = this.mergeBindings(binding, branch[j]);

        for (var k = 0; k < unionKeys.length; ++k) {
          var key = unionKeys[k];
          vResults[key][merge.binding[key]] = true;
        }

        joined.push(merge);
      }
    }
  }

  return {bindings: joined, uniques: _.mapValues(vResults, function (cache) {
    return Object.keys(cache);
  })};
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

N3StoreInterface.prototype.mergeBindings = function (left, right) {
  var merged = {indices: {}, binding: {}}, variable;
  for (variable in left.binding)
    merged.binding[variable] = left.binding[variable];
  for (variable in right.binding)
    merged.binding[variable] = right.binding[variable];

  for (variable in left.indices)
    merged.indices[variable] = left.indices[variable];
  for (variable in right.indices)
    merged.indices[variable] = right.indices[variable];

  return merged;
};

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