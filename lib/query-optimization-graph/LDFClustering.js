/**
 * Created by joachimvh on 20/08/2014.
 */

var rdf = require('../util/RdfUtil'),
    _ = require('lodash'),
    TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
    Iterator = require('../iterators/Iterator'),
    RDFStoreInterface = require('./RDFStoreInterface'),
    N3 = require('n3'),
    Logger = require ('../util/Logger'),
    ClusteringUtil = require('./ClusteringUtil');

// TODO: semantic graph
// TODO: take max cache size into account?

// options are necessary to access LDF data
function LDFClustering (options, patterns) {
  this.nodes = [];
  this.clusters = {};
  this.store = new RDFStoreInterface();
  this._options = options;
  this._logger = new Logger("LDFClustering");
  this._logger.disable();
  var self = this;
  if (patterns)
    _.each(patterns, function (pattern) { self.addTriplePattern(pattern); });
}

LDFClustering.prototype.isFinished = function () {
  return _.every(this.nodes, function (node) { return node.complete; });
};

LDFClustering._toN3String = function (triples, callback) {
  var writer = N3.Writer();
  _.each(triples, function (triple) { writer.addTriple(triple.subject, triple.predicate, triple.object); });
  writer.end(callback);
};

LDFClustering.prototype._addTriplesToStore = function (triples, callback) {
  console.time("STORE ADD");
  var self = this;
  this.store.addTriples(triples, function () {
    console.timeEnd("STORE ADD");
    if (callback)
      callback();
  });
};

LDFClustering.prototype._getVariableBindings = function (v, callback) {
  var cluster = this.clusters[v];
  if (!cluster)
    return;
  var nodes = _.filter(cluster.nodes, function (node) { return node.complete; });
  if (nodes.length <= 0)
    return;
  this.store.matchBindings(_.pluck(nodes, 'pattern'), function (results) {
    results = _.uniq(_.pluck(results, v));
    callback(results);
  });
};

LDFClustering.prototype._addCluster = function (key, bindings) {
  this.clusters[key] = {
    nodes: _.filter(this.nodes, function (node) { return ClusteringUtil.getVariables(node.pattern).indexOf(key) >= 0; }),
    bindings: bindings ? bindings : null,
    key: key
  };
};

LDFClustering.prototype.addTriplePattern = function (pattern) {
  if (!rdf.hasVariables(pattern))
    return;
  var node = { pattern: pattern, count: -1, complete: false, triples: null, fixCount: {} };
  var self = this;
  _.each(ClusteringUtil.getVariables(pattern), function(entity) {
    if (!self.clusters.hasOwnProperty(entity))
      self._addCluster(entity);
    self.clusters[entity].nodes.push(node);
  });
  self.nodes.push(node);
};

LDFClustering.prototype.setBinding = function (key, vals) {
  if (!vals.length)
    vals = [vals];
  this._addCluster(key, vals);
};

LDFClustering.prototype.getResults = function () {
  return LDFClustering._getValidPaths(_.filter(this.nodes, function (node) { return node.complete; }));
};

LDFClustering.DEBUG = 0;
// 23s
LDFClustering._getValidPaths = function (nodes, bindings) {
  if (!bindings)
    bindings = {};
  if (nodes.length === 0)
    return bindings;
  var bindingNodes = _.filter(nodes, function(node) {
    return _.some(ClusteringUtil.getVariables(node.pattern), function (v) {
      return bindings[v];
    });
  });
  // no nodes that have variables matching one of the bindings (probably empty bindings or some nodes are unconnected)
  if (bindingNodes.length <= 0)
    bindingNodes = nodes;
  var minimalNode = _.min(bindingNodes, function (node) { return node.count; });
  var varPositions = _.filter(["subject", "predicate", "object"], function (pos) {
    return rdf.isVariable(minimalNode.pattern[pos]) && bindings[minimalNode.pattern[pos]];
  });
  nodes.splice(nodes.indexOf(minimalNode), 1);
  ++LDFClustering.DEBUG;
  var validTriples = _.filter(minimalNode.triples, function (triple) {
    // every triple will be valid if varPositions is empty
    return _.every(varPositions, function (pos) {
      return bindings[minimalNode.pattern[pos]] === triple[pos];
    });
  });

  var validBindings = _.map(validTriples, function (triple) {
    var updatedBindings = rdf.extendBindings(bindings, minimalNode.pattern, triple);
    return LDFClustering._getValidPaths(nodes, updatedBindings);
  });

  validBindings = _.flatten(validBindings);

  nodes.push(minimalNode);

  return validBindings;
};

LDFClustering.prototype.countNode = function (node, callback) {
  var self = this;
  var fragment = this._options.fragmentsClient.getFragmentByPattern(node.pattern);
  fragment.getProperty('metadata', function(metadata) {
    fragment.close();
    node.count = metadata.totalTriples;
    callback(self);
  });
};

LDFClustering.prototype.downloadNode = function (node, callback) {
  var self = this;
  var iterator = new TriplePatternIterator(Iterator.single({}), node.pattern, this._options);
  iterator.toArray(function(error, items) {
    if (!items)
      throw new Error(error);
    node.triples = _.map(items, function(item) {
      return rdf.applyBindings(item, node.pattern);
    });
    self._addTriplesToStore(node.triples);
    node.complete = true;
    node.count = node.triples.length;
    callback(self);
  });
};

LDFClustering.prototype.countBindings = function (node, bindVar, callback) {
  var self = this;

  node.fixCount[bindVar] = {};

  var delayedCallback = _.after(self.clusters[bindVar].bindings.length, function () { callback(self); });
  _.each(self.clusters[bindVar].bindings, function (binding) {
    var triple = rdf.triple(
      node.pattern.subject === bindVar ? binding : node.pattern.subject,
      node.pattern.predicate === bindVar ? binding : node.pattern.predicate,
      node.pattern.object === bindVar ? binding : node.pattern.object
    );
    var fragment = self._options.fragmentsClient.getFragmentByPattern(triple);
    fragment.getProperty('metadata', function(metadata){
      fragment.close();
      self._logger.info("COUNTED (" + metadata.totalTriples + "): { " + triple.subject + " " + triple.predicate + " " + triple.object + " }");
      node.fixCount[bindVar][binding] = metadata.totalTriples;

      delayedCallback();
    });
  });
};

LDFClustering.prototype.applyBindings = function (node, vars, callback) {
  if (!vars.hasOwnProperty('length'))
    vars = [vars];
  var p = node.pattern;
  var subjects = vars.indexOf(p.subject) >= 0 ? this.clusters[p.subject].bindings : [p.subject];
  var predicates = vars.indexOf(p.predicate) >= 0 ? this.clusters[p.predicate].bindings : [p.predicate];
  var objects = vars.indexOf(p.object) >= 0 ? this.clusters[p.object].bindings : [p.object];
  if (!subjects || !predicates || !objects)
    throw new Error('Invalid input. All fixed variables need to have matching non-null clusters.');

  var self = this;
  var result = [];
  var delayedCallback = _.after(subjects.length*predicates.length*objects.length, function() {
    node.triples = _.flatten(result);
    self._addTriplesToStore(node.triples);
    node.count = node.triples.length;
    node.complete = true;
    callback(self);
  });
  subjects.forEach(function (subject) {
    predicates.forEach(function (predicate) {
      objects.forEach(function (object) {
        var triple = rdf.triple(subject, predicate, object);
        var iterator = new TriplePatternIterator(Iterator.single({}), triple, self._options);
        iterator.toArray(function(error, items) {
          self._logger.info("DOWNLOADED (" + items.length + "): { " + triple.subject + " " + triple.predicate + " " + triple.object + " }");
          result.push(_.map(items, function (item) { return rdf.applyBindings(item, triple); }));
          delayedCallback();
        });
      });
    });
  });
};

LDFClustering.prototype._rdfstoreBenchmark = function () {
  var self = this;
  console.time("RDF");
  this.store.matchBindings(
    _.pluck(_.filter(self.nodes, function (node) { return node.complete; }), 'pattern'),
    function () { console.timeEnd("RDF"); }
  );
};

LDFClustering.prototype.propagatePathData = function () {
  console.time("PROPAGATE");
  var self = this;
  self._logger.info("UPDATING DATA");

  // TODO: DEBUG
  var DEBUG_STORAGE = {};

  //this._rdfstoreBenchmark();

  this._weakClustering(); // quickly update some easy parts to speed up the exact path detection

  var blocks = this._connectedCompleteBlocks();
  _.each(blocks, function (block) {
    console.time("PATH");
    var bindings = LDFClustering._getValidPaths(block.nodes);
    self._logger.info("BINDINGS COUNT: " + bindings.length);
    console.timeEnd("PATH");

    console.time("CLUSTERS");
    _.each(block.vars, function (v) {
      DEBUG_STORAGE[v] = self.clusters[v].bindings ? self.clusters[v].bindings.length : 0;
      self.clusters[v].bindings = _.uniq(_.pluck(bindings, v));
    });
    console.timeEnd("CLUSTERS");

    // TODO: DEBUG
    _.each(block.vars, function (v) {
      if (DEBUG_STORAGE[v] !== self.clusters[v].bindings.length)
        self._logger.info("CHANGED " + v + ": " + DEBUG_STORAGE[v] + " -> " + self.clusters[v].bindings.length);
    });

    // unfortunately the following block slows down the process instead of speeding it up
    /*console.time("TRIPLES");
    // also update node triples;
    // TODO: also remove unneeded fixcosts
    _.each(block.nodes, function (node) {
      var varPositions = _.filter(["subject", "predicate", "object"], function (pos) { return rdf.isVariable(node.pattern[pos]) && block.vars.indexOf(node.pattern[pos]) >= 0; });

      node.triples = _.filter(node.triples, function (triple) {
        return _.some(bindings, function (binding) {
          return _.every(varPositions, function (pos) {
            return triple[pos] === binding[node.pattern[pos]];
          });
        });
      });
      node.count = node.triples.length;
    });
    console.timeEnd("TRIPLES");*/
  });
  self._logger.info("RECURSION STEPS: " + LDFClustering.DEBUG);
  LDFClustering.DEBUG = 0;
};

LDFClustering._matchesBinding = function (pattern, triple, binding) {
  return (!rdf.isVariable(pattern.subject)   || binding[pattern.subject]   === triple.subject) &&
         (!rdf.isVariable(pattern.predicate) || binding[pattern.predicate] === triple.predicate) &&
         (!rdf.isVariable(pattern.object)    || binding[pattern.object]    === triple.object);
};

LDFClustering.prototype._connectedCompleteBlocks = function () {
  // find all connected blocks of complete nodes (patterns that are connected through variables might be unconnected now because some nodes are still incomplete)
  var completes = _.filter(this.nodes, function (node) { return node.complete; });
  var blocks = [];
  while (completes.length > 0) {
    var initialNode = completes.pop();
    var block = {nodes: [initialNode], vars: ClusteringUtil.getVariables(initialNode.pattern)};
    var connected = _.filter(completes, function (node) {
      var vars = ClusteringUtil.getVariables(node.pattern);
      return _.intersection(this.vars, vars).length > 0;
    }, block);
    while (connected.length > 0) {
      var neighbour = connected.pop();
      completes.splice(completes.indexOf(neighbour), 1);
      block.nodes.push(neighbour);
      block.vars = _.union(ClusteringUtil.getVariables(neighbour.pattern), block.vars);

      connected = _.filter(completes, function (node) {
        var vars = ClusteringUtil.getVariables(node.pattern);
        return _.intersection(this.vars, vars).length > 0;
      }, block);
    }
    blocks.push(block);
  }
  return blocks;
};

LDFClustering.prototype._weakClustering = function () {
  var nodes = _.filter(this.nodes, function (node) { return node.complete; });
  var clusters = this.clusters;
  // some preprocessing to minimize the data expansion
  _.each(nodes, function (node) {
    var varPositions = _.filter(["subject", "predicate", "object"], function (pos) {
      return rdf.isVariable(node.pattern[pos]);
    });
    _.each(varPositions, function (pos) {
      var vals = _.uniq(_.pluck(node.triples, pos));
      if (clusters[node.pattern[pos]].bindings) {
        clusters[node.pattern[pos]].bindings = _.intersection(clusters[node.pattern[pos]].bindings, vals);
      } else {
        clusters[node.pattern[pos]].bindings = vals;
      }
    });
  });
  _.each(nodes, function (node) {
    var varPositions = _.filter(["subject", "predicate", "object"], function (pos) {
      return rdf.isVariable(node.pattern[pos]);
    });
    node.triples = _.filter(node.triples, function (triple) {
      return _.every(varPositions, function (pos) {
        return _.contains(clusters[node.pattern[pos]].bindings, triple[pos]);
      });
    });
  });
};

module.exports = LDFClustering;