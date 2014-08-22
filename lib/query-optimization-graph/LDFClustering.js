/**
 * Created by joachimvh on 20/08/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
  Iterator = require('../iterators/Iterator'),
  rdfstore = require('rdfstore'),
  N3 = require('N3');

// TODO: semantic graph
// TODO: take max cache size into account?

// options are necessary to access LDF data
function LDFClustering (options, patterns) {
  this.nodes = [];
  this.clusters = {};
  this._options = options;
  var root = this;
  _.each(patterns, function (pattern) { root.addTriplePattern(pattern); });
}

LDFClustering.prototype.isFinished = function () {
  return _.every(this.nodes, function (node) { return node.complete; });
};

LDFClustering.getVariables = function (triple) {
  return _.filter([triple.subject, triple.predicate, triple.object], function(v){ return rdf.isVariable(v); });
};

LDFClustering.prototype._addCluster = function (key, bindings) {
  this.clusters[key] = {
    nodes: _.filter(this.nodes, function (node) { return LDFClustering.getVariables(node.pattern).indexOf(key) >= 0; }),
    bindings: bindings ? bindings : null,
    key: key
  };
};

LDFClustering.prototype.addTriplePattern = function (pattern) {
  if (!rdf.hasVariables(pattern))
    return;
  var node = { pattern: pattern, count: -1, complete: false, triples: null, fixCount: {} };
  var root = this;
  _.each(LDFClustering.getVariables(pattern), function(entity) {
    if (!root.clusters.hasOwnProperty(entity))
      root._addCluster(entity);
    root.clusters[entity].nodes.push(node);
  });
  root.nodes.push(node);
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
    return _.some(LDFClustering.getVariables(node.pattern), function (v) {
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

// same speed as recursive version
/*LDFClustering._getValidPathsLoop = function (nodes) {
  nodes = _.clone(nodes);
  var bindings = [{}];

  while (nodes.length > 0) {
    var bindingNodes = _.filter(nodes, function(node) {
      return _.some(LDFClustering.getVariables(node.pattern), function (v) {
        return bindings[0][v];
      });
    });
    if (bindingNodes.length <= 0)
      bindingNodes = nodes;
    var node = _.min(bindingNodes, function (node) { return node.count; });
    nodes.splice(nodes.indexOf(node), 1);
    var varPositions = _.filter(["subject", "predicate", "object"], function (pos) {
      return rdf.isVariable(node.pattern[pos]) && bindings[0][node.pattern[pos]]; // bindings will at least always have 1 element, all elements have the same keys
    });

    bindings = _.flatten(_.map(bindings, function (binding) {
      // for every valid triple, extend the binding
      var validTriples = _.filter(node.triples, function (triple) {
        // every triple will be valid if varPositions is empty
        return _.every(varPositions, function (pos) {
          return binding[node.pattern[pos]] === triple[pos];
        });
      });
      return _.map(validTriples, function (triple) {
        return rdf.extendBindings(binding, node.pattern, triple);
      });
    }));
  }
  return bindings;
};*/

LDFClustering.prototype.countNode = function (node, callback) {
  var root = this;
  var fragment = this._options.fragmentsClient.getFragmentByPattern(node.pattern);
  fragment.getProperty('metadata', function(metadata) {
    fragment.close();
    node.count = metadata.totalTriples;
    callback(root);
  });
};

LDFClustering.prototype.downloadNode = function (node, callback) {
  var root = this;
  var iterator = new TriplePatternIterator(Iterator.single({}), node.pattern, this._options);
  iterator.toArray(function(error, items) {
    if (!items)
      throw new Error(error);
    node.triples = _.map(items, function(item) {
      return rdf.applyBindings(item, node.pattern);
    });
    callback(root);
  });
};

LDFClustering.prototype.countBindings = function (node, bindVar, callback) {
  var root = this;

  node.fixCount[bindVar] = {};

  var delayedCallback = _.after(root.clusters[bindVar].bindings.length, function () { callback(root); });
  _.each(root.clusters[bindVar].bindings, function (binding) {
    var triple = rdf.triple(
      node.pattern.subject === bindVar ? binding : node.pattern.subject,
      node.pattern.predicate === bindVar ? binding : node.pattern.predicate,
      node.pattern.object === bindVar ? binding : node.pattern.object
    );
    var fragment = root._options.fragmentsClient.getFragmentByPattern(triple);
    fragment.getProperty('metadata', function(metadata){
      fragment.close();
      //console.error("COUNTED (" + metadata.totalTriples + "): { " + triple.subject + " " + triple.predicate + " " + triple.object + " }");
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

  var root = this;
  var result = [];
  var delayedCallback = _.after(subjects.length*predicates.length*objects.length, function() {
    node.triples = _.flatten(result);
    node.complete = node.triples.count;
    node.complete = true;
    callback(root);
  });
  subjects.forEach(function (subject) {
    predicates.forEach(function (predicate) {
      objects.forEach(function (object) {
        var triple = rdf.triple(subject, predicate, object);
        var iterator = new TriplePatternIterator(Iterator.single({}), triple, root._options);
        iterator.toArray(function(error, items) {
          //console.error("DOWNLOADED (" + items.length + "): { " + triple.subject + " " + triple.predicate + " " + triple.object + " }");
          result.push(_.map(items, function (item) { return rdf.applyBindings(item, triple); }));
          delayedCallback();
        });
      });
    });
  });
};

LDFClustering.prototype._rdfstoreBenchmark = function () {
  var root = this;
  var nodes = _.filter(root.nodes, function (node) { return node.complete; });
  if (nodes.length <= 0)
    return;
  var N3Util = N3.Util;
  var queryBody = _.map(nodes, function (node) {
    return _.map(["subject", "predicate", "object"], function (pos) {
      return N3Util.isUri(node.pattern[pos]) && !rdf.isVariable(node.pattern[pos]) ? "<" + node.pattern[pos] + ">" : node.pattern[pos];
    }).join(" ");
  }).join(" . ");
  var queryVariables = _.union.apply(null, _.map(nodes, function (node) { return LDFClustering.getVariables(node.pattern); }));
  var query = "SELECT " + queryVariables.join(" ") + " WHERE { " + queryBody + " }";
  console.error("QUERY: " + query);
  var triples = _.flatten(_.map(nodes, function (node) { return node.triples; }));
  var writer = N3.Writer();
  _.each(triples, function (triple) { writer.addTriple(triple.subject, triple.predicate, triple.object); });
  writer.end(function (error, result) {
    rdfstore.create(function (store) {
      store.load("text/turtle", result, function (success, results) {
        console.time("RDF");
        store.execute(query, function (success, results) {
          console.timeEnd("RDF");
        });
      });
    });
  });
};

LDFClustering.prototype.propagatePathData = function () {
  console.time("PROPAGATE");
  console.error("UPDATING DATA");
  var root = this;

  // TODO: DEBUG
  var DEBUG_STORAGE = {};

  this._rdfstoreBenchmark();

  this._weakClustering(); // quickly update some easy parts to speed up the exact path detection

  var blocks = this._connectedCompleteBlocks();
  _.each(blocks, function (block) {
    console.time("PATH");
    var bindings = LDFClustering._getValidPaths(block.nodes);
    console.error("BINDINGS COUNT: " + bindings.length);
    console.timeEnd("PATH");

    console.time("CLUSTERS");
    _.each(block.vars, function (v) {
      DEBUG_STORAGE[v] = root.clusters[v].bindings ? root.clusters[v].bindings.length : 0;
      root.clusters[v].bindings = _.uniq(_.map(bindings, function (binding) { return binding[v]; }));
    });
    console.timeEnd("CLUSTERS");

    // TODO: DEBUG
    _.each(block.vars, function (v) {
      if (DEBUG_STORAGE[v] !== root.clusters[v].bindings.length)
        console.error("CHANGED " + v + ": " + DEBUG_STORAGE[v] + " -> " + root.clusters[v].bindings.length);
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
  console.error("RECURSION STEPS: " + LDFClustering.DEBUG);
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
    var block = {nodes: [initialNode], vars: LDFClustering.getVariables(initialNode.pattern)};
    var connected = _.filter(completes, function (node) {
      var vars = LDFClustering.getVariables(node.pattern);
      return _.intersection(this.vars, vars).length > 0;
    }, block);
    while (connected.length > 0) {
      var neighbour = connected.pop();
      completes.splice(completes.indexOf(neighbour), 1);
      block.nodes.push(neighbour);
      block.vars = _.union(LDFClustering.getVariables(neighbour.pattern), block.vars);

      connected = _.filter(completes, function (node) {
        var vars = LDFClustering.getVariables(node.pattern);
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
      var vals = _.uniq(_.map(node.triples, function (triple) {
        return triple[pos];
      }));
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

/*
LDFClustering._comparePatterns = function (node1, node2) {
  var matches = [];
  ["subject", "predicate", "object"].forEach(function (x){
    ["subject", "predicate", "object"].forEach(function (y) {
      if (node1.pattern[x] === node2.pattern[y])
        matches.push([x, y]);
    });
  });
  var validNode1PatternsBool = [];
  for (var i = 0; i < node1.triples.length; ++i)
    validNode1PatternsBool.push(false);
  var validNode2PatternsBool = [];
  for (i = 0; i < node2.triples.length; ++i)
    validNode2PatternsBool.push(false);

  _.each(node1.triples, function (triple1, idx1) {
    _.each(node2.triples, function (triple2, idx2) {
      var valid = _.every(matches, function (match) {
        return node1.pattern[match[0]] === node2.pattern[match[1]];
      });
      if (valid) {
        validNode1PatternsBool[idx1] = true;
        validNode2PatternsBool[idx2] = true;
      }
    });
  });

  return [
    _.filter(node1.triples, function (val, idx) { return validNode1PatternsBool[idx]; }),
    _.filter(node2.triples, function (val, idx) { return validNode2PatternsBool[idx]; })
  ];
};*/

module.exports = LDFClustering;