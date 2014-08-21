/**
 * Created by joachimvh on 20/08/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
  Iterator = require('../iterators/Iterator'),
  pageSize = 100; // TODO: load this from metadata

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
  var node = { pattern: pattern, count: -1, complete: false, triples: null, fixCount: {subject: -1, predicate: -1, object: -1} };
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
  var validBindings = [];
  nodes.splice(nodes.indexOf(minimalNode), 1);
  _.each(minimalNode.triples, function (triple) {
    // will be valid for every triple if varPositions is empty
    var valid = _.every(varPositions, function (pos) {
      return bindings[minimalNode.pattern[pos]] === triple[pos];
    });
    if (valid) {
      var updatedBindings = rdf.extendBindings(bindings, minimalNode.pattern, triple);
      validBindings.push(LDFClustering._getValidPaths(nodes, updatedBindings));
    }
  });
  nodes.push(minimalNode);
  validBindings = _.flatten(validBindings);
  return validBindings;
};

LDFClustering.prototype.countNode = function (node, callback) {
  var fragment = this._options.fragmentsClient.getFragmentByPattern(node.pattern);
  fragment.getProperty('metadata', function(metadata) {
    fragment.close();
    node.count = metadata.totalTriples;
    callback(this);
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
  var result = 0;
  var subjects = node.pattern.subject === bindVar ? this.clusters[bindVar].bindings : [node.pattern.subject];
  var predicates = node.pattern.predicate === bindVar ? this.clusters[bindVar].bindings : [node.pattern.predicate];
  var objects = node.pattern.object === bindVar ? this.clusters[bindVar].bindings : [node.pattern.object];

  var delayedCallback = _.after(subjects.length*predicates.length*objects.length, function() {
    if (node.pattern.subject === bindVar) node.fixCount.subject = result;
    if (node.pattern.predicate === bindVar) node.fixCount.predicate = result;
    if (node.pattern.object === bindVar) node.fixCount.object = result;
    callback(root);
  });
  _.each(subjects, function(s) {
    _.each(predicates, function(p) {
      _.each(objects, function(o) {
        var fragment = root._options.fragmentsClient.getFragmentByPattern(rdf.triple(s, p, o));
        fragment.getProperty('metadata', function(metadata){
          fragment.close();
          // TODO: other (better?) solution: change fixS to store a map of binding -> count (also allows for easy changes when bindings get reduced)
          result += Math.max(0, Math.ceil(metadata.totalTriples/pageSize)-1); // store the number of pages needed (else we can't get this data back at a later time), -1 because first page will be cached
          delayedCallback();
        });
      });
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

  // TODO: is it a problem that the incomplete data is not stored in our object?
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
          result.push(_.map(items, function (item) { return rdf.applyBindings(item, triple); }));
          delayedCallback();
        });
      });
    });
  });
};

/*
LDFClustering.prototype.propagateData = function () {
  var finished;
  var changed = false;
  var root = this;

  // compare patterns that have at least 2 similar vars
  var nodesWithTriples = _.filter(root.nodes, function(node) { return node.triples !== null; });
  finished = false;
  while (!finished) {
    finished = true;
    _.each(nodesWithTriples, function (node1, idx) {
      _.each(_.rest(nodesWithTriples, idx+1), function (node2) {
        if (_.intersection(LDFClustering.getVariables(node1.pattern), LDFClustering.getVariables(node2.pattern)).length > 1) {
          var results = LDFClustering._comparePatterns(node1, node2);
          if (results[0].length !== node1.triples.length || results[1].length !== node2.triples.length) {
            finished = false;
            node1.triples = results[0];
            node1.count = node1.triples.length;
            node2.triples = results[1];
            node2.count = node2.triples.length;
          }
        }
      });
    });
  }

  // update the bindings with the known triples
  finished = false;
  while (!finished) {
    finished = true;
    _.each(_.filter(root.nodes, function(node) { return node.complete; }), function(node) {
      var input = {triples: node.triples};
      _.each(["subject", "predicate", "object"], function(v) {
        input[v + "s"] = rdf.isVariable(node.pattern[v]) ? root.clusters[node.pattern[v]].bindings : [node.pattern[v]];
      });
      var result = LDFClustering._updateBindings(input.triples, input.subjects, input.predicates, input.objects);
      if (_.some(["subject", "predicate", "object", "triple"], function(v) {
        var vS = v + "s";
        var fixed = (node.pattern[v] && !rdf.isVariable(node.pattern[v]));
        var changed = (input[vS] === null && result[vS] !== null) || (input[vS] !== null && input[vS].length !== result[vS].length);
        return changed && !fixed;
      })) {
        finished = false; // arrays only get smaller, so we are guaranteed to finish
        changed = true;
        node.triples = result.triples;
        node.count = node.triples.length;
        _.each(
          _.filter(["subject", "predicate", "object"], function(v) { return rdf.isVariable(node.pattern[v]); }),
          function(v) { root.clusters[node.pattern[v]].bindings = result[v + "s"]; });
      }
    });
  }
  return changed;
};

LDFClustering._updateBindings = function (triples, s, p, o) {
  // prevent double checkups
  var subjects = {};
  var predicates = {};
  var objects = {};
  var result = {subjects: [], predicates: [], objects: [], triples: []};
  if (triples.length === 0 || (s !== null && s.length === 0) || (p !== null && p.length === 0) || (o !== null && o.length === 0))
    return result;
  // well that was easy
  _.each(triples, function (triple) {
    if (!subjects[triple.subject] && (s === null || s.indexOf(triple.subject) >= 0))
      result.subjects.push(triple.subject);
    subjects[triple.subject] = true;
    if (!predicates[triple.predicate] && (p === null || p.indexOf(triple.predicate) >= 0))
      result.predicates.push(triple.predicate);
    predicates[triple.predicate] = true;
    if (!objects[triple.object] && (o === null || o.indexOf(triple.object) >= 0))
      result.objects.push(triple.object);
    objects[triple.object] = true;
    if (subjects[triple.subject] && predicates[triple.predicate] && objects[triple.object])
      result.triples.push(triple);
  });
  return result;
};*/

LDFClustering.prototype.propagatePathData = function () {
  var root = this;
  var blocks = this._connectedCompleteBlocks();
  _.each(blocks, function (block) {
    var bindings = LDFClustering._getValidPaths(block.nodes);
    _.each(block.vars, function (v) {
      root.clusters[v].bindings = []; // clear the bindings since we will refill them
    });
    // update cluster bindings
    _.each(bindings, function (binding) {
      _.each(_.keys(binding), function (key) {
        root.clusters[key].bindings.push(binding[key]);
      });
    });
    // also update node triples;
    _.each(block.nodes, function (node) {
      var triples = node.triples;
      node.triples = [];
      _.each(triples, function (triple) {
        var valid = _.some(bindings, function (binding) {
          return LDFClustering._matchesBinding(node.pattern, triple, binding);
        });
        if (valid)
          node.triples.push(triple);
      });
      node.count = node.triples.length;
    });
  });
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
      return _.intersection(block.vars, vars).length > 0;
    });
    while (connected.length > 0) {
      var neighbour = connected.pop();
      completes.splice(completes.indexOf(neighbour), 1);
      block.nodes.push(neighbour);
      block.vars = _.union(LDFClustering.getVariables(neighbour.pattern), block.vars);

      connected = _.filter(completes, function (node) {
        var vars = LDFClustering.getVariables(node.pattern);
        return _.intersection(block.vars, vars).length > 0;
      });
    }
    blocks.push(block);
  }
  return blocks;
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