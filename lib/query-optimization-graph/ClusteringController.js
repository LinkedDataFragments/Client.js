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
  Stream = require('./Stream'),
  Cluster = require('./Cluster'),
  Node = require('./Node'),
  N3StoreInterface = require('./N3StoreInterface'),
  fs = require('fs');

function ClusteringController (nodes, clusters) {
  nodes = _.sortBy(nodes, function (node) { return rdf.toQuickString(node.pattern); }); // easier debugging if they are always in the same order here
  this.clusters = clusters;
  this.nodes = nodes;
  this.vars = _.union.apply(null, _.map(nodes, function (node) { return ClusteringUtil.getVariables(node.pattern); })).sort();
  this.resultCount = 0;
  _.each(nodes, function (node) {
    // TODO: save this for a later implementation?
//    node.equivalents = _.filter(nodes, function (node2) {
//      if (node === node2) return false;
//      return _.every(['subject', 'predicate', 'object'], function (pos) {
//        return node.pattern[pos] === node2.pattern[pos] || (rdf.isVariable(node.pattern[pos]) && rdf.isVariable(node2.pattern[pos]));
//      });
//    });
    // equivalent nodes should share the same download stream
    // TODO: this might give some invalid variables but this should not influence the results?
    _.each(node.equivalents, function (equivalent) {
      equivalent.fullStream = node.fullStream;
      equivalent.activeStream = equivalent.fullStream; // default setting
    });
  });
  this.logger = new Logger("ClusteringController");
  this.callback = null;
  //this.logger.disable();
  //this.store = new RDFStoreInterface();
  //this.store = new NeDBStoreInterface();
  //this.store = new HashStoreInterface(nodes);
  this.store = new N3StoreInterface();
  this.results = {};
  this.DEBUGtime = 0;
  this.DEBUGstart = new Date();
  this.DEBUGtimer = {preread:0, write_file:0, read:0, http:0, read_pre:0, read_read:0, read_post:0, read_add_pre:0, read_add_read:0,
    read_add_post:0, postread:0, postread_updates:0, postread_switchcheck:0, postread_matches:0, postread_resultscheck:0, postread_preupdate:0,
    postread_postupdate:0, postread_feed:0, postread_stabilize:0};
  this.DEBUGoriginalOrder = this.nodes;

  // TODO: make this real code
  var self = this;
  _.each(this.clusters, function (cluster) { cluster.DEBUGcontroller = self;});
}

ClusteringController.create = function (patterns, options, callback) {
  var clusters = {};
  var nodes = [];

  var delayedCallback = _.after(_.size(patterns), function () {
    var controller = new ClusteringController(nodes, clusters);
    setImmediate(callback(controller));
  });

  _.each(patterns, function (pattern) {
    var fragment = options.fragmentsClient.getFragmentByPattern(pattern);
    fragment.getProperty('metadata', function(metadata) {
      fragment.close();
      var node = new Node(pattern, metadata.totalTriples, options);
      nodes.push(node);
      var vars = ClusteringUtil.getVariables(pattern);
      _.each(vars, function (v) {
        clusters[v] = clusters[v] || new Cluster(v);
        clusters[v].nodes.push(node);
        node.DEBUGclusters[v] = clusters[v];
      });
      delayedCallback();
    });
  });
};

// TODO: don't allow nodes to supply to nodes that have a smaller count? (also not good if difference isn't big enough)
ClusteringController.prototype.start = function (callback) {
  this.callback = callback || _.noop;
  // start with best node to make sure supply gets called at least once
  var minNode = _.min(this.nodes, function (node) { return node.activeStream.cost; });

  // only minNode starts as download stream, rest becomes bound
  // TODO: won't work for unconnected parts
  var varsToUpdate = ClusteringUtil.getVariables(minNode.pattern);
  var varsDone = [];
  var parsedNodes = [minNode];
  while (!_.isEmpty(varsToUpdate)) {
    var v = varsToUpdate.shift();
    varsDone.push(v);
    var newVars = [];
    _.each(_.sortBy(this.clusters[v].nodes, function (node) { return node.fullStream.count; }), function (node) {
      // haven't updated this node yet
      if (node.activeStream === node.fullStream && node !== minNode && !node.fixed) {
        // TODO: this shouldn't be necessary, already create all binding streams?
        node.bindStreams[v] = new Stream.BindingStream(node.fullStream.count, node.pattern, v, node._options);
        node.activeStream = node.bindStreams[v];
        newVars = newVars.concat(ClusteringUtil.getVariables(node.pattern));
        parsedNodes.push(node);
      }
    });
    newVars = _.uniq(newVars);
    newVars = _.difference(newVars, varsDone);
    varsToUpdate = _.union(varsToUpdate, newVars);
  }

  var self = this;
  var changed = true;
  while (changed) {
    changed = false;
    _.each(this.nodes, function (node) {
      var bindVar = node.waitingFor();
      if (!bindVar) return;
      var suppliers = self.clusters[bindVar].suppliers();
      var full = _.every(suppliers, function (supplier) { return node.fullStream.count < supplier.fullStream.count/100; }); // TODO: pagesize
      changed |= full;
      if (full) {
        // TODO: have switch to fullstream function to prevent bugs
        node.activeStream = node.fullStream;
        _.each(node.equivalents, function (equivalent) { equivalent.activeStream = equivalent.fullStream; });
      } else {
        var minVar = _.min(ClusteringUtil.getVariables(node.pattern), function (v) {
          var suppliers = _.without(self.clusters[v].suppliers(), node);
          return _.min(_.map(suppliers, function (supplier) { return supplier.fullStream.count; }));
        });
        var switchBindVar = minVar !== bindVar;
        changed |= switchBindVar;
        if (switchBindVar) {
          // TODO: this shouldn't be necessary, already create all binding streams?
          node.bindStreams[minVar] = new Stream.BindingStream(node.fullStream.count, node.pattern, minVar, node._options);
          node.activeStream = node.bindStreams[minVar];
        }
      }
    });
  }

  parsedNodes = _.sortBy(_.filter(this.nodes, function (node) { return node.activeStream === node.fullStream; }), function (node) {return node.fullStream.count;});
  var vars = _.union.apply(null, _.map(parsedNodes, function (node) { return ClusteringUtil.getVariables(node.pattern); }));
  while (parsedNodes.length < this.nodes.length) {
    var remaining = _.filter(this.nodes, function (node) { return !_.contains(parsedNodes, node) && _.intersection(vars, ClusteringUtil.getVariables(node.pattern)).length > 0;  });
    var best = _.min(remaining, function (node) { return node.fullStream.count; });
    parsedNodes.push(best);
    vars = _.union(vars, ClusteringUtil.getVariables(best.pattern));
  }

  // TODO: make sure non-suppliers end up last?

  //this.nodes = _.sortBy(this.nodes, function (node) { return node.fullStream.count; }); // TODO: breaks var fixing
  this.nodes = parsedNodes;

  _.each(this.nodes, function (node) {
    node.updateDependency();
    node.logger.info("initial bindVar: " + node.activeStream.bindVar + " (" + node.fullStream.count + ")");
  });

  this.logger.info("node order: " + _.map(_.pluck(this.nodes, 'pattern'), rdf.toQuickString));

  //fs.writeFileSync('../execution_order.log', _.map(_.pluck(this.DEBUGoriginalOrder, 'pattern'), rdf.toQuickString).join('\n'));
  this.readNode(minNode);
};

ClusteringController.prototype.read = function () {
  //var minNode = _.min(this.nodes, function (node) { return node.cost(); });
//  var votes = _.object(_.map(this.clusters, function (cluster) { return [cluster.v, cluster.vote()]; }));
//  var hungryVotes = _.filter(votes, function (vote, v) { return vote && _.contains(hungryVars, v); });
//  var filteredVotes = _.isEmpty(hungryVotes) ? _.filter(votes) : hungryVotes;
//  var minNode = _.min(filteredVotes, function (node) { return node.cost(); });

//  var hungryVars = []; //_.uniq(_.filter(_.invoke(this.nodes, 'waitingFor')));
//  var hungryClusters = _.filter(this.clusters, function (cluster) { return _.contains(hungryVars, cluster.v); });
//  if (_.isEmpty(hungryClusters))
//    hungryClusters = this.clusters;
  var start = new Date();
  var votes = {};
  var self = this;
  var delayedCallback = _.after(_.size(this.clusters), function () {
    votes = _.omit(votes, _.isNull);
    // TODO: note to self: clusters will always be needed to store estimates per variable
    var boundNodes = _.filter(votes, function (node) { return node.activeStream.feed; });
    var suppliers = _.flatten(_.map(boundNodes, function (node) {
      if (node.activeStream.bindVar) {
        return _.without(self.clusters[node.activeStream.bindVar].supplyPath([node]), node);
      }
    }));
    // TODO: is it possible we end up with an empty list?
    var candidates = _.reject(votes, function (node) { return _.contains(suppliers, node); });
    // TODO: better value than 'cost'? what will get us the closest to the next result?
    //var minNode = ClusteringUtil.infiniMin(candidates, function (node) { return node.cost(); });
    var minNode = ClusteringUtil.infiniMin(candidates, function (node) { return node.triples.length; });
    //var minNode = ClusteringUtil.infiniMin(candidates, function (node) { return node.activeStream.count/node.activeStream.triples.length; });

    var votingResults = _.map(votes, function (node, v) {
      if (!node)
        return null;

      var patternStr = v + ":" + rdf.toQuickString(node.pattern) + "(" + node.activeStream.bindVar + ")";

      if (node === minNode)
        patternStr = "*" + patternStr + "*";
      return patternStr;
    });
    //self.logger.info("requested: " + hungryVars);
    self.logger.info("votes: " + votingResults);

    self.DEBUGtimer.preread += new Date()-start;
    if (minNode === Infinity) {
      self.DEBUGtime = new Date() - self.DEBUGstart;
      self.logger.info("RDF match time: " + Math.floor(self.store.DEBUGtime));
      self.logger.info("HTTP call time: " +  Math.floor(self.nodes[0]._options.fragmentsClient._client.DEBUGtime));
      self.logger.info("HTTP calls: " +  self.nodes[0]._options.fragmentsClient._client.DEBUGcalls);
      self.logger.info("Remaining time: " + Math.floor(self.DEBUGtime - self.store.DEBUGtime - self.nodes[0]._options.fragmentsClient._client.DEBUGtime));
      self.logger.info("Total time: " + self.DEBUGtime);
      self.logger.info(_.map(_.keys(self.DEBUGtimer), function (key) { return key + ': ' + self.DEBUGtimer[key]; }).join(', '));
      self.logger.info(_.map(_.keys(self.store.DEBUGTIMERTOTAL), function (key) { return key + ': ' + self.store.DEBUGTIMERTOTAL[key]; }).join(', '));
      self.callback([], true);

      return self.logger.info('Finished, totally not a bug!');
    } else {
      self.readNode(minNode);
    }
  });
  _.each(this.clusters, function (cluster) {
    cluster.vote(function (node) {
      votes[cluster.v] = node;
      delayedCallback();
    });
  });
};

// TODO: start a read while we are updating? (since we need to wait on http response anyway
ClusteringController.prototype.readNode = function (minNode) {
  var start = new Date();
  //var str = '';
  //var idx = this.DEBUGoriginalOrder.indexOf(minNode);
  //while (str.length < 2*this.nodes.length) {
  //  if (str.length == 2*idx)
  //    str += '| ';
  //  else
  //    str += '  ';
  //}
  //fs.appendFileSync('../execution_order.log', '\n' + str + ' : ' + rdf.toQuickString(minNode.pattern) + ' (' + minNode.activeStream.bindVar + ')');
  this.DEBUGtimer.write_file += new Date() - start;
  start = new Date();
  var minCost = minNode.cost();
  var self = this;
  self.logger.info("cost: " + minCost);
  if (minCost > 0 && _.isFinite(minCost))
    _.each(self.nodes, function (node) { node.spend(minCost); });
  self.DEBUGtimer.preread += new Date() - start;
  start = new Date();
  minNode.read(function (add, remove) {
    self.DEBUGtimer.read += new Date()-start;
    self.DEBUGtimer.http = Math.floor(self.nodes[0]._options.fragmentsClient._client.DEBUGtime);
    var startPostread = new Date();
//    _.each(add, function (triple) {
//      _.each(ClusteringUtil.getVariables(minNode.pattern), function (v) {
//        if (!minNode.store[v][triple[minNode.getVariablePosition(v)]])
//          minNode.store[v][triple[minNode.getVariablePosition(v)]] = {};
//        minNode.store[v][triple[minNode.getVariablePosition(v)]][rdf.toQuickString(triple)] = triple;
//      });
//    });
//    self.store.addTriples(add, function () {
//      // TODO: timing
////      var vars = ClusteringUtil.getVariables(minNode.pattern);
////      _.each(vars, function (v) {
////        var pos = minNode.getVariablePosition(v);
////        self.clusters[v].removeBindings(_.filter(_.pluck(remove, pos)));
////        self.clusters[v].addBindings(_.filter(_.pluck(add, pos)));
////        if (minNode.ended()) {
////          var bounds = _.uniq(_.pluck(minNode.triples, pos));
////          self.clusters[v].addBounds(bounds);
////        }
////      });
//    });

    // TODO: switching to download stream here, maybe I can do this on a per cluster basis? (not sure if safe, should check)
    // TODO: maybe also move switching binding streams?

    start = new Date();
    var delayedCallback = _.after(_.size(self.clusters), function () {
      self.DEBUGtimer.postread_updates += new Date()-start;
      start = new Date();
      // only switch to download stream if we are sure it is for the best
      // TODO: estimate can be wonky at start if there are multiple streams, need better value
      // TODO: not sure of best time yet, need to be after supply to have estimates?
      // start with the cheapest node and continue until we find an acceptable switch (or no nodes are left)
      _.some(_.sortBy(_.filter(self.nodes, function (node) { return node.activeStream.bindVar; }), function (node) { return node.fullStream.count; }), function (node) {
        var v = node.activeStream.bindVar; // TODO: will this always be the correct choice?
        // value will be infinite if no values have been matched yet
        // TODO: can't use cluster values if we want to remove supply ...
        //if (_.isFinite(self.clusters[v].estimate) && self.clusters[v].estimate > node.fullStream.cost) {
        if (_.isFinite(node.activeStream.cost) && node.activeStream.cost > 1.1*node.fullStream.cost) { // 1.1 to prevent unnecessary switching when the difference is small
          _.each([node].concat(node.equivalents), function (node) {
            if (node.activeStream === node.fullStream)
              return;
            node.logger.info("SWITCH STREAM " + v + " -> " + undefined + ", estimate: " + self.clusters[v].estimate + ", cost: " + node.fullStream.cost);
            node.activeStream = node.fullStream;
            node.triples = node.activeStream.triples;
            self.store.reset(node); // TODO: probably no way to somehow save results?
            // TODO: not sure if necessary
            // move node to the front next to the other download streams
            var idx = _.indexOf(self.nodes, node);
            self.nodes.splice(idx, 1);
            var insertIdx = _.findIndex(self.nodes, function (node) { return !node.activeStream.bindVar; });
            if (insertIdx < 0)
              insertIdx = self.nodes.length;
            self.nodes.splice(insertIdx, 0, node);
          });

          _.each(self.nodes, function (node) {
            node.updateDependency();
          });
          return true; // found a match, wait until next iteration to try again
        }
      });
      self.DEBUGtimer.postread_switchcheck += new Date() - start;

      start = new Date();
      // TODO: actually we are only interested in new results, can this be done?
      self.store.matchBindings(self.nodes, function (results) {
        self.DEBUGtimer.postread_matches += new Date() - start;
        start = new Date();
        self.logger.info("COMPLETE MATCHES: " + _.size(results));
        if (results.length < self.resultCount)
          self.logger.info("RESULTS DECREASED!");
        var gotResults = _.size(results) > self.resultCount;
        //_.each(results, function (result) {
        //  // TODO: still too much slowdown
        //  var str = bindingToString(result);
        //  if (!_.has(self.results, str)) {
        //    self.results[str] = result;
        //    //console.log(result);
        //  }
        //});
        // TODO: can we guarantee these results will always be new and never overlap with the known results? (actually yes since previous results will be loaded from cache value?)
        var newResults = results.slice(self.resultCount);
        self.callback(newResults, false);
        _.each(newResults, function (result) {
          //var str = bindingToString(result);
          //self.results[str] = result;
          self.logger.info(result);
        });
        self.resultCount = Math.max(_.size(results), self.resultCount);

        //function bindingToString (binding) {
        //  return _.map(self.vars, function (key) { return key+' '+binding[key]; }).join(' ');
        //}
        self.DEBUGtimer.postread_resultscheck += new Date() - start;
        self.DEBUGtimer.postread += new Date() - startPostread;

        if (gotResults) {
          self.DEBUGtime = new Date() - self.DEBUGstart;
          self.logger.info("RDF match time: " + Math.floor(self.store.DEBUGtime));
          self.logger.info("HTTP call time: " +  Math.floor(self.nodes[0]._options.fragmentsClient._client.DEBUGtime));
          self.logger.info("HTTP calls: " +  self.nodes[0]._options.fragmentsClient._client.DEBUGcalls);
          self.logger.info("Remaining time: " + Math.floor(self.DEBUGtime - self.store.DEBUGtime - self.nodes[0]._options.fragmentsClient._client.DEBUGtime));
          self.logger.info("Total time: " + self.DEBUGtime);
          self.logger.info(_.map(_.keys(self.DEBUGtimer), function (key) { return key + ': ' + self.DEBUGtimer[key]; }).join(', '));
          self.logger.info(_.map(_.keys(self.store.DEBUGTIMERTOTAL), function (key) { return key + ': ' + self.store.DEBUGTIMERTOTAL[key]; }).join(', '));
        }

        //if (results.length >= 50) {
        //  self.logger.info("TOTAL TIME: " + (new Date() - self.DEBUGstart));
        //  process.exit(0);
        //}

        setImmediate(function () { self.read(); });
      });
    });
    _.each(self.clusters, function (cluster) {
      cluster.update(minNode, delayedCallback);
    });
  });
};

//ClusteringController.prototype.read2 = function () {
//  var minNode = _.min(this.nodes, function (node) { return node.cost(); });
//
//  if (minNode === Infinity)
//    return console.error('Finished, totally not a bug!');
//
//  var minCost = minNode.cost();
//
//  // TODO: count unique values on page download -> more is good (/total ?)
//  var self = this;
//  _.each(this.nodes, function (node) {
//    // TODO: problem: expensive streams, need to detect bound or not
//    node.read2(minCost, function (add, remove) {
//      // TODO: count each value?
//      var vars = ClusteringUtil.getVariables(node.pattern);
//      _.each(vars, function (v) {
//        self.clusters[v].removeBindings(_.filter(_.pluck(remove, v)));
//        if (node.supplies(v))
//          self.clusters[v].addBindings(_.filter(_.pluck(add, v)));
//        // TODO: bounds and stuff
//      });
//    });
//  });
//};

//ClusteringController.prototype.getAllPaths = function (node1, node2, varsUsed, used) {
//  if (node1 === node2 && !_.isEmpty(used))
//    return [[node1]];
//  used = used || [];
//  used = used.concat([node1.pattern]);
//  var self = this;
//  var legalNeighbours = _.flatten(_.map(_.difference(ClusteringUtil.getVariables(node1.pattern), varsUsed), function (v) { return self.clusters[v].nodes; }));
//  legalNeighbours = _.filter(legalNeighbours, function (node) { return !ClusteringUtil.containsObject(used, node.pattern); });
//  legalNeighbours = _.uniq(legalNeighbours, function (node) { return rdf.toQuickString(node.pattern); });
//  var paths = _.flatten(_.map(legalNeighbours, function (node) {
//    // TODO: possibly incorrect if multiple vars match or one var occurs multiple times
//    var v = _.first(_.difference(_.intersection(ClusteringUtil.getVariables(node1.pattern), ClusteringUtil.getVariables(node.pattern)), varsUsed));
//    var neighbourPaths = self.getAllPaths(node, node2, varsUsed.concat([v]), used);
//    return _.map(neighbourPaths, function (path) { return [node1].concat(path); });
//  }), true);
//  return paths;
//};

//ClusteringController.prototype.validatePath = function (binding, path) {
//  // TODO: starting from both sides probably faster
//  var validBindings = [binding];
//  while (!_.isEmpty(path) && !_.isEmpty(validBindings)) {
//    var node = _.first(path);
//    path = _.rest(path);
//    validBindings = _.flatten(_.map(validBindings, function (binding) {
//      // TODO: lots of double work prolly
//      return _.filter(_.map(node.activeStream.triples, function (triple) {
//        try { return rdf.extendBindings(binding, node.pattern, triple);}
//        catch (bindingError) { return null; }
//      }));
//    }));
//  }
//  return !_.isEmpty(validBindings);
//};

module.exports = ClusteringController;