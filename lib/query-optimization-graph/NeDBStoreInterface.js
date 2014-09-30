/**
 * Created by joachimvh on 30/09/2014.
 */

var Datastore = require('nedb'),
    rdf = require('../util/RdfUtil'),
    _ = require('lodash'),
    rdfstore = require('rdfstore'),
    N3 = require('N3'),
    Logger = require ('../util/Logger'),
    ClusteringUtil = require('./ClusteringUtil');

function NeDBStoreInterface() {
  this.db = new Datastore();
  this.logger = new Logger("NeDBStore");
  this.logger.disable();

  this.db.ensureIndex({ fieldName: 'subject' });
  this.db.ensureIndex({ fieldName: 'predicate' });
  this.db.ensureIndex({ fieldName: 'object' });
}

NeDBStoreInterface.prototype.addTriples = function (triples, callback) {
  // well that was easy
  this.db.insert(triples, callback);
};


NeDBStoreInterface.prototype.matchBindings = function (patterns, callback) {
  // assume we are happy with the way the patterns are sorted alreay
  if (_.size(patterns) < 1) {
    callback([]);
  } else {
    var bindings = null;
    var vars = [];
    var sortVar = null;
    var self = this;
    _iterate();
    function _iterate () {
      if (_.isEmpty(patterns))
        return callback(bindings);
      var patternIdx = _.findIndex(patterns, function (p) { return !_.isEmpty(_.intersection(vars, ClusteringUtil.getVariables(p))); });
      // should only happen the first time or we have unconnected blocks
      if (patternIdx < 0)
        patternIdx = 0;
      var pattern = patterns.splice(patternIdx, 1)[0];
      var patternVars = ClusteringUtil.getVariables(pattern);
      var sortChange = !_.contains(patternVars, sortVar);
      if (sortChange) {
        // change sortVar to an optimal choice
        sortVar = _.first(_.intersection(patternVars, vars));
        // this should only happen the first time
        if (!sortVar) {
          var nextPattern = _.findIndex(patterns, function (p) { return !_.isEmpty(_.intersection(patternVars, ClusteringUtil.getVariables(p))); });
          if (nextPattern)
            sortVar = _.first(_.intersection(patternVars, ClusteringUtil.getVariables(nextPattern)));
        }
      }
      vars = _.union(vars, patternVars);

      var sortPos = _.first(_.filter(['subject', 'predicate', 'object'], function (pos) { return pattern[pos] === sortVar; }));
      var sortObj = _.object([[sortPos, 1]]);

      var query = self.db.find(_.omit(pattern, function (val) { return rdf.isVariable(val); }));
      if (sortVar) {
        query = query.sort(sortObj);
//        if (sortChange)
//          bindings = _.sortBy(bindings, sortVar);
      }
      query.exec(function (err, docs) {
        bindings = NeDBStoreInterface.mergeSorted(sortVar, bindings, sortPos, pattern, docs);
        if (_.isEmpty(bindings))
          patterns = []; // no need to continue if we are out of matches
        _iterate();
      });
    }
  }
};

// TODO: problem if there are multiple identical bindings to one var, need to sort over multiple vars
NeDBStoreInterface.mergeSorted = function (sortVar, bindings, sortPos, pattern, triples) {
  var patternVarPos = _.map(_.filter(['subject', 'predicate', 'object'], function (pos) { return rdf.isVariable(pattern[pos]); }), function (pos) {
    return {p:pos, v:pattern[pos]};
  });
  if (bindings === null) {
    return _.map(triples, function (triple) {
      return _.object(_.pluck(patternVarPos, 'v'), _.map(patternVarPos, function (varPos) { return triple[varPos.p]; }));
    });
  }
  // TODO: this sort of negates the sorting...
  var groupedBindings = _.groupBy(bindings, sortVar);
  if (_.isEmpty(bindings) || _.isEmpty(triples))
    return [];
  var results = [];
  _.each(triples, function (triple) {
    var matchedBindings = groupedBindings[triple[sortPos]] || [];
    _.each(matchedBindings, function (binding) {
      var valid = _.every(patternVarPos, function (varPos) { return !_.has(binding, varPos.v) || binding[varPos.v] === triple[varPos.p]; } );
      if (valid) {
        var result = _.clone(binding);
        _.each(patternVarPos, function (varPos) { result[varPos.v] = triple[varPos.p]; });
        results.push(result);
      }
    });
  });
  return results;

//  var results = [];
//  var bindingIdx = 0;
//  var tripleIdx = 0;
//  while (bindingIdx < _.size(bindings) && tripleIdx < _.size(triples)) {
//    var binding = bindings[bindingIdx];
//    var triple = triples[tripleIdx];
//    if (binding[sortVar] === triple[sortPos]) {
//      var valid = _.every(patternVarPos, function (varPos) { return !_.has(binding, varPos.v) || binding[varPos.v] === triple[varPos.p]; } );
//      if (valid) {
//        var result = _.clone(binding);
//        _.each(patternVarPos, function (varPos) { result[varPos.v] = triple[varPos.p]; });
//        results.push(result);
//      }
//    }
//    if (binding[sortVar] <= triple[sortPos])
//      bindingIdx++;
//    if (triple[sortPos] <= binding[sortVar])
//      tripleIdx++;
//  }
//  return results;
};

module.exports = NeDBStoreInterface;