/**
 * Created by joachimvh on 27/08/2014.
 */


var rdf = require('../util/RdfUtil'),
    _ = require('lodash'),
    rdfstore = require('rdfstore'),
    N3 = require('N3'),
  Logger = require ('../util/Logger'),
    ClusteringUtil = require('./ClusteringUtil');

var MAX_BGP_SIZE = 1000;
var GRAPHNAME = "http://example.org/clustering";

function RDFStoreInterface () {
  this.store = rdfstore.create();
  this.logger = new Logger("RDFStore");
  this.logger.disable();
}

RDFStoreInterface.prototype.addTriples = function (triples, callback) {
  if (_.isEmpty(triples)) {
    callback();
    return;
  }
  var self = this;
  var delayedTimer =_.after(Math.ceil(triples.length/MAX_BGP_SIZE), function () { callback(); });
  _.each(_.groupBy(triples, function (triple, idx) { return Math.floor(idx/MAX_BGP_SIZE); }), function (tripleGroup) {
    var queryBody = self._triplesToBGP(tripleGroup);
    var query = "INSERT DATA { GRAPH <" + GRAPHNAME + "> { " + queryBody + "} }";
    //console.time("DB ADD");
    self.store.execute(query, function (success, results) {
      //console.timeEnd("DB ADD");
      self.logger.info("Inserted " + _.size(triples) + " triples");
      delayedTimer();
    });
  });
};

// TODO: parameters for distinct? parameter for required vars?
RDFStoreInterface.prototype.matchBindings = function (patterns, callback) {
  if (_.isEmpty(patterns)) {
    callback([]);
    return;
  }
  var self = this;
  var queryBody = this._triplesToBGP(patterns);
  var query = "SELECT " + this._patternsToVariables(patterns).join(' ') + " FROM <" + GRAPHNAME + "> WHERE { " + queryBody + " }";
  this.logger.info(query);
  //console.time("DB MATCH");
  this.store.execute(query, function (success, results) {
    //console.timeEnd("DB MATCH");
    // map these results to the standard format used everywhere else
    callback(self._RDFStoreResultsToLocal(results));
  });
};

RDFStoreInterface.prototype._triplesToBGP = function (triples) {
  return _.map(triples, function (triple) {
    return _.map(["subject", "predicate", "object"], function (pos) {
      return N3.Util.isUri(triple[pos]) && !rdf.isVariable(triple[pos]) ? "<" + triple[pos] + ">" : triple[pos];
    }).join(" ");
  }).join(" . ");
};

RDFStoreInterface.prototype._patternsToVariables = function (patterns) {
  return _.reduce(patterns,
    function (memo, val) {
      var vars = ClusteringUtil.getVariables(val);
      vars = _.uniq(_.filter(vars, function (v) { return !_.contains(memo, v); }));
      memo.push.apply(memo, vars); // concat memo and vars
      return memo;
    },
    []
  );
};

// convert RDFStore.js query results to local binding map
RDFStoreInterface.prototype._RDFStoreResultsToLocal = function (results) {
  return _.map(results, function (result) {
    return _.object(_.map(result, function (val, key) {
      return ['?' + key, val.value];
    }));
  });
};

module.exports = RDFStoreInterface;