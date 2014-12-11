///**
// * Created by joachimvh on 27/08/2014.
// */
//
//
//var rdf = require('../util/RdfUtil'),
//    _ = require('lodash'),
//    rdfstore = require('rdfstore'),
//    N3 = require('N3'),
//    Logger = require ('../util/Logger'),
//    ClusteringUtil = require('./ClusteringUtil');
//
//var MAX_BGP_SIZE = 1000;
//var GRAPHNAME = "http://example.org/clustering";
//
//function RDFStoreInterface () {
//  this.store = rdfstore.create();
//  this.logger = new Logger("RDFStore");
//  this.logger.disable();
//  this.DEBUGtotal = 0;
//  this.DEBUGtime = 0;
//}
//
//RDFStoreInterface.prototype.addTriples = function (triples, callback) {
//  if (_.isEmpty(triples)) {
//    callback();
//    return;
//  }
//  var self = this;
//  var delayedTimer =_.after(Math.ceil(triples.length/MAX_BGP_SIZE), function () { callback(); });
//  _.each(_.groupBy(triples, function (triple, idx) { return Math.floor(idx/MAX_BGP_SIZE); }), function (tripleGroup) {
//    var queryBody = self._triplesToBGP(tripleGroup);
//    var query = "INSERT DATA { GRAPH <" + GRAPHNAME + "> { " + queryBody + "} }";
//    //console.time("DB ADD");
//    //self.logger.info(query);
//    self.store.execute(query, function (success, results) {
//      //console.timeEnd("DB ADD");
//      self.DEBUGtotal += _.size(triples);
//      self.logger.info("Inserted " + _.size(triples) + " triples (" + self.DEBUGtotal + ")");
//      delayedTimer();
//    });
//  });
//};
//
//// TODO: parameters for distinct? parameter for required vars?
//RDFStoreInterface.prototype.matchBindings = function (patterns, callback) {
//  if (_.isEmpty(patterns)) {
//    callback([]);
//    return;
//  }
//  var self = this;
//  var query = this.patternsToSelectQuery(patterns);
//  //this.logger.info(query);
//  var DEBUGdate = new Date();
//  this.store.execute(query, function (success, results) {
//    self.DEBUGtime += new Date() - DEBUGdate;
//    // map these results to the standard format used everywhere else
//    callback(self._RDFStoreResultsToLocal(results));
//  });
//};
//
//RDFStoreInterface.prototype.patternsToSelectQuery = function (patterns) {
//  var queryBody = this._triplesToBGP(patterns);
//  var query = "SELECT * FROM <" + GRAPHNAME + "> WHERE { " + queryBody + " }";
//  return query;
//};
//
//RDFStoreInterface.prototype._triplesToBGP = function (triples) {
//  return _.map(triples, function (triple) {
//    return _.map(["subject", "predicate", "object"], function (pos) {
//      var dataIdx = triple[pos].indexOf('^^');
//      if (dataIdx >= 0)
//        triple[pos] = triple[pos].substring(0, dataIdx) + '^^<' + triple[pos].substring(dataIdx+2) + ">";
//      return N3.Util.isUri(triple[pos]) && !rdf.isVariable(triple[pos]) ? "<" + triple[pos] + ">" : triple[pos];
//    }).join(" ");
//  }).join(" . ");
//};
//
//RDFStoreInterface.prototype._patternsToVariables = function (patterns) {
//  return _.reduce(patterns,
//    function (memo, val) {
//      var vars = ClusteringUtil.getVariables(val);
//      vars = _.uniq(_.filter(vars, function (v) { return !_.contains(memo, v); }));
//      memo.push.apply(memo, vars); // concat memo and vars
//      return memo;
//    },
//    []
//  );
//};
//
//// convert RDFStore.js query results to local binding map
//RDFStoreInterface.prototype._RDFStoreResultsToLocal = function (results) {
//  return _.map(results, function (result) {
//    return _.object(_.map(result, function (val, key) {
//      return ['?' + key, val.value];
//    }));
//  });
//};
//
//module.exports = RDFStoreInterface;