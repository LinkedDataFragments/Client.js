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

function N3StoreInterface () {
  this.store = N3.Store();
}

N3StoreInterface.prototype.addTriples = function (triples, callback) {
  this.store.addTriples(triples);
  callback();
};

N3StoreInterface.prototype.matchBindings = function (patterns, callback) {
  var it = new ReorderingGraphPatternIterator(Iterator.single({}), patterns, {fragmentsClient: new N3FragmentsClientWrapper(this.store)});
//  var parameters = _.map(['subject', 'predicate', 'object'], function (pos) { return rdf.isVariable(patterns[0][pos]) ? null : patterns[0][pos]; });
//  var data = this.store.find.apply(this.store, parameters);
//  var it = Iterator.ArrayIterator(data);
  it.toArray(function (error, data) {
    callback(data);
  });

//  var pattern = patterns[0];
//  var parameters = _.map(['subject', 'predicate', 'object'], function (pos) { return rdf.isVariable(pattern[pos]) ? null : pattern[pos]; });
//  var data = this.store.find.apply(this.store, parameters);
//  callback(data);
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