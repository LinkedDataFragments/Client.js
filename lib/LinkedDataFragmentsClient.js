// A LinkedDataFragmentsClient queries a Linked Data Fragment Server

var SparqlParser = require('./SparqlParser'),
    _ = require('underscore'),
    q = require('q');

// Creates a new LinkedDataFragmentsClient
function LinkedDataFragmentsClient(datasource) {
  this._datasource = datasource;
  this._parser = new SparqlParser();
}

LinkedDataFragmentsClient.prototype = {
  // Finds clusters of triples that share variables
  _findClusters: function (triples) {
    // create initial clusters of single triples
    var clusters = triples.map(function (triple) {
      return { triples: [triple], vars: getVariables(triple), };
    });

    // continue clustering as long as different clusters have common variables
    while (true) {
      // find a variable that occurs in more than one cluster
      var vars = _.flatten(_.pluck(clusters, 'vars')),
          dupVar = _.find(vars, hasDuplicate, vars);
      if (!dupVar)
        return _.pluck(clusters, 'triples');

      // partition the clusters by whether they contain the common variable
      var partitions = _.groupBy(clusters, function (c) { return _.contains(c.vars, dupVar); });
      // replace the clusters with the common variable by a new cluster that combines them
      clusters = partitions[false] || [];
      clusters.push({
        triples: _.union.apply(null, _.pluck(partitions[true], 'triples')),
        vars:    _.union.apply(null, _.pluck(partitions[true], 'vars')),
      });
    }
  },

  // Gets a promise for the values that match the given pattern
  _getPatternResults: function (pattern) {
    return q.resolve(pattern.map(function (triple) {
      return 'getting results for ' + triple.subject + ' ' + triple.predicate + ' ' + triple.object;
    }));
  },

  // Gets a promise for the results of the given SPARQL query
  getQueryResults: function (sparql) {
    var self = this;
    return this._parser.parse(sparql).then(function (query) {
      var clusters = self._findClusters(query.wherePattern);
      return q.all(clusters.map(self._getPatternResults, self));
    });
  },
};

// Indicates whether the URI represents a variable
function isVariable(uri) {
  return (/^var#[a-z]+$/i).test(uri);
}

// Gets all variables in the triple
function getVariables(triple) {
  return _.values(triple).filter(isVariable);
}

// Array filter that indicates whether the value has duplicates
function hasDuplicate(value, index) {
  return index !== this.lastIndexOf(value);
}

module.exports = LinkedDataFragmentsClient;
