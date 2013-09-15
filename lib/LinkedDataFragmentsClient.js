// A LinkedDataFragmentsClient queries a Linked Data Fragment Server

var SparqlParser = require('./SparqlParser'),
    LinkedDataFragmentParser = require('./LinkedDataFragmentParser'),
    _ = require('underscore'),
    q = require('q'),
    request = require('request'),
    log = new (require('./Logger'))('LinkedDataFragmentsClient');

// Creates a new LinkedDataFragmentsClient
function LinkedDataFragmentsClient(datasource) {
  var self = this;
  this._datasource = datasource;
  this._sparqlParser = new SparqlParser();
  this._fragmentParser = new LinkedDataFragmentParser();
  this._startFragment = performRequest(datasource).then(function (startResource) {
    return self._fragmentParser.parse(startResource, datasource);
  });
}

LinkedDataFragmentsClient.prototype = {
  // Finds clusters of triples that share variables
  _findClusters: function (triples) {
    // create initial clusters of single triples
    var clusters = triples.map(function (triple) {
          return { triples: [triple], vars: getVariables(triple), };
        }),
        vars, dupVar;

    // continue clustering as long as different clusters have common variables
    do {
      // find a variable that occurs in more than one cluster
      vars = _.flatten(_.pluck(clusters, 'vars'));
      if (dupVar = _.find(vars, hasDuplicate, vars)) {
        // partition the clusters by whether they contain the common variable
        var partitions = _.groupBy(clusters, function (c) { return _.contains(c.vars, dupVar); });
        // replace the clusters with the common variable by a new cluster that combines them
        clusters = partitions[false] || [];
        clusters.push({
          triples: _.union.apply(null, _.pluck(partitions[true], 'triples')),
          vars:    _.union.apply(null, _.pluck(partitions[true], 'vars')),
        });
      }
    } while (dupVar);

    log.info('Found', clusters.length, 'clusters');
    return _.pluck(clusters, 'triples');
  },

  // Gets the fragment associated with the triple
  _getFragment: function (triple) {
    var self = this;
    // retrieve the fragment from the server
    return this._startFragment.then(function (startFragment) {
      var subject   = isVariable(triple.subject)   ? null : triple.subject,
          predicate = isVariable(triple.predicate) ? null : triple.predicate,
          object    = isVariable(triple.object)    ? null : triple.object;
      log.info('Requesting fragment:', triple);
      return performRequest(startFragment.getQueryLink(subject, predicate, object));
    })
    // parse the fragment
    .then(function (fragmentBody) {
      var fragment = self._fragmentParser.parse(fragmentBody, self._datasource);
      fragment.triple = triple;
      return fragment;
    });
  },

  // Gets a promise for the values that match the given pattern
  _getPatternResults: function (pattern) {
    var fragments = q.all(pattern.map(this._getFragment.bind(this)));
    return fragments;
  },

  // Gets a promise for the results of the given SPARQL query
  getQueryResults: function (sparql) {
    var self = this;
    log.info('Received query:', sparql);
    return this._sparqlParser.parse(sparql).then(function (query) {
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

// Returns a promise for the HTTP request's result
function performRequest(options) {
  var deferred = q.defer();
  request(options, function (error, response, body) {
    if (error)
      deferred.reject(new Error(error));
    else if (response.statusCode !== 200)
      deferred.reject(new Error(body));
    else
      deferred.resolve(body);
  });
  return deferred.promise;
}

module.exports = LinkedDataFragmentsClient;
