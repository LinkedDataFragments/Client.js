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

  // Finds interconnected subpatterns within the possibly disconnected pattern
  _findSubPatterns: function (pattern) {
    // create initial subpatterns of single triples
    var subPatterns = pattern.map(function (triple) {
          return { triples: [triple], variables: getVariables(triple), };
        }),
        variables, dupVar;

    // continue clustering as long as different subpatterns have common variables
    do {
      // find a variable that occurs in more than one subpattern
      variables = _.flatten(_.pluck(subPatterns, 'variables'));
      if (dupVar = _.find(variables, hasDuplicate, variables)) {
        // partition the subpatterns by whether they contain the common variable
        var partitions = _.groupBy(subPatterns, function (c) { return _.contains(c.variables, dupVar); });
        // replace the subpatterns with the common variable by a new subpattern that combines them
        subPatterns = partitions[false] || [];
        subPatterns.push({
          triples:   _.union.apply(_, _.pluck(partitions[true], 'triples')),
          variables: _.union.apply(_, _.pluck(partitions[true], 'variables')),
        });
      }
    } while (dupVar);

    log.info('Found', subPatterns.length, 'subpatterns');
    return _.pluck(subPatterns, 'triples');
  },

  // Gets a promise for the values that match the given subpattern (= triples connected by variables)
  _getSubPatternResults: function (subPattern) {
    var fragments = q.all(subPattern.map(this._getFragment, this));
    return fragments.then(function (fragments) {
      var tripleCounts = _.pluck(_.pluck(fragments, 'triples'), 'length');

      // if one of the subpattern triples has no result, the entire subpattern has no results
      if (_.contains(tripleCounts, 0)) {
        return { results: [] };
      }

      // if all subpattern triples have one match, there is one result
      if (_.max(tripleCounts) === 1) {
        return { results: [] };
      }

      // all subpattern triples have multiple matches; follow the one with the least matches
      var bestFragment = _.min(fragments, function (f) { return f.matchCount; }),
          varTriple = bestFragment.triple;
      return bestFragment.triples.map(function (boundTriple) {
        var bindings = getBindings(varTriple, boundTriple),
            boundPattern = applyBindings(bindings, subPattern);
        return boundPattern;
      });
    });
  },

  // Gets a promise for the values that match the given (possibly disconnected) pattern
  _getPatternResults: function (pattern) {
    var subPatterns = this._findSubPatterns(pattern);
    return q.all(subPatterns.map(this._getSubPatternResults, this));
  },

  // Gets a promise for the results of the given SPARQL query
  getQueryResults: function (sparql) {
    var self = this;
    log.info('Received query:', sparql);
    return this._sparqlParser.parse(sparql).then(function (query) {
      var results = self._getPatternResults(query.wherePattern);
      return results;
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

// Binds the first triple (with variables) to the second (without variables), returning the bindings
function getBindings(varTriple, boundTriple) {
  var bindings = {};
  addBinding(bindings, varTriple.subject,   boundTriple.subject);
  addBinding(bindings, varTriple.predicate, boundTriple.predicate);
  addBinding(bindings, varTriple.object,    boundTriple.object);
  return bindings;
}

// Adds a binding from the left entity (possibly variable) to the second (not a variable)
function addBinding(bindings, left, right) {
  if (isVariable(left)) {
    var prevBinding = bindings[left];
    if (!prevBinding)
      bindings[left] = right;
    else if (right !== prevBinding)
      throw new Error(['cannot bind', left, 'to', right,
                       'because it was already bound to', prevBinding].join(' '));
  }
  else if (left !== right) {
    throw new Error(['cannot bind', left, 'to', right].join(' '));
  }
}

// Apply the given bindings to the triples, returning a bound copy
function applyBindings(bindings, triples) {
  return triples.map(function (triple) {
    return _.defaults({
      subject:   bindings[triple.subject],
      predicate: bindings[triple.predicate],
      object:    bindings[triple.object],
    }, triple);
  });
}

// Array filter that indicates whether the value has duplicates
function hasDuplicate(value, index) {
  return index !== this.lastIndexOf(value);
}

// Returns a promise for the HTTP request's result
function performRequest(options) {
  // execute if possible, queue otherwise
  if (performRequest.pending < performRequest.maxParallel)
    execute();
  else
    performRequest.queue.push(execute);

  var deferred = q.defer();
  function execute() {
    performRequest.pending++;
    request(options, function (error, response, body) {
      performRequest.pending--;
      if (error)
        deferred.reject(new Error(error));
      else if (response.statusCode !== 200)
        deferred.reject(new Error('Request failed: ' + (options.url || options)));
      else
        deferred.resolve(body);

      // if a pending call exists, execute it
      var next = performRequest.queue.shift();
      if (next) next();
    });
  }
  return deferred.promise;
}
// Only execute this many requests in parallel
performRequest.maxParallel = 1;
// The number of currently pending requests
performRequest.pending = 0;
// Queue of request execution functions
performRequest.queue = [];


module.exports = LinkedDataFragmentsClient;
