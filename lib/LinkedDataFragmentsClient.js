/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A LinkedDataFragmentsClient queries a Linked Data Fragment Server */

var SparqlParser = require('./SparqlParser'),
    LinkedDataFragmentHtmlParser = require('./LinkedDataFragmentHtmlParser'),
    LinkedDataFragmentTurtleParser = require('./LinkedDataFragmentTurtleParser'),
    _ = require('underscore'),
    q = require('q'),
    request = require('request'),
    log = new (require('./Logger'))('LinkedDataFragmentsClient');

// Creates a new LinkedDataFragmentsClient
function LinkedDataFragmentsClient(datasource) {
  var self = this;
  this._datasource = datasource;
  this._sparqlParser = new SparqlParser();
  this._htmlParser = new LinkedDataFragmentHtmlParser();
  this._turtleParser = new LinkedDataFragmentTurtleParser();
  this._startFragment = performRequest(datasource).then(function (startResource) {
    var parser = startResource.type === 'text/turtle' ? self._turtleParser : self._htmlParser;
    return parser.parse(startResource.body, startResource.url);
  });
}

LinkedDataFragmentsClient.prototype = {
  // Retrieved triples and fragments
  _triples: [],
  _fragments: {},

  // Add the specified triples to the ground truth of retrieved triples
  _addTriples: function (newTriples) {
    Array.prototype.push.apply(this._triples, _.without(newTriples, this._triples));
  },

  // Gets the fragment associated with the triple pattern
  _getFragment: function (triplePattern) {
    var self = this, subject = null, predicate = null, object = null, tripleTemplate = {};
    if (!isVariable(triplePattern.subject))
      tripleTemplate.subject = subject = triplePattern.subject;
    if (!isVariable(triplePattern.predicate))
      tripleTemplate.predicate = predicate = triplePattern.predicate;
    if (!isVariable(triplePattern.object))
      tripleTemplate.object = object = triplePattern.object;

    // check the fragment cache
    var key = subject + ':' + predicate + ':' + object;
    if (key in this._fragments) {
      var fragment = this._fragments[key];
      log.info('Already retrieved the fragment:', fragment.triplePattern);
      return q.resolve(fragment);
    }

    // if looking for a specific triple (no variables), check the triple cache
    if (subject && predicate && object && _.findWhere(this._triples, tripleTemplate)) {
      log.info('Already retrieved the triple:', triplePattern);
      return q.resolve({ triples: [triplePattern], matchCount: 1, });
    }

    // retrieve the fragment from the server
    return this._startFragment.then(function (fragment) {
      log.info('Requesting fragment:', triplePattern);
      var url = fragment.getQueryLink(encodeEntity(subject), encodeEntity(predicate), encodeEntity(object));
      return performRequest(url);
    })
    // parse the fragment
    .then(function (fragment) {
      var parser = fragment.type === 'text/turtle' ? self._turtleParser : self._htmlParser;
      return parser.parse(fragment.body, fragment.url);
    },
    // if an error occurs, act as if no triples where found
    function (error) {
      return { triples: [], matchCount: 0 };
    })
    // add metadata and collect the triples
    .then(function (fragment) {
      log.info('Fragment', triplePattern, 'has', fragment.matchCount, 'matches');
      // only consider those triples that match the pattern (the fragment may contain more data)
      fragment.triplePattern = triplePattern;
      fragment.triples = _.where(fragment.triples, tripleTemplate);
      // cache fragment and triples
      self._fragments[key] = fragment;
      self._addTriples(fragment.triples);
      return fragment;
    });
  },

  // Finds interconnected subpatterns within the possibly disconnected graph pattern
  _findSubPatterns: function (graphPattern) {
    // create initial subpatterns of single triples
    var subPatterns = graphPattern.map(function (triple) {
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

  // Gets a promise for the possible bindings of the given subpattern (= connected graph pattern)
  _getSubPatternOptions: function (subPattern) {
    var self = this,
        fragments = q.all(subPattern.map(this._getFragment, this));
    return fragments.then(function (fragments) {
      var triples = _.pluck(fragments, 'triples'),
          tripleCounts = _.pluck(triples, 'length');

      // if one of the subpattern triples has no result, no consistent bindings exist
      if (_.contains(tripleCounts, 0))
        return [];

      // if all subpattern triples have one match, there is exactly one result
      if (_.max(tripleCounts) === 1) {
        var bindings = subPattern.reduce(function (bindings, varTriple, index) {
          var boundTriple = fragments[index].triples[0];
          return addBindings(bindings, varTriple, boundTriple);
        }, {});
        // if no binding is necessary, "null" signals that no option must be chosen
        return _.isEmpty(bindings) ? null : [{ bindings: bindings }];
      }

      // all subpattern triples have multiple matches; follow the one with the least matches
      var bestFragment = _.min(fragments, function (f) { return f.matchCount; }),
          varTriple = bestFragment.triplePattern,
           // get the options for the best subpattern
          subOptions = q.all(bestFragment.triples.map(function (boundTriple) {
            var bindings = addBindings({}, varTriple, boundTriple),
                boundPattern = applyBindings(bindings, subPattern);
            return self._getPatternOptions(boundPattern)
            // add the bindings to the options
            .then(function (options) {
              // if no option must be chosen, just return the bindings
              if (options === null)
                return { bindings: bindings };
              // if no valid option exists, signal a dead end with "false"
              return options.length === 0 ? false : { bindings: bindings, options: options };
            });
          }));
      // remove dead ends for which no options exist
      return subOptions.then(_.compact);
    });
  },

  // Gets a promise for the possible bindings of the given (possibly disconnected) graph pattern
  _getPatternOptions: function (graphPattern) {
    // get possible bindings for each subpattern
    var subPatterns = this._findSubPatterns(graphPattern),
        optionsPerSubPattern = q.all(subPatterns.map(this._getSubPatternOptions, this));

    // combine the options of the different subpatterns
    return optionsPerSubPattern.then(function (optionsPerSubPattern) {
      // only process those options that leave choices
      optionsPerSubPattern = _.compact(optionsPerSubPattern);

      // if no choices need to be made at all, signal this with "null"
      if (optionsPerSubPattern.length === 0)
        return null;

      // if one subpattern has no valid options, the combination has no options either
      if (optionsPerSubPattern.some(_.isEmpty))
        return [];

      // combine the options, sorting by length to minimize the needed combinations
      return _.sortBy(optionsPerSubPattern, 'length').reduce(
        function (combinedOptions, options) {
          // add options to all child options
          combinedOptions.forEach(function addOptions(child) {
            // if the child doesn't have any options yet, add them
            if (!child.options)
              return child.options = options;
            // otherwise, add the options to the child's options
            child.options.forEach(addOptions);
          });
          return combinedOptions;
        });
    });
  },

  // Gets a promise for the results of the given SPARQL query
  getQueryResults: function (sparql) {
    var self = this;
    log.info('Received query:', sparql);
    return this._sparqlParser.parse(sparql).then(function (query) {
      return self._getPatternOptions(query.wherePattern);
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

// Add the bindings of the first triple (with variables) to the second (without variables)
function addBindings(bindings, varTriple, boundTriple) {
  addBinding(bindings, varTriple.subject,   boundTriple.subject);
  addBinding(bindings, varTriple.predicate, boundTriple.predicate);
  addBinding(bindings, varTriple.object,    boundTriple.object);
  return bindings;
}

// Adds a binding from the left entity (possibly variable) to the second (not a variable)
function addBinding(bindings, left, right) {
  if (isVariable(left)) {
    if (!(left in bindings))
      bindings[left] = right;
    else if (right !== bindings[left])
      throw new Error(['cannot bind', left, 'to', right,
                       'because it was already bound to', bindings[left]].join(' '));
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
function performRequest(url) {
  // execute if possible, queue otherwise
  if (performRequest.pending < performRequest.maxParallel)
    execute();
  else
    performRequest.queue.push(execute);

  var deferred = q.defer();
  function execute() {
    var headers = { 'Accept': 'text/turtle;q=1.0,text/html;q=0.5' };
    performRequest.pending++;
    request({ url: url, headers: headers }, function (error, response, body) {
      performRequest.pending--;
      if (error)
        return deferred.reject(new Error(error));
      if (response.statusCode !== 200)
        return deferred.reject(new Error('Request failed: ' + url));
      var contentType = /^[^;]+/.exec(response.headers['content-type'] || 'text/html')[0];
      deferred.resolve({ url: url, type: contentType, body: body });

      // if a pending call exists, execute it
      var next = performRequest.queue.shift();
      if (next) next();
    });
  }
  return deferred.promise;
}
// Only execute this many requests in parallel
performRequest.maxParallel = 5;
// The number of currently pending requests
performRequest.pending = 0;
// Queue of request execution functions
performRequest.queue = [];


// Encode a URI, literal, or blank node for querying
function encodeEntity(entity) {
  return entity ? (/^"/.test(entity) ? entity : '<' + entity + '>') : '';
}

module.exports = LinkedDataFragmentsClient;
