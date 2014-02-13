/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A LinkedDataFragmentsClient queries a Linked Data Fragment Server */

var SparqlParser = require('./SparqlParser'),
    LinkedDataFragmentHtmlParser = require('./LinkedDataFragmentHtmlParser'),
    LinkedDataFragmentTurtleParser = require('./LinkedDataFragmentTurtleParser'),
    HttpFetcher = require('./HttpFetcher'),
    _ = require('underscore'),
    q = require('q'),
    log = new (require('./Logger'))('LinkedDataFragmentsClient'),
    assert = require('assert');

// Creates a new LinkedDataFragmentsClient
function LinkedDataFragmentsClient(config) {
  // Allow data source URL instead of full configuration object
  if (typeof config === 'string')
    config = { datasource: config };

  var self = this;
  this._triples = [];
  this._fragments = {};
  this._datasource = config.datasource;
  this._fetcher = new HttpFetcher(config.maxParallelRequests || 10);
  this._sparqlParser = new SparqlParser(config.prefixes);
  this._htmlParser = new LinkedDataFragmentHtmlParser();
  this._turtleParser = new LinkedDataFragmentTurtleParser();
}

LinkedDataFragmentsClient.prototype = {
  // Add the specified triples to the ground truth of retrieved triples
  _addTriples: function (newTriples) {
    Array.prototype.push.apply(this._triples, _.without(newTriples, this._triples));
  },

  // Fetches a fragment associated with the triple pattern
  // by following controls in the given base fragment
  _getFragment: function (baseFragment, triplePattern) {
    var self = this, method = 'GET', hasVariables,
        subject = null, predicate = null, object = null, tripleTemplate = {};
    if (!isVariable(triplePattern.subject))
      tripleTemplate.subject = subject = triplePattern.subject;
    if (!isVariable(triplePattern.predicate))
      tripleTemplate.predicate = predicate = triplePattern.predicate;
    if (!isVariable(triplePattern.object))
      tripleTemplate.object = object = triplePattern.object;
    hasVariables = !(subject && predicate && object);

    // check the fragment cache
    var key = subject + ':' + predicate + ':' + object;
    if (key in this._fragments) {
      var fragment = this._fragments[key];
      log.info('Already retrieved fragment:', fragment.triplePattern);
      return q.resolve(fragment);
    }

    // if looking for a specific triple (no variables), we can use shortcuts
    if (!hasVariables) {
      // first check the triple cache
      if (_.findWhere(this._triples, triplePattern)) {
        log.info('Already retrieved triple:', triplePattern);
        return q.resolve({ triples: [triplePattern], matchCount: 1 });
      }
      // if we have to look up, a HEAD request will be sufficient:
      // 200 means the triple exists, 404 means it doesn't.
      method = 'HEAD';
    }

    // follow the given fragment to retrieve the requested fragment
    log.info('Requesting fragment:', triplePattern);
    return q.resolve().then(function () {
      return self._fetcher.request(baseFragment.getUrlToFragment(tripleTemplate), method);
    })
    // if this doesn't work, try following the entry fragment instead
    .catch(function (error) {
      return self._entryFragment.then(function (entryFragment) {
        return self._fetcher.request(entryFragment.getUrlToFragment(tripleTemplate), method);
      });
    })
    // extract the fragment data from he response
    .then(function (fragmentResponse) {
      // if the fragment does not exist, there were no triples
      if (fragmentResponse.status >= 400)
        return { triples: [], matchCount: 0 };
      // if the fragment exists and there were no variables, the fragment has one triple
      if (!hasVariables)
        return { triples: [triplePattern], matchCount: 1 };
      // in all other case, parse the body for triples
      var parser = fragmentResponse.type === 'text/turtle' ? self._turtleParser : self._htmlParser;
      return parser.parse(fragmentResponse.body, fragmentResponse.url);
    })
    // if an error occurs, act as if no triples where found
    .catch(function (error) {
      log.info('Error retrieving/parsing fragment', triplePattern, error.stack);
      return { triples: [], matchCount: 0 };
    })
    // add metadata and collect the triples
    .then(function (fragment) {
      log.info('Fragment', triplePattern, 'has', fragment.matchCount, 'matches');
      // only consider those triples that match the pattern (the fragment may contain more data)
      if (!_.isEmpty(tripleTemplate))
        fragment.triples = _.where(fragment.triples, tripleTemplate);
      fragment.triplePattern = triplePattern;
      // cache fragment and triples
      self._fragments[key] = fragment;
      self._addTriples(fragment.triples);
      return fragment;
    });
  },

  // Finds interconnected subpatterns within the possibly disconnected graph pattern
  _findSubPatterns: function (graphPattern) {
    // initially create subpatterns as clusters of a single triple
    var clusters = graphPattern.map(function (triple) {
          return { triples: [triple], variables: getVariables(triple), };
        }), commonVar;

    // continue clustering as long as different subpatterns have common variables
    do {
      // find a variable that occurs in more than one subpattern
      var allVariables = _.flatten(_.pluck(clusters, 'variables'));
      if (commonVar = _.find(allVariables, hasDuplicate)) {
        // partition the subpatterns by whether they contain that common variable
        var partitions = _.groupBy(clusters, function (c) { return _.contains(c.variables, commonVar); });
        // replace the subpatterns having that common variable by a new subpattern that combines them
        clusters = partitions[false] || [];
        clusters.push({
          triples:   _.union.apply(_, _.pluck(partitions[true], 'triples')),
          variables: _.union.apply(_, _.pluck(partitions[true], 'variables')),
        });
      }
    } while (commonVar);

    // the subpatterns consist of the triples of each cluster
    log.info('Found', clusters.length, 'subpatterns');
    var subPatterns = _.pluck(clusters, 'triples');
    subPatterns.forEach(function (subPattern) { subPattern.fragment = graphPattern.fragment; });
    return subPatterns;
  },

  // Gets a promise for the possible bindings of the given subpattern (= connected graph pattern)
  _getSubPatternOptions: function (subPattern) {
    var self = this;
    return this._getBestFragments(subPattern).then(function (bestFragments) {
      // if at least one fragment has no result, the entire subpattern has no consistent bindings
      if (bestFragments.minCount === 0)
        return [];

      // if each of the fragments has exactly one match, and they are consistent, there's one result
      if (bestFragments.maxCount === 1) {
        assert.equal(bestFragments.length, subPattern.length);
        try {
          var bindings = subPattern.reduce(function (bindings, triplePattern, index) {
            var boundTriple = bestFragments[index].triples[0];
            return addBindings(bindings, triplePattern, boundTriple);
          }, {});
          // if no binding is necessary, `null` signals that no option must be chosen
          return _.isEmpty(bindings) ? null : [{ bindings: bindings }];
        }
        // if the matches are inconsistent, the subpattern has no consistent bindings
        catch (bindingError) { return []; }
      }

      // some patterns have multiple matches; follow the best fragment
      var fragment = bestFragments[0],
          fragmentOptions = q.all(fragment.triples.map(function (boundTriple) {
            var bindings = addBindings({}, fragment.triplePattern, boundTriple),
                boundPattern = applyBindings(bindings, subPattern);
            boundPattern.fragment = fragment;
            return self._getPatternOptions(boundPattern)
            // add the bindings to the options
            .then(function (options) {
              // if no option must be chosen, just return the bindings
              if (options === null)
                return { bindings: bindings };
              // if no valid option exists, signal a dead end with `false`
              return options.length === 0 ? false : { bindings: bindings, options: options };
            });
          }));
      // remove dead ends for which no options exist
      return fragmentOptions.then(_.compact);
    });
  },

  // Gets a promise for the fragments of the subpattern with minimal query cost
  _getBestFragments: function (subPattern) {
    var fragments = q.all(subPattern.map(this._getFragment.bind(this, subPattern.fragment)));
    return fragments.then(function (fragments) {
      var totalCounts = fragments.map(function (f) { return f.matchCount || f.triples.length; }),
          minCount = _.min(totalCounts), maxCount = _.max(totalCounts),
          bestFragments = fragments.filter(function (f, i) { return totalCounts[i] === minCount; });
      bestFragments.minCount = minCount;
      bestFragments.maxCount = maxCount;
      return bestFragments;
    });
  },

  // Gets a promise for the possible bindings of the given (possibly disconnected) graph pattern
  _getPatternOptions: function (graphPattern) {
    // check if the pattern contains a triple without variables
    var noVarTriple = _.find(graphPattern, function (t) { return !_.values(t).some(isVariable); });
    if (noVarTriple) {
      // quick dead end elimination: check whether that triple exists
      return this._getFragment(graphPattern.fragment, noVarTriple).then((function (fragment) {
        // if it doesn't exist, the entire graph pattern has no solutions
        if (fragment.triples.length === 0) return [];
        // if it does exist, find options for the others
        var remainingGraphPattern = _.without(graphPattern, noVarTriple);
        remainingGraphPattern.fragment = graphPattern.fragment;
        return this._getPatternOptions(remainingGraphPattern);
      }).bind(this));
    }

    // get possible bindings for each subpattern
    var subPatterns = this._findSubPatterns(graphPattern),
        optionsPerSubPattern = q.all(subPatterns.map(this._getSubPatternOptions, this));
    // combine the options of the different subpatterns
    return optionsPerSubPattern.then(function (optionsPerSubPattern) {
      // only process those options that leave choices
      optionsPerSubPattern = _.compact(optionsPerSubPattern);

      // if no choices need to be made at all, signal this with `null`
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

    // Parse the Query
    return this._sparqlParser.parse(sparql).then(function (query) {
      // Initialize the entry fragment if necessary
      if (!self._entryFragment) {
        var fragmentDocument = self._fetcher.repeat(self._datasource);
        self._entryFragment = fragmentDocument.then(function (entryResource) {
          var parser = entryResource.type === 'text/turtle' ? self._turtleParser : self._htmlParser;
          return parser.parse(entryResource.body, entryResource.url);
        });
      }

      // When the entry fragment is ready, get the options for the WHERE pattern
      return self._entryFragment.then(function (entryFragment) {
        var pattern = query.wherePattern;
        pattern.fragment = entryFragment;
        return self._getPatternOptions(pattern);
      });
    });
  },

  // Closes the client
  destroy: function () {
    this._fetcher && this._fetcher.cancelAll();
    delete this._fetcher;
    delete this._triples;
    delete this._fragments;
  },
};

// Indicates whether the URI represents a variable
function isVariable(uri) {
  return (/^urn:var#[a-z0-9]+$/i).test(uri);
}

// Gets all variables in the triple
function getVariables(triple) {
  return _.values(triple).filter(isVariable);
}

// Add the bindings of the first triple (with variables) to the second (without variables)
function addBindings(bindings, triplePattern, boundTriple) {
  addBinding(bindings, triplePattern.subject,   boundTriple.subject);
  addBinding(bindings, triplePattern.predicate, boundTriple.predicate);
  addBinding(bindings, triplePattern.object,    boundTriple.object);
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

// Array filter that indicates whether the given value occurs more than once
function hasDuplicate(value, index, array) {
  return index !== array.lastIndexOf(value);
}

module.exports = LinkedDataFragmentsClient;
