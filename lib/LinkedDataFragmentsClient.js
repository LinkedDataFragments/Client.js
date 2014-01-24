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
  this._datasource = config.datasource;
  this._fetcher = new HttpFetcher(config.maxParallelRequests || 10);
  this._sparqlParser = new SparqlParser(config.prefixes);
  this._htmlParser = new LinkedDataFragmentHtmlParser();
  this._turtleParser = new LinkedDataFragmentTurtleParser();
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
      return self._fetcher.get(url);
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
      var bestFragment = bestFragments[0],
          fragmentOptions = q.all(bestFragment.triples.map(function (boundTriple) {
            var bindings = addBindings({}, bestFragment.triplePattern, boundTriple),
                boundPattern = applyBindings(bindings, subPattern);
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
    var fragments = q.all(subPattern.map(this._getFragment, this));
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
      return this._getFragment(noVarTriple).then((function (fragment) {
        // if it doesn't exist, the entire graph pattern has no solutions
        if (fragment.triples.length === 0) return [];
        // if it does exist, find options for the others
        return this._getPatternOptions(_.without(graphPattern, noVarTriple));
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
      // Initialize the start fragment if necessary
      if (!self._startFragment) {
        self._startFragment = self._fetcher.get(self._datasource).then(function (startResource) {
          var parser = startResource.type === 'text/turtle' ? self._turtleParser : self._htmlParser;
          return parser.parse(startResource.body, startResource.url);
        });
      }

      // When the start fragment is ready, get the options for the WHERE pattern
      return self._startFragment.then(self._getPatternOptions.bind(self, query.wherePattern));
    });
  },
};

// Indicates whether the URI represents a variable
function isVariable(uri) {
  return (/^urn:var#[a-z]+$/i).test(uri);
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

// Array filter that indicates whether the value has duplicates
function hasDuplicate(value, index) {
  return index !== this.lastIndexOf(value);
}

// Encode a URI, literal, or blank node for querying
function encodeEntity(entity) {
  return entity ? (/^"/.test(entity) ? entity : '<' + entity + '>') : '';
}

module.exports = LinkedDataFragmentsClient;
