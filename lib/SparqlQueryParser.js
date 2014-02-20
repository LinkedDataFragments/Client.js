/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SparqlQueryParser parses SPARQL queries */

var N3Parser = require('n3').Parser,
    q = require('q'),
    _ = require('underscore');

// Query templates
var prefixesTemplate = /^\s*(?:(?:PREFIX\s+[\-a-z0-9]*:\s*|BASE\s+)<[^>]*>\s+)*/i;
var selectTemplate = /^\s*SELECT\s*(\?[a-z0-9]+(?:\s*,?\s*\?[a-z0-9]+)*|\*)\s+WHERE\s*\{\s*([^\}]+)\s+?\}\s*(?:LIMIT\s*(\d+))?\s*$/i;
var constructTemplate = /^\s*CONSTRUCT\s*\{\s*([^\}]+)\s+?\}\s*WHERE\s*\{\s*([^\}]+)\s+?\}\s*(?:LIMIT\s*(\d+))?\s*$/i;

// Prefix for variable URIs
var VARIABLE_PREFIX = 'urn:var#';

// Creates a new SparqlQueryParser
function SparqlQueryParser(defaultPrefixes) {
  this._defaultPrefixes = '';
  for (var prefix in defaultPrefixes)
    this._defaultPrefixes += 'PREFIX ' + prefix + ': <' + defaultPrefixes[prefix] + '> ';
}

SparqlQueryParser.prototype = {
  // Parses the components of a SELECT query into a promise for a query object
  _parseSelect: function (variables, whereGraph, limit) {
    var self = this;
    return this._parseGraph(whereGraph).then(function (wherePattern) {
      var variableNames = _.contains(variables, '*') ? self._findVariables(wherePattern) : variables;
      return {
        type: 'SELECT',
        variables: variableNames.map(function (v) { return v.replace(/^\?/i, VARIABLE_PREFIX); }),
        wherePattern: wherePattern,
        limit: limit ? parseInt(limit, 10) : null,
      };
    });
  },

  // Parses the components of a CONSTRUCT query into a promise for a query object
  _parseConstruct: function (constructGraph, whereGraph, limit) {
    var self = this;
    return q.all([this._parseGraph(constructGraph), this._parseGraph(whereGraph)])
            .spread(function (constructPattern, wherePattern) {
      var variableNames = self._findVariables(constructPattern);
      return {
        type: 'CONSTRUCT',
        variables: variableNames.map(function (v) { return v.replace(/^\?/i, VARIABLE_PREFIX); }),
        constructPattern: constructPattern,
        wherePattern: wherePattern,
        limit: limit ? parseInt(limit, 10) : null,
      };
    });
  },

  // Parses the given SPARQL graph pattern into a promise for triple patterns
  _parseGraph: function (pattern) {
    // SPARQL patterns don't have to end with a dot; Turtle patterns do
    if (!/\.\s*$/.test(pattern))
      pattern += '.';
    // variables are currently not supported by the parser; replace them by URIs
    pattern = pattern.replace(/\?([a-z0-9]+)/ig, '<' + VARIABLE_PREFIX + '$1>');

    var deferred = q.defer(),
        triples = [];
    new N3Parser().parse(pattern, function (error, triple) {
      if (error)
        deferred.reject(new Error(error));
      else if (triple)
        triples.push(triple);
      else
        // no triple means everything has been parsed
        deferred.resolve(triples);
    });
    return deferred.promise;
  },

  // Finds all variables in the pattern
  _findVariables: function (pattern) {
    return _.uniq(_.flatten(pattern.map(_.values))
                  .filter(function (v) { return v.indexOf(VARIABLE_PREFIX) === 0; }));
  },

  // Parses the given query into a promise for a query object
  parse: function (query) {
    var match = query.match(prefixesTemplate),
        prefixes = this._defaultPrefixes + match[0];
    query = query.substr(match[0].length);

    if (match = query.match(selectTemplate))
      return this._parseSelect(match[1].split(/\s+|\s*,\s*/), prefixes + match[2], match[3]);

    if (match = query.match(constructTemplate))
      return this._parseConstruct(match[1], prefixes + match[2], match[3]);

    return q.reject(new Error('Could not parse query: ' + query));
  },
};

SparqlQueryParser.VARIABLE_PREFIX = VARIABLE_PREFIX;

module.exports = SparqlQueryParser;
