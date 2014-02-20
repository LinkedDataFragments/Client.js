/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SparqlQueryParser parses SPARQL queries */

var N3Parser = require('n3').Parser,
    q = require('q');

// Query templates
var prefixesTemplate = /^\s*(?:(?:PREFIX\s+[\-a-z0-9]*:\s*|BASE\s+)<[^>]*>\s+)*/i;
var selectTemplate = /^\s*SELECT\s*(\?[a-z0-9]+(?:\s*,?\s*\?[a-z0-9]+)*|\*)\s+WHERE\s*\{\s*([^\}]+)\s+?\}\s*(?:LIMIT\s*(\d+))?\s*$/i;
var constructTemplate = /^\s*CONSTRUCT\s*\{\s*([^\}]+)\s+?\}\s*WHERE\s*\{\s*([^\}]+)\s+?\}\s*(?:LIMIT\s*(\d+))?\s*$/i;

// Creates a new SparqlQueryParser
function SparqlQueryParser(defaultPrefixes) {
  this._defaultPrefixes = '';
  for (var prefix in defaultPrefixes)
    this._defaultPrefixes += 'PREFIX ' + prefix + ': <' + defaultPrefixes[prefix] + '> ';
}

SparqlQueryParser.prototype = {
  // Parses the components of a SELECT query into a promise for a query object
  _parseSelect: function (variables, whereClause, limit) {
    var self = this;
    return this._parsePattern(whereClause).then(function (wherePattern) {
      return {
        type: 'SELECT',
        variables: variables.map(replaceVariables),
        wherePattern: wherePattern,
        limit: limit ? parseInt(limit, 10) : null,
      };
    });
  },

  // Parses the components of a CONSTRUCT query into a promise for a query object
  _parseConstruct: function (constructPattern, whereClause, limit) {
    var self = this;
    return q.all([this._parsePattern(constructPattern), this._parsePattern(whereClause)])
            .spread(function (constructPattern, wherePattern) {
      return {
        type: 'CONSTRUCT',
        constructPattern: constructPattern,
        wherePattern: wherePattern,
        limit: limit ? parseInt(limit, 10) : null,
      };
    });
  },

  // Parses the given SPARQL triple pattern into a promise for triples
  _parsePattern: function (pattern) {
    // SPARQL patterns don't have to end with a dot; Turtle patterns do
    if (!/\.\s*$/.test(pattern))
      pattern += '.';
    // variables are currently not supported by the parser; replace them
    pattern = replaceVariables(pattern);

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

// Replace variables in the string by variable URIs
function replaceVariables(string) {
  return string.replace(/\?([a-z0-9]+)/ig, '<urn:var#$1>');
}

module.exports = SparqlQueryParser;
