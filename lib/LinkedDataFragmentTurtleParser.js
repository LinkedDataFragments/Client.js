/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A LinkedDataFragmentTurtleParser parses a Turtle response from a Linked Data Server */

var url = require('url'),
    q = require('q'),
    _ = require('underscore'),
    UriTemplate = require('uritemplate'),
    N3 = require('N3'),
    N3Parser = N3.Parser, N3Store = N3.Store, N3Util = N3.Util;

var voID = 'http://rdfs.org/ns/void#',
    hydra = 'http://www.w3.org/ns/hydra/core#',
    rdf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';

// Creates a new LinkedDataFragmentTurtleParser
function LinkedDataFragmentTurtleParser() { }

LinkedDataFragmentTurtleParser.prototype = {
  // Parses the specified Linked Data Fragment
  parse: function (document, documentUrl) {
    var self = this, triples = [], count = 0, triplesDeferred = q.defer(), controlTriples = [];
    new N3Parser({ documentURI: documentUrl }).parse(document, function (error, triple) {
      if (error)   return triplesDeferred.reject(new Error(error));
      if (!triple) return triplesDeferred.resolve(triples);
      triples.push(triple);
      // Store triples related to hypermedia controls
      if (triple.predicate.indexOf(hydra) === 0)
        controlTriples.push(triple);
      // Extract triple count
      if (!count && triple.predicate === voID + 'triples' && decodedURIEquals(documentUrl, triple.subject))
        count = parseInt(N3Util.getLiteralValue(triple.object), 10);
    });
    // return a promise to the parsed fragment
    return triplesDeferred.promise.then(function () {
      return {
        triples: triples,
        matchCount: count,
        // Gets a URL to a fragment for the given triple pattern
        getUrlToFragment: function (triplePattern) {
          if (!this._searchTemplate)
            this._searchTemplate = self._extractHydraSearchTemplate(controlTriples);
          return this._searchTemplate.expand(triplePattern);
        },
      };
    });
  },

  // Extracts a Hydra search template from the given triples
  _extractHydraSearchTemplate: function (triples) {
    var store = new N3Store(triples),
        // find the triple that describes the template
        searchTriple = store.find(null, hydra + 'search', null)[0],
        templateTriple = store.find(searchTriple.object, hydra + 'template', null)[0],
        // instantiate the actual URI Template
        template = UriTemplate.parse(N3Util.getLiteralValue(templateTriple.object)),
        // find which variables names the template uses for subject, predicate, and object
        mappingTriples = store.find(templateTriple.subject, hydra + 'mappings', null),
        mappingProperties = mappingTriples.map(function (triple) {
          return store.find(triple.object, hydra + 'property', null)[0];
        }),
        variableNames = ['subject', 'predicate', 'object'].reduce(function (variableNames, component) {
          var id = _.findWhere(mappingProperties, { object: rdf + component }).subject,
              nameTriple = store.find(id, hydra + 'variable', null)[0];
          return variableNames[component] = N3Util.getLiteralValue(nameTriple.object), variableNames;
        }, {});
    // Return a URI Template that expands to the Hydra search template
    return {
      expand: function (pattern) {
        var variables = {};
        for (var variable in pattern)
          variables[variableNames[variable]] = encodeEntity(pattern[variable]);
        return template.expand(variables);
      },
    };
  },
};

// Encodes a URI, literal, or blank node for querying
function encodeEntity(entity) {
  return entity ? (/^"/.test(entity) ? entity : '<' + entity + '>') : '';
}

// Checks whether two URIs are equal after decoding, to make up for encoding differences
function decodedURIEquals(URIa, URIb) {
  if (URIa === URIb) return true;
  try { return decodeURI(URIa) === decodeURI(URIb); }
  catch (error) { return false; }
}

module.exports = LinkedDataFragmentTurtleParser;
