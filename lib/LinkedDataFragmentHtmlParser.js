/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A LinkedDataFragmentHtmlParser parses an HTML response from a Linked Data Server */

var RDFaParser = require('./RDFaParser'),
    url = require('url'),
    _ = require('underscore'),
    N3 = require('n3'),
    N3Store = N3.Store, N3Util = N3.Util;

var voID = 'http://rdfs.org/ns/void#',
    hydra = 'http://www.w3.org/ns/hydra/core#',
    rdf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';

// Creates a new LinkedDataFragmentHtmlParser
function LinkedDataFragmentHtmlParser() {
  this._rdfaParser = new RDFaParser();
}

LinkedDataFragmentHtmlParser.prototype = {
  // Parses the specified Linked Data Fragment
  parse: function (document, documentUrl) {
    var self = this;
    return this._rdfaParser.parse(document, documentUrl).then(function (triples) {
      // Try to find the count metadata
      var countTriple = _.findWhere(triples, { subject: documentUrl, predicate: voID + 'triples' }),
          countLiteral = countTriple && countTriple.object;
      // Return fragment object
      return {
        triples: triples,
        matchCount: countLiteral && parseInt(N3Util.getLiteralValue(countLiteral), 10),
        // Gets a URL to a fragment for the given triple pattern
        getUrlToFragment: function (triplePattern) {
          if (!this._formTemplate)
            this._formTemplate = self._extractFormTemplate(documentUrl, triples);
          return this._formTemplate.expand(triplePattern);
        },
      };
    });
  },

  // Extracts a URI template from an HTML form
  _extractFormTemplate: function (documentUrl, triples) {
    var store = new N3Store(triples),
        template = url.parse(documentUrl),
        // find the triple that describes the form
        searchTriple = store.find(null, hydra + 'search', null)[0],
        // find which variables names the form uses for subject, predicate, and object
        mappingTriples = store.find(searchTriple.object, hydra + 'mapping', null),
        mappingProperties = mappingTriples.map(function (triple) {
          return store.find(triple.object, hydra + 'property', null)[0];
        }),
        variableNames = ['subject', 'predicate', 'object'].reduce(function (variableNames, component) {
          var id = _.findWhere(mappingProperties, { object: rdf + component }).subject,
              nameTriple = store.find(id, hydra + 'variable', null)[0];
          return variableNames[component] = N3Util.getLiteralValue(nameTriple.object), variableNames;
        }, {});
    // Return a URI Template that expands to the HTML form template
    return {
      expand: function (pattern) {
        template.query = {};
        delete template.search;
        for (var variable in pattern)
          template.query[variableNames[variable]] = encodeEntity(pattern[variable]);
        return url.format(template);
      },
    };
  },
};

// Encodes a URI, literal, or blank node for querying
function encodeEntity(entity) {
  return entity ? (/^"/.test(entity) ? entity : '<' + entity + '>') : '';
}

module.exports = LinkedDataFragmentHtmlParser;
