/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A LinkedDataFragmentHtmlParser parses an HTML response from a Linked Data Server */

var RDFaParser = require('./RDFaParser'),
    url = require('url'),
    _ = require('underscore'),
    N3Util = require('n3').Util;

var countPredicate = 'http://rdfs.org/ns/void#triples';

// Creates a new LinkedDataFragmentHtmlParser
function LinkedDataFragmentHtmlParser() {
  this._rdfaParser = new RDFaParser();
}

LinkedDataFragmentHtmlParser.prototype = {
  // Parses the specified Linked Data Fragment
  parse: function (document, documentUrl) {
    return this._rdfaParser.parse(document, documentUrl).then(function (triples) {
      // Try to find the count metadata
      var countTriple = _.findWhere(triples, { subject: documentUrl, predicate: countPredicate }),
          countLiteral = countTriple && countTriple.object;

      // Return fragment object
      return {
        // Gets the URL to the results for the given triple pattern
        // TODO: this should be derived from the HTML rather than constructed from the URL
        getQueryLink: function (subject, predicate, object) {
          var link = url.parse(documentUrl);
          delete link.search;
          link.query = { subject: subject, predicate: predicate, object: object };
          return url.format(link);
        },

        // All extracted triples (fragment and metadata)
        triples: triples,

        // The total number of matches for the pattern
        matchCount: countLiteral && parseInt(N3Util.getLiteralValue(countLiteral), 10),
      };
    });
  },
};

module.exports = LinkedDataFragmentHtmlParser;
