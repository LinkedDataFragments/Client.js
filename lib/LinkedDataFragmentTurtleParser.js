/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A LinkedDataFragmentTurtleParser parses a Turtle response from a Linked Data Server */

var N3Parser = require('n3').Parser,
    N3Util = require('n3').Util,
    url = require('url'),
    q = require('q');

var countPredicate = 'http://rdfs.org/ns/void#triples';

// Creates a new LinkedDataFragmentTurtleParser
function LinkedDataFragmentTurtleParser() { }

LinkedDataFragmentTurtleParser.prototype = {
  // Parses the specified Linked Data Fragment
  parse: function (document, documentUrl) {
    var triples = [], count = 0, triplesDeferred = q.defer();
    new N3Parser({ documentURI: documentUrl }).parse(document, function (error, triple) {
      if (error)
        return triplesDeferred.reject(new Error(error));
      if (!triple)
        return triplesDeferred.resolve(triples);
      if (!count && triple.subject === documentUrl && triple.predicate === countPredicate)
        count = parseInt(N3Util.getLiteralValue(triple.object), 10);
      triples.push(triple);
    });

    // return a promise to the parsed fragment
    return triplesDeferred.promise.then(function () {
      return {
        // Gets the URL to the results for the given triple pattern
        // TODO: this should be derived from the representation rather than constructed from the URL
        getQueryLink: function (subject, predicate, object) {
          var link = url.parse(documentUrl);
          delete link.search;
          link.query = { subject: subject, predicate: predicate, object: object };
          return url.format(link);
        },
        triples: triples,
        matchCount: count || triples.length,
      };
    });
  },
};


module.exports = LinkedDataFragmentTurtleParser;
