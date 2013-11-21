/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/**
 * A RDFaParser extracts RDFa data from a document.
 * The current implementation relies on green-turtle loaded through jsdom,
 * which can be slow and is difficult to compile on Windows.
 * A faster RDFa implementation should be used in the future.
 */

var path = require('path'),
    fs = require('fs'),
    q = require('q'),
    jsdom; // expensive, load lazily

// The green-turtle RDFa script source code
var RDFaJS = fs.readFileSync(path.resolve(__dirname, '../external/RDFa.js'));

// Common URIs and namespaces
var rdfObject = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#object',
    rdfPlainLiteral = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral',
    xmlNamespace = 'http://www.w3.org/XML/1998/namespace';


// Creates a new RDFaParser
function RDFaParser() {}

RDFaParser.prototype = {
  // Parses RDFa in the given HTML document, returning a promise to triples
  parse: function (html, baseURI) {
    var deferred = q.defer();
    // load the HTML document in jsdom
    if (!jsdom)
      jsdom = require('jsdom');
    jsdom.env(html, function (error, window) {
      if (error)
        return deferred.reject(error);

      // fixes for green-turtle on jsdom:
      // - set baseURI property on all elements
      // - expose all lang attributes also as xml:lang attributes
      var document = window.document,
          elements = document.getElementsByTagName('*');
      for (var i = 0; i < elements.length; i++) {
        var element = elements[i];
        element.baseURI = baseURI;
        if (element.lang) {
          var langAttribute = document.createAttributeNS(xmlNamespace, 'lang');
          langAttribute.value = element.lang;
          element.setAttributeNodeNS(langAttribute);
        }
      }

      // add jsdom script
      var script = document.createElement('script');
      script.text = RDFaJS;
      document.implementation.addFeature('ProcessExternalResources', ['script']);
      document.body.appendChild(script);

      // extract triples
      var subjects = document.data.graph.subjects, triples = [];
      for (var subjectURI in subjects) {
        var subject = subjects[subjectURI], predicates = subject.predicates;
        for (var predicateURI in predicates) {
          var predicate = predicates[predicateURI], objects = predicate.objects;
          for (i = 0; i < objects.length; i++) {
            var object = objects[i];
            triples.push({
              subject: subjectURI,
              predicate: predicateURI,
              object: object.type === rdfObject ? object.value : createLiteralString(object)
            });
          }
        }
      }
      deferred.resolve(triples);
    });
    return deferred.promise;
  }
};

// Transforms the green-turtle literal into a node-n3 literal string
function createLiteralString(literal) {
  if (literal.type && literal.type !== rdfPlainLiteral)
    return '"' + literal.value + '"^^<' + literal.type + '>';
  else if (literal.language)
    return '"' + literal.value + '"@' + literal.language;
  else
    return '"' + literal.value + '"';
}

module.exports = RDFaParser;
