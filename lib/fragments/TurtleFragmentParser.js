/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TurtleFragmentParser parses basic Linked Data Fragment in Turtle. */

var TransformIterator = require('../iterators/Iterator').TransformIterator,
    TriplesIterator = require('../iterators/TriplesIterator'),
    rdf = require('../util/RdfUtil'),
    UriTemplate = require('uritemplate'),
    _ = require('lodash'),
    assert = require('assert');

// Creates a new TurtleFragmentParser
function TurtleFragmentParser(source, documentUrl) {
  if (!(this instanceof TurtleFragmentParser))
    return new TurtleFragmentParser(source, documentUrl);
  TransformIterator.call(this, new TriplesIterator(source, { documentURI: documentUrl }));

  this._documentUrl = documentUrl;
  this._controlTriples = {};
  // When a metadata or controls listener is added, drain the source to read it
  this.on('newListener', function metadataListenerAdded(event) {
    if (event === 'metadataSet' || event === 'controlsSet') {
      this.removeListener('newListener', metadataListenerAdded);
      this._bufferAll();
    }
  });
}
TransformIterator.inherits(TurtleFragmentParser);

// Processes a triple from the Turtle parser
TurtleFragmentParser.prototype._transform = function (triple, push, done) {
  push(triple), done();
  // Inspect metadata triples
  if (!this.hasProperty('metadata')) {
    // Parse total triple count
    if (triple.predicate === rdf.VOID_TRIPLES &&
        rdf.decodedURIEquals(this._documentUrl, triple.subject)) {
      var totalTriples = parseInt(rdf.getLiteralValue(triple.object), 10);
      this.setProperty('metadata', { totalTriples: totalTriples });
    }
  }
  // Save control triples for control parsing
  if (triple.predicate.indexOf(rdf.HYDRA) === 0) {
    var key = triple.predicate.substr(rdf.HYDRA.length),
        controlType = this._controlTriples[key] || (this._controlTriples[key] = []);
    controlType.push(triple);
  }
};

// Sets the controls and ends
TurtleFragmentParser.prototype._flush = function (push) {
  this.setProperty('controls', this.parseControls(this._controlTriples));
  delete this._controlTriples;
  this._end();
};

// Parses the hypermedia controls inside the representation
TurtleFragmentParser.prototype.parseControls = function (triples) {
  var controls = {}, searchForms = triples.search;
  // TODO: Make parser independent of a specifically structured control set
  if (searchForms) {
    // Parse the search form
    assert(searchForms.length === 1, 'Expected 1 hydra:search');
    var searchForm = searchForms[0].object,
        searchTemplates = _.where(triples.template || [], { subject: searchForm });

    // Parse the template
    assert(searchTemplates.length === 1, 'Expected 1 hydra:template for ' + searchForm);
    var searchTemplateValue = rdf.getLiteralValue(searchTemplates[0].object),
        searchTemplate = UriTemplate.parse(searchTemplateValue);

    // Parse the template mappings
    var mappings = _.where(triples.mapping || [], { subject: searchForm });
    assert(mappings.length === 3, 'Expected 3 hydra:mappings for ' + searchForm);
    mappings = mappings.reduce(function (mappings, mapping) {
      var varTriple  = _.find(triples.variable || [], { subject: mapping.object }),
          propTriple = _.find(triples.property || [], { subject: mapping.object });
      assert(varTriple,  'Expected a hydra:variable for ' + mapping.object);
      assert(propTriple, 'Expected a hydra:property for ' + mapping.object);
      mappings[propTriple.object] = rdf.getLiteralValue(varTriple.object);
      return mappings;
    }, {});

    // Gets the URL of the basic Linked Data Fragment with the given triple pattern
    controls.getFragmentUrl = function (triplePattern) {
      var variables = {};
      variables[mappings[rdf.RDF + 'subject']]   = triplePattern.subject;
      variables[mappings[rdf.RDF + 'predicate']] = triplePattern.predicate;
      variables[mappings[rdf.RDF + 'object']]    = triplePattern.object;
      return searchTemplate.expand(variables);
    };
  }
  return controls;
};

// Indicates whether the class supports the content type
TurtleFragmentParser.supportsContentType = function (contentType) {
  return /^(?:text\/turtle|text\/n3|application\/n-triples)$/.test(contentType);
};

module.exports = TurtleFragmentParser;
