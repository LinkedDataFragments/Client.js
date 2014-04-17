/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TurtleFragmentParser parses basic Linked Data Fragment in Turtle. */

var TransformIterator = require('../iterators/Iterator').TransformIterator,
    TriplesIterator = require('../iterators/TriplesIterator'),
    rdf = require('../util/RdfUtil'),
    UriTemplate = require('uritemplate'),
    assert = require('assert');

// Extract these types of links from representations
var linkTypes = ['firstPage', 'nextPage', 'previousPage', 'lastPage'];

// Creates a new TurtleFragmentParser
function TurtleFragmentParser(source, fragmentUrl) {
  if (!(this instanceof TurtleFragmentParser))
    return new TurtleFragmentParser(source, fragmentUrl);
  TransformIterator.call(this, new TriplesIterator(source, { documentURI: fragmentUrl }));

  this._fragmentUrl = fragmentUrl;
  this._controlData = {};
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
  // Inspect metadata triples
  if (!this.hasProperty('metadata')) {
    // Parse total triple count
    if (triple.predicate === rdf.VOID_TRIPLES &&
        rdf.decodedURIEquals(this._fragmentUrl, triple.subject)) {
      var totalTriples = parseInt(rdf.getLiteralValue(triple.object), 10);
      this.setProperty('metadata', { totalTriples: totalTriples });
    }
  }
  // Save control triples for control parsing
  if (triple.predicate.indexOf(rdf.HYDRA) === 0) {
    // Store data per property and per subject
    var property = triple.predicate.substr(rdf.HYDRA.length),
        propertyData = this._controlData[property] || ( this._controlData[property] = {}),
        subjectData = propertyData[triple.subject] || (propertyData[triple.subject] = []);
    subjectData.push(triple.object);
  }
  // Pass on the triples
  push(triple), done();
};

// Sets the controls and ends
TurtleFragmentParser.prototype._flush = function (push) {
  this.setProperty('controls', this.parseControls(this._controlData));
  delete this._controlTriples;
  this._end();
};

// Parses the hypermedia controls inside the representation
TurtleFragmentParser.prototype.parseControls = function (controlData) {
  var controls = Object.create(defaultControls),
      fragmentUrl = controls._fragmentUrl = this._fragmentUrl;

  // Parse the links
  linkTypes.forEach(function (property) {
    var linkTargets = (controlData[property] || {})[fragmentUrl];
    if (linkTargets && linkTargets.length > 0)
      Object.defineProperty(controls, property, { value: linkTargets[0] });
  });

  // Parse the search form
  // TODO: Make parser independent of a specifically structured control set
  var searchForms = controlData.search;
  if (searchForms) {
    assert(Object.keys(searchForms).length === 1, 'Expected 1 hydra:search');
    var searchForm = searchForms[Object.keys(searchForms)[0]][0],
        searchTemplates = (controlData.template || {})[searchForm] || [];

    // Parse the template
    assert(searchTemplates.length === 1, 'Expected 1 hydra:template for ' + searchForm);
    var searchTemplateValue = rdf.getLiteralValue(searchTemplates[0]),
        searchTemplate = UriTemplate.parse(searchTemplateValue);

    // Parse the template mappings
    var mappings = (controlData.mapping || {})[searchForm] || [];
    assert(mappings.length === 3, 'Expected 3 hydra:mappings for ' + searchForm);
    mappings = mappings.reduce(function (mappings, mapping) {
      var variable = ((controlData.variable || {})[mapping] || [])[0],
          property = ((controlData.property || {})[mapping] || [])[0];
      assert(variable, 'Expected a hydra:variable for ' + mapping);
      assert(property, 'Expected a hydra:property for ' + mapping);
      mappings[property] = rdf.getLiteralValue(variable);
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

// Default hypermedia controls for fragments
var defaultControls = {
  getFragmentUrl: function (triplePattern) {
    throw new Error('The fragment ' + this._fragmentUrl +
                    ' does not contain basic Linked Data Fragment hypermedia controls.');
  },
};
linkTypes.forEach(function (property) {
  Object.defineProperty(defaultControls, property, {
    enumerable: true,
    get: function () {
      throw new Error('The fragment ' + this._fragmentUrl +
                      ' does not contain controls for ' + property + '.');
   },
 });
});

module.exports = TurtleFragmentParser;
