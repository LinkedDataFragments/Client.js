/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A ControlsExtractor extracts hypermedia controls from a triple stream. */

var rdf = require('../util/RdfUtil'),
    UriTemplate = require('uritemplate'),
    assert = require('assert');

// Extract these types of links from representations
var LINK_TYPES = ['firstPage', 'nextPage', 'previousPage', 'lastPage'];

/**
 * Creates a new `ControlsExtractor`.
 * @classdesc A `ControlsExtractor` extracts hypermedia controls from a triple stream.
 * @constructor
 * @augments MetadataExtractor
 */
function ControlsExtractor() {
  if (!(this instanceof ControlsExtractor))
    return new ControlsExtractor();
}

/* Extracts controls from the stream of triples. */
ControlsExtractor.prototype.extract = function (metadata, tripleStream, callback) {
  // Store control triples per property and per subject
  var controlData = Object.create(null);
  tripleStream.on('data', function (triple) {
    if (triple.predicate.indexOf(rdf.HYDRA) === 0) {
      var property = triple.predicate.substr(rdf.HYDRA.length),
          propertyData = controlData[property] || (controlData[property] = {}),
          subjectData = propertyData[triple.subject] || (propertyData[triple.subject] = []);
      subjectData.push(triple.object);
    }
  });
  // Send the controls
  tripleStream.on('end', function () {
    var controls = Object.create(defaultControls);
    controls.fragment = metadata.fragmentUrl;

    // Parse the links
    LINK_TYPES.forEach(function (property) {
      var linkTargets = (controlData[property] || {})[controls.fragment];
      if (linkTargets && linkTargets.length > 0)
        Object.defineProperty(controls, property, { value: linkTargets[0] });
    });

    // Parse the search form
    // TODO: Make parser independent of a specifically structured control set
    var searchForms = controlData.search;
    if (searchForms) {
      assert(Object.keys(searchForms).length === 1, 'Expected 1 hydra:search');
      var searchForm = searchForms[Object.keys(searchForms)[0]][0],
          searchTemplates = (controlData.template || {})[searchForm] || [];

      // Parse the template
      assert(searchTemplates.length === 1, 'Expected 1 hydra:template for ' + searchForm);
      var searchTemplateValue = rdf.getLiteralValue(searchTemplates[0]),
          searchTemplate = UriTemplate.parse(searchTemplateValue);

      // Parse the template mappings
      var mappings = (controlData.mapping || {})[searchForm] || [];
      assert(mappings.length === 3, 'Expected 3 hydra:mappings for ' + searchForm);
      mappings = mappings.reduce(function (mappings, mapping) {
        var variable = ((controlData.variable || {})[mapping] || [])[0],
            property = ((controlData.property || {})[mapping] || [])[0];
        assert(variable, 'Expected a hydra:variable for ' + mapping);
        assert(property, 'Expected a hydra:property for ' + mapping);
        mappings[property] = rdf.getLiteralValue(variable);
        return mappings;
      }, {});

      // Gets the URL of the Triple Pattern Fragment with the given triple pattern
      controls.getFragmentUrl = function (triplePattern) {
        var variables = {};
        variables[mappings[rdf.RDF_SUBJECT]]   = triplePattern.subject;
        variables[mappings[rdf.RDF_PREDICATE]] = triplePattern.predicate;
        variables[mappings[rdf.RDF_OBJECT]]    = triplePattern.object;
        return searchTemplate.expand(variables);
      };
    }

    callback && callback(null, 'controls', controls);
  });
};

// Default hypermedia controls for fragments
var defaultControls = {
  getFragmentUrl: function (triplePattern) {
    throw new Error('The fragment ' + this.fragment +
                    ' does not contain Triple Pattern Fragment hypermedia controls.');
  },
};
LINK_TYPES.forEach(function (property) {
  Object.defineProperty(defaultControls, property, {
    enumerable: true,
    get: function () {
      throw new Error('The fragment ' + this.fragment +
                      ' does not contain controls for ' + property + '.');
    },
  });
});

module.exports = ControlsExtractor;
