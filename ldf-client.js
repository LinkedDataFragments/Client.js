/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Main ldf-client module exports. */
var SparqlResultWriter = require('./lib/writers/SparqlResultWriter');
SparqlResultWriter.register('application/json', './JSONResultWriter');
SparqlResultWriter.register('application/sparql-results+json', './SparqlJSONResultWriter');
SparqlResultWriter.register('application/sparql-results+xml', './SparqlXMLResultWriter');

module.exports = {
  SparqlIterator: require('./lib/triple-pattern-fragments/SparqlIterator.js'),
  FragmentsClient: require('./lib/triple-pattern-fragments/FragmentsClient'),
  Logger: require('./lib/util/Logger'),
  SparqlResultWriter: SparqlResultWriter,
};
