/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Serializing the output of a SparqlIterator */

var TransformIterator = require('../iterators/Iterator').TransformIterator;

function SparqlResultWriter(mediaType, sparqlIterator) {
  if (!(this instanceof SparqlResultWriter))
    return new SparqlResultWriter(mediaType, sparqlIterator);
  TransformIterator.call(this, sparqlIterator);

  if (typeof SparqlResultWriter.writers[mediaType] === 'string') SparqlResultWriter.writers[mediaType] = require(SparqlResultWriter.writers[mediaType]);
  var ResultWriter = SparqlResultWriter.writers[mediaType];
  return ResultWriter ? new ResultWriter(sparqlIterator) : this;
}
TransformIterator.inherits(SparqlResultWriter);

/**
var writers = {
  'application/sparql+json': SparqlJSONResultWriter,
  'application/json': JSONResultWriter,
  'application/sparql+xml': SparqlXMLResultWriter
}; **/
SparqlResultWriter.writers = {};
SparqlResultWriter.register = function (mimeType, ResultWriter) {
  SparqlResultWriter.writers[mimeType] = ResultWriter;
};

SparqlResultWriter.prototype._transform = function (result, done) {
  if (typeof result === 'boolean')
    this._writeBoolean(result);
  else
    this._writeBindings(result);
  done();
};

SparqlResultWriter.prototype._writeBoolean = SparqlResultWriter.prototype._writeBindings = function (result, done) {
  this._push(result);
};

module.exports = SparqlResultWriter;
