/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Serializing the output of a SparqlIterator */

var TransformIterator = require('../iterators/Iterator').TransformIterator;

function SparqlResultWriter(mediaType, sparqlIterator) {
  if (!(this instanceof SparqlResultWriter))
    return new SparqlResultWriter(mediaType, sparqlIterator);
  TransformIterator.call(this, sparqlIterator);

  if (typeof SparqlResultWriter.writers[mediaType] === 'string') SparqlResultWriter.writers[mediaType] = require(SparqlResultWriter.writers[mediaType]);
  var ResultWriter = SparqlResultWriter.writers[mediaType];
  if (!ResultWriter)
    throw new Error('The requested result format ' + mediaType + ' is not supported.');
  return new ResultWriter(sparqlIterator);
}
TransformIterator.inherits(SparqlResultWriter);

/**
Writers register as constructor or as string to be lazy loaded
{
  'application/sparql+json': SparqlJSONResultWriter,
  'application/json': './lib/writers/JSONResultWriter',
  'application/sparql+xml': SparqlXMLResultWriter
};
**/
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

SparqlResultWriter.prototype._writeBoolean = function (result, done) {
  throw new Error('The _writeBoolean method has not been implemented.');
};

SparqlResultWriter.prototype._writeBindings = function (result, done) {
  throw new Error('The _writeBindings method has not been implemented.');
};

module.exports = SparqlResultWriter;
