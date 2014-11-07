/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Serializing the output of a SparqlIterator */

var TransformIterator = require('../iterators/Iterator').TransformIterator;

function SparqlResultWriter(mediaType, sparqlIterator) {
  // Handle subclass construction by initializing an instance
  if (mediaType === true) {
    TransformIterator.call(this, true);
    this._empty = true;
    return setImmediate((function () {
      var variables = sparqlIterator.getProperty('variables') || [];
      this._writeHead(variables.map(function (v) { return v.substring(1); }));
      this.setSource(sparqlIterator);
    }).bind(this));
  }
  // Otherwise, create a result writer based on the media type
  else {
    var ResultWriter = SparqlResultWriter.writers[mediaType];
    if (!ResultWriter)
      throw new Error('The requested result format ' + mediaType + ' is not supported.');
    if (typeof ResultWriter === 'string')
      ResultWriter = SparqlResultWriter.writers[mediaType] = require(ResultWriter);
    return new ResultWriter(sparqlIterator);
  }
}
TransformIterator.inherits(SparqlResultWriter);

SparqlResultWriter.writers = {};
SparqlResultWriter.register = function (mimeType, ResultWriter) {
  SparqlResultWriter.writers[mimeType] = ResultWriter;
};

SparqlResultWriter.prototype._writeHead = function (variableNames) { };

SparqlResultWriter.prototype._transform = function (result, done) {
  if (typeof result === 'boolean')
    this._writeBoolean(result);
  else
    this._writeBindings(result);
  delete this._empty;
  done();
};

SparqlResultWriter.prototype._writeBindings = function (result, done) {
  throw new Error('The _writeBindings method has not been implemented.');
};

SparqlResultWriter.prototype._writeBoolean = function (result, done) {
  throw new Error('The _writeBoolean method has not been implemented.');
};

module.exports = SparqlResultWriter;
