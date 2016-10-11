/*! @license MIT ©2014-2016 Miel Vander Sande, Ghent University - imec */
/* Serializing the output of a SparqlIterator */

var TransformIterator = require('asynciterator').TransformIterator;

function SparqlResultWriter(source) {
  TransformIterator.call(this, source);
  this._empty = true;
  this._variables = source.getProperty('variables') || [];
}
TransformIterator.subclass(SparqlResultWriter);

SparqlResultWriter.prototype._begin = function (done) {
  this._writeHead(this._variables.map(function (v) { return v.substring(1); }));
  done();
};

SparqlResultWriter.prototype._writeHead = function (variableNames) { };

SparqlResultWriter.prototype._transform = function (result, done) {
  if (typeof result === 'boolean')
    this._writeBoolean(result);
  else
    this._writeBindings(result);
  this._empty = false;
  done();
};

SparqlResultWriter.prototype._writeBindings = function (result) {
  throw new Error('The _writeBindings method has not been implemented.');
};

SparqlResultWriter.prototype._writeBoolean = function (result) {
  throw new Error('The _writeBoolean method has not been implemented.');
};

// Index of registered writers
SparqlResultWriter.writers = {};

// Register a writer for a given media type
SparqlResultWriter.register = function (mediaType, ResultWriter) {
  SparqlResultWriter.writers[mediaType] = ResultWriter;
};

// Instantiate a writer of a given media type
SparqlResultWriter.instantiate = function (mediaType, source) {
  // Look up the class or class name
  var ResultWriter = SparqlResultWriter.writers[mediaType];
  if (!ResultWriter)
    throw new Error('No writer available for media type ' + mediaType + '.');
  // If it is a class name, load the class
  if (typeof ResultWriter === 'string')
    ResultWriter = SparqlResultWriter.writers[mediaType] = require(ResultWriter);
  // Create an instance of the subclass
  return new ResultWriter(source);
};

module.exports = SparqlResultWriter;
