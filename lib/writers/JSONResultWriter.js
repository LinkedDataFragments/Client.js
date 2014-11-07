/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Serializing the output as a plain JSON array */

var SparqlResultWriter = require('./SparqlResultWriter'),
    TransformIterator = require('../iterators/Iterator').TransformIterator;

/**
 * Formats results as JSON
 **/
function JSONResultWriter(sparqlIterator) {
  TransformIterator.call(this, sparqlIterator);
  this._empty = true;
  this._push('[');
}
SparqlResultWriter.inherits(JSONResultWriter);

JSONResultWriter.prototype._transform = function (result, done) {
  var prefix = ',';
  if (this._empty) {
    prefix = '';
    this._empty = false;
  }
  this._push(prefix + JSON.stringify(result));
  done();
};

JSONResultWriter.prototype._end = function () {
  this._push(']');
  SparqlResultWriter.prototype._end();
};

module.exports = JSONResultWriter;
