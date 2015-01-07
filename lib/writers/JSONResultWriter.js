/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Writer that serializes a SPARQL query result as a plain JSON array */

var SparqlResultWriter = require('./SparqlResultWriter');

function JSONResultWriter(sparqlIterator) {
  SparqlResultWriter.call(this, true, sparqlIterator);
}
SparqlResultWriter.inherits(JSONResultWriter);

JSONResultWriter.prototype._writeHead = function () {
  this._push('[');
};

JSONResultWriter.prototype._transform = function (result, done) {
  this._push(this._empty ? (delete this._empty) && '\n' : ',\n');
  this._push(JSON.stringify(result).trim());
  done();
};

JSONResultWriter.prototype._end = function () {
  this._push(this._empty ? ']\n' : '\n]\n');
  SparqlResultWriter.prototype._end.call(this);
};

module.exports = JSONResultWriter;
