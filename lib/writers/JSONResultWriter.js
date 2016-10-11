/*! @license MIT ©2014-2016 Miel Vander Sande, Ghent University - imec */
/* Writer that serializes a SPARQL query result as a plain JSON array */

var SparqlResultWriter = require('./SparqlResultWriter');

function JSONResultWriter(sparqlIterator) {
  SparqlResultWriter.call(this, sparqlIterator);
}
SparqlResultWriter.subclass(JSONResultWriter);

JSONResultWriter.prototype._writeHead = function () {
  this._push('[');
};

JSONResultWriter.prototype._writeBindings = function (bindings) {
  this._push(this._empty ? '\n' : ',\n');
  this._push(JSON.stringify(bindings).trim());
};

JSONResultWriter.prototype._writeBoolean = function (result) {
  this._push('\n' + result);
};

JSONResultWriter.prototype._flush = function (done) {
  this._push(this._empty ? ']\n' : '\n]\n');
  done();
};

module.exports = JSONResultWriter;
