/*! @license MIT ©2016 Ruben Verborgh, Ghent University - imec */
/* Writer that serializes SPARQL query results as a list. */

var SparqlResultWriter = require('./SparqlResultWriter'),
    _ = require('lodash');

function SimpleResultWriter(sparqlIterator) {
  SparqlResultWriter.call(this, sparqlIterator);
}
SparqlResultWriter.subclass(SimpleResultWriter);

SimpleResultWriter.prototype._writeHead = function (variableNames) {
  var maxLength = _.max(_.pluck(variableNames, 'length'));
  variableNames.map(function (v) {
    this['?' + v] = new Array(maxLength - v.length + 1).join(' ') + v + ': ';
  }, this._paddedNames = {});
};

SimpleResultWriter.prototype._writeBindings = function (bindings) {
  this._empty || this._push('\n');
  this._push(_.map(bindings, function (value, variable) {
    return this[variable] + value;
  }, this._paddedNames).join('\n') + '\n');
};

module.exports = SimpleResultWriter;
