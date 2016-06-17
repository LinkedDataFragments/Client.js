/*! @license ©2016 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* Writer that serializes SPARQL query results as a list. */

var SparqlResultWriter = require('./SparqlResultWriter'),
    _ = require('lodash');

function SimpleResultWriter(sparqlIterator) {
  SparqlResultWriter.call(this, true, sparqlIterator);
}
SparqlResultWriter.inherits(SimpleResultWriter);

SimpleResultWriter.prototype._writeHead = function (variables) {
  var maxLength = _.max(_.pluck(variables, 'length'));
  variables.map(function (v) {
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
