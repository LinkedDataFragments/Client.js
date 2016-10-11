/*! @license MIT ©2016 Miel Vander Sande, Ghent University - imec */
/* Writer that serializes a SPARQL query result as a plain table */

var SparqlResultWriter = require('./SparqlResultWriter');

function TableResultWriter(sparqlIterator) {
  SparqlResultWriter.call(this, sparqlIterator);
  this._columnWidth = 50;
  this._padding = repeat(' ', this._columnWidth);
}
SparqlResultWriter.subclass(TableResultWriter);

TableResultWriter.prototype._writeHead = function (variableNames) {
  var header = variableNames.map(this._pad, this).join(' ');
  this._push(header +  '\n' + repeat('-', header.length) + '\n');
};

TableResultWriter.prototype._writeBindings = function (bindings) {
  this._push(this._variables.map(function (variable) {
    return this._pad(bindings[variable] + '');
  }, this).join(' ') + '\n');
};

TableResultWriter.prototype._pad = function (value) {
  if (value.length <= this._columnWidth)
    return value + this._padding.slice(value.length);
  else
    return value.slice(0, this._columnWidth - 1) + '…';
};

function repeat(string, count) {
  return new Array(count + 1).join(string);
}

module.exports = TableResultWriter;
