/*! @license ©2016 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Writer that serializes a SPARQL query result as a plain table */

var SparqlResultWriter = require('./SparqlResultWriter');

function TableResultWriter(sparqlIterator) {
  SparqlResultWriter.call(this, true, sparqlIterator);
}
SparqlResultWriter.inherits(TableResultWriter);

function pad(value) {
  var padString =  "                                                       ";
  return (padString + value).slice(-padString.length);
}

TableResultWriter.prototype._writeHead = function (variableNames) {
  var header = '';
  variableNames.forEach(function (variable) {
    header += pad(variable);
  });

  this._push(header + '\n');

  for (var i = 0; i < header.length; i++)
    this._push('-');

  this._push('\n');

  this._variableNames = variableNames;
};

TableResultWriter.prototype._transform = function (result, done) {
  var self = this;
  this._variableNames.forEach(function (variable) {
    self._push(pad(result['?' + variable]));
  });
  this._push('\n');
  done();
};

TableResultWriter.prototype._end = function () {
  SparqlResultWriter.prototype._end.call(this);
};

module.exports = TableResultWriter;
