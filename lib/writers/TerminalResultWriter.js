/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Writer that serializes a SPARQL query result as a plain Console table */

var SparqlResultWriter = require('./SparqlResultWriter');

function ConsoleResultWriter(sparqlIterator) {
  SparqlResultWriter.call(this, true, sparqlIterator);
}
SparqlResultWriter.inherits(ConsoleResultWriter);

function pad(value) {
  var padString =  "                                                       ";
  return (padString + value).slice(-padString.length);
}

ConsoleResultWriter.prototype._writeHead = function (variableNames) {
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

ConsoleResultWriter.prototype._transform = function (result, done) {
  var self = this;
  this._variableNames.forEach(function (variable) {
    self._push(pad(result['?' + variable]));
  });
  this._push('\n');
  done();
};

ConsoleResultWriter.prototype._end = function () {
  SparqlResultWriter.prototype._end.call(this);
};

module.exports = ConsoleResultWriter;
