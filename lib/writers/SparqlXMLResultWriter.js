/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Writer that serializes a SPARQL query result application/sparql+xml */

var SparqlResultWriter = require('./SparqlResultWriter'),
    _ = require('lodash'),
    util = require('../util/RdfUtil'),
    xml = require('xml');

function SparqlXMLResultWriter(sparqlIterator) {
  SparqlResultWriter.call(this, true, sparqlIterator);
}
SparqlResultWriter.inherits(SparqlXMLResultWriter);

SparqlXMLResultWriter.prototype._writeHead = function (variableNames) {
  var root = this._root = xml.element({ _attr: { xlmns: 'http://www.w3.org/2005/sparql-results#' } }),
      results = this._results = xml.element({}), self = this;
  xml({ sparql: root }, { stream: true, indent: '  ', declaration: true })
     .on('data', function (chunk) { self._push(chunk + '\n'); });
  if (variableNames.length)
    root.push({ head: variableNames.map(function (v) { return { variable: { _attr: { name: v } } }; }) });
  root.push({ results: results });
};

SparqlXMLResultWriter.prototype._writeBindings = function (result) {
  //Unbounded variables cannot be part of xml
  result = _.omit(result, function (value) {
    return value === undefined || value === null;
  });
  this._results.push({
    result: _.map(result, function (value, variable) {
      var xmlValue, lang, type;
      if (!util.isLiteral(value))
        xmlValue = util.isBlank(value) ? { bnode: value } : { uri: value };
      else {
        xmlValue = { literal: util.getLiteralValue(value) };
        if (lang = util.getLiteralLanguage(value))
          xmlValue.literal = [{ _attr: { 'xml:lang': lang } }, xmlValue.literal];
        else if (type = util.getLiteralType(value))
          xmlValue.literal = [{ _attr: {   datatype: type } }, xmlValue.literal];
      }
      return { binding: [{ _attr: { name: variable.substring(1) } }, xmlValue] };
    })
  });
};

SparqlXMLResultWriter.prototype._writeBoolean = function (result) {
  this._root.push({ boolean: result });
};

SparqlXMLResultWriter.prototype._end = function () {
  this._results.close();
  this._root.close();
  SparqlResultWriter.prototype._end.call(this);
};

module.exports = SparqlXMLResultWriter;
