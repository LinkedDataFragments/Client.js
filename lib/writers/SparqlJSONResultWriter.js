/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Writer that serializes a SPARQL query result as application/sparql+json */

var SparqlResultWriter = require('./SparqlResultWriter'),
    util = require('../util/RdfUtil'),
    _ = require('lodash');

function SparqlJSONResultWriter(sparqlIterator) {
  if (!(this instanceof SparqlJSONResultWriter))
    return new SparqlJSONResultWriter(sparqlIterator);
  SparqlResultWriter.call(this, true, sparqlIterator);
}
SparqlResultWriter.inherits(SparqlJSONResultWriter);

SparqlJSONResultWriter.prototype._writeHead = function (variableNames) {
  var head = {};
  if (variableNames.length) head.vars = variableNames;
  this._push('{"head": ' + JSON.stringify(head) + ',\n');
};

SparqlJSONResultWriter.prototype._writeBindings = function (result) {
  this._push(this._empty ? '"results": { "bindings": [\n' : ',\n');
  this._push(JSON.stringify(_.transform(result, function (result, value, variable) {
    if (value !== undefined && value !== null) {
      variable = variable.substring(1);
      if (!util.isLiteral(value))
        result[variable] = { value: value, type: util.isBlank(value) ? 'bnode' : 'uri' };
      else {
        var literal, lang, type;
        literal = result[variable] = { value: util.getLiteralValue(value), type: 'literal' };
        if (lang = util.getLiteralLanguage(value))
          literal['xml:lang'] = lang;
        else if (type = util.getLiteralType(value))
          literal.datatype = type;
      }
    }
  })));
};

SparqlJSONResultWriter.prototype._writeBoolean = function (result) {
  this._end = SparqlResultWriter.prototype._end; // prevent other characters from being written
  this._push('"boolean":' + result + '}\n');
};

SparqlJSONResultWriter.prototype._end = function () {
  this._push(this._empty ? '"results": { "bindings": [] }}\n' : '\n]}}\n');
  SparqlResultWriter.prototype._end.call(this);
};

module.exports = SparqlJSONResultWriter;
