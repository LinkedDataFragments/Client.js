var SparqlResultWriter = require('./SparqlResultWriter'),
  TransformIterator = require('../iterators/Iterator').TransformIterator,
  util = require('../util/RdfUtil'),
  _ = require('lodash');

/**
 * Formats results as SPARQL+JSON
 **/
function SparqlJSONResultWriter(sparqlIterator) {
  TransformIterator.call(this, sparqlIterator);

  this._empty = true;
  this._vars = _.map(sparqlIterator._variables, function (variable) {
    return variable.substring(1);
  });
  this._writeHead();
}
SparqlResultWriter.inherits(SparqlJSONResultWriter);

function constructSparqlResult(result) {
  return _.transform(result, function (result, value, key) {
    key = key.substring(1); //remove '?' from variable
    if (util.isUri(value))
      result[key] = {
        type: 'uri',
        value: value
      };

    else if (util.isBlank(value))
      result[key] = {
        type: 'bnode',
        value: value
      };
    else {
      var literal = {
          type: 'literal',
          value: util.getLiteralValue(value)
        },
        lang = util.getLiteralLanguage(value),
        type = util.getLiteralType(value);

      if (lang)
        literal["xml:lang"] = lang;
      else if (type)
        literal.datatype = type;

      result[key] = literal;
    }
  });
}
SparqlJSONResultWriter.prototype._writeHead = function () {
  var head = {};
  if (this._vars && this._vars.length)
    head.vars = this._vars;

  this._push('{"head":' + JSON.stringify(head) + ', ');
};

SparqlJSONResultWriter.prototype._writeBindings = function (result) {
  var prefix = ',';
  if (this._empty) {
    prefix = '"results": { "bindings": [';
    this._empty = false;
  }
  this._push(prefix + JSON.stringify(constructSparqlResult(result)));
};

SparqlJSONResultWriter.prototype._writeBoolean = function (result) {
  this._end = SparqlResultWriter.prototype._end; // prevent other characters from being written
  this._push('"boolean":' + result + '}');
};

SparqlJSONResultWriter.prototype._end = function (result) {
  if (this._empty)
    this._push('"results": { "bindings": [');
  this._push(']}}');
  SparqlResultWriter.prototype._end();
};

module.exports = SparqlJSONResultWriter;
