var SparqlResultWriter = require('./SparqlResultWriter'),
    TransformIterator = require('../iterators/Iterator').TransformIterator,
    util = require('../util/RdfUtil'),
    _ = require('lodash');

/**
 * Formats results as SPARQL+JSON
 **/
function SparqlJSONResultWriter(sparqlIterator) {
  TransformIterator.call(this, sparqlIterator);

  this._head = {};
  if (sparqlIterator._variables && sparqlIterator._variables.length)
    this._head.vars = _.map(sparqlIterator._variables, function (variable) {
      return variable.substring(1);
    });
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

SparqlJSONResultWriter.prototype._writeBindings = function (result) {
  var chunk = '';
  if (this._head) {
    chunk += '{"head":' + JSON.stringify(this._head) + ', "results": { "bindings": [';
    this._head = null;
  } else chunk += ',';

  chunk += JSON.stringify(constructSparqlResult(result));

  this._push(chunk);
};

SparqlJSONResultWriter.prototype._writeBoolean = function (result) {
  this._end = SparqlResultWriter.prototype._end; // prevent other characters from being written
  this._push('{"head": {}, "boolean":' + result + '}');
};

SparqlJSONResultWriter.prototype._end = function (result) {
  this._push(']}}');
  SparqlResultWriter.prototype._end();
};

module.exports = SparqlJSONResultWriter;
