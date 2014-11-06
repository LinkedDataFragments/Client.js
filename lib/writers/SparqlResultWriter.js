/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Serializing the output of a SparqlIterator */

var util = require('../util/RdfUtil'),
  TransformIterator = require('../iterators/Iterator').TransformIterator,
  _ = require('lodash');

function SparqlResultWriter(mediaType, sparqlIterator) {
  if (!(this instanceof SparqlResultWriter))
    return new SparqlResultWriter(mediaType, sparqlIterator);
  TransformIterator.call(this, sparqlIterator);

  var ResultWriter = writers[mediaType];
  return ResultWriter ? new ResultWriter(sparqlIterator) : this;
}
TransformIterator.inherits(SparqlResultWriter);

var writers = {
  'application/sparql+json': SparqlJSONResultWriter,
  'application/json': JSONResultWriter,
  'application/sparql+xml': SparqlXMLResultWriter
};

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

SparqlResultWriter.prototype._transform = function (result, done) {
  if (typeof result === 'boolean')
    this._writeBoolean(result);
  else
    this._writeBindings(result);
  done();
};

SparqlResultWriter.prototype._writeBoolean = SparqlResultWriter.prototype._writeBindings = function (result, done) {
  this._push(result);
};


/**
 * Formats results as JSON
 **/
function JSONResultWriter(sparqlIterator) {
  TransformIterator.call(this, sparqlIterator);
}
SparqlResultWriter.inherits(JSONResultWriter);

JSONResultWriter.prototype._transform = function (result, done) {
  this._push(JSON.stringify(result));
  done();
};

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

SparqlJSONResultWriter.prototype._writeBindings = function (result) {
  var chunk = '';
  if (this._head) {
    chunk += '{"head":' + JSON.stringify(this._head) + ', "results": [';
    this._head = null;
  } else chunk += ',';

  chunk += JSON.stringify(constructSparqlResult(result));

  this._push(chunk);
};

SparqlJSONResultWriter.prototype._writeBoolean = function (result) {
  this._push('{"head": {}, "boolean":' + result + '}');
  SparqlResultWriter.prototype._end();
};

SparqlJSONResultWriter.prototype._end = function (result) {
  this._push(']}');
  SparqlResultWriter.prototype._end();
};

/**
 * Formats results as SPARQL+XML
 **/
function SparqlXMLResultWriter(output) {

}
SparqlResultWriter.inherits(SparqlXMLResultWriter);

module.exports = SparqlResultWriter;
SparqlResultWriter.SparqlJSON = SparqlJSONResultWriter;
SparqlResultWriter.SparqlXML = SparqlJSONResultWriter;
SparqlResultWriter.JSON = JSONResultWriter;
