/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Serializing the output of a SparqlIterator */

var util = require('../util/RdfUtil'),
  TransformIterator = require('../iterators/Iterator').TransformIterator,
  _ = require('lodash'),
  xml = require('xml');

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

  var chunk = '';
  if (this._head) {
    chunk += '{"head":' + JSON.stringify(this._head) + ', "results": [';
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
  this._push(']}');
  SparqlResultWriter.prototype._end();
};

/**
 * Formats results as SPARQL+XML
 **/
function SparqlXMLResultWriter(sparqlIterator) {
  TransformIterator.call(this, sparqlIterator);

  this._root = xml.element({
    _attr: {
      xlmns: 'http://www.w3.org/2005/sparql-results#'
    }
  });

  var self = this;
  xml({
    sparql: this._root
  }, {
    stream: true,
    declaration: true,
    indent: '\t'
  }).on('data', function (chunk) {
    self._push(chunk);
  });

  this._head = [];
  if (sparqlIterator._variables && sparqlIterator._variables.length)
    this._head = _.map(sparqlIterator._variables, function (variable) {
      return {
        variable: {
          _attr: {
            name: variable.substring(1)
          }
        }
      };
    });

  this._root.push({
    head: this._head
  });
}
SparqlResultWriter.inherits(SparqlXMLResultWriter);

SparqlXMLResultWriter.prototype._writeBindings = function (result) {
  function constructSparqlResult(result) {
    return _.map(result, function (value, key) {
      key = key.substring(1); //remove '?' from variable
      if (util.isUri(value))
        return {
          binding: [{
            _attr: {
              name: key
            }
          }, {
            uri: value
          }]
        };

      else if (util.isBlank(value))
        return {
          binding: [{
            _attr: {
              name: key
            }
          }, {
            bnode: value
          }]
        };
      else {
        var literal = {
            binding: [{
              _attr: {
                name: key
              }
            }, {
              literal: util.getLiteralValue(value)

            }]
          },
          lang = util.getLiteralLanguage(value),
          type = util.getLiteralType(value);

        if (lang)
          literal.binding[1].literal = [{
            _attr: {
              "xml:lang": lang
            }
          }, literal.binding[1].literal];
        else if (type)
          literal.binding[1].literal = [{
            _attr: {
              datatype: type
            }
          }, literal.binding[1].literal];

        return literal;
      }
    });
  }

  var self = this;
  if (!this._results) {
    this._results = xml.element({});

    xml({
      results: this._results
    }, {
      stream: true,
      indent: '\t'
    }).on('data', function (chunk) {
      self._push(chunk);
    });
  }

  this._results.push({
    result: constructSparqlResult(result)
  });
};

SparqlXMLResultWriter.prototype._writeBoolean = function (result) {
  this._end = SparqlResultWriter.prototype._end; // prevent other characters from being written
  this._root.push({
    boolean: result
  });
  this._root.close();
};

SparqlXMLResultWriter.prototype._end = function (result) {
  this._results.close();
  this._root.close();
  SparqlResultWriter.prototype._end();
};



module.exports = SparqlResultWriter;
SparqlResultWriter.SparqlJSON = SparqlJSONResultWriter;
SparqlResultWriter.SparqlXML = SparqlJSONResultWriter;
SparqlResultWriter.JSON = JSONResultWriter;
