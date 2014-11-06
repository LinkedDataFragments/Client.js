var SparqlResultWriter = require('./SparqlResultWriter'),
    TransformIterator = require('../iterators/Iterator').TransformIterator,
    _ = require('lodash'),
    util = require('../util/RdfUtil'),
    xml = require('xml');

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

SparqlXMLResultWriter.prototype._writeBindings = function (result) {
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

module.exports = SparqlXMLResultWriter;
