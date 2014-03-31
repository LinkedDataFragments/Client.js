/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** An SparqlIterator returns the results of a SPARQL query. */

var SparqlParser = require('sparql-parser'),
    Iterator = require('./Iterator'),
    TransformIterator = Iterator.TransformIterator,
    GraphPatternIterator = require('./GraphPatternIterator'),
    N3 = require('n3'),
    rdf = require('../rdf/RdfUtil'),
    assert = require('assert');

// Creates an iterator from a SPARQL query
function SparqlIterator(query, options) {
  // Parse the query and select the appropriate sub-iterator
  if (typeof(query) === 'string') {
    query = new SparqlParser(options.prefixes).parse(query);
    switch (query.type) {
    case 'SELECT':
      return new SparqlSelectIterator(query, options);
    case 'CONSTRUCT':
      return new SparqlConstructIterator(query, options);
    default:
      throw new Error('Unsupported query type: ' + query.type);
    }
  }

  // If the query was already parsed, construct the SparqlIterator superclass
  if (!(this instanceof SparqlIterator))
    return new SparqlIterator(query, options);
  TransformIterator.call(this, true, options);
  this.parsedQuery = query;
}
TransformIterator.inherits(SparqlIterator);



// Creates an iterator for a parsed SPARQL SELECT query
function SparqlSelectIterator(query, options) {
  assert.equal(query.type, 'SELECT');
  if (!(this instanceof SparqlSelectIterator))
    return new SparqlSelectIterator(query, options);
  SparqlIterator.call(this, query, options);

  this._variables = query.variables;
  this.setSource(new BGPIterator(query.pattern, options));
}
SparqlIterator.inherits(SparqlSelectIterator);

// Executes the SELECT projection
SparqlSelectIterator.prototype._transform = function (bindings, push, done) {
  push(this._variables.reduce(function (row, variable) {
    // Project a simple variable by copying its value
    if (variable !== '*')
      row[variable] = bindings[variable];
    // Project a star selector by copying all values
    else
      for (variable in bindings)
        row[variable] = bindings[variable];
    return row;
  }, Object.create(null)));
  done();
};



// Creates an iterator for a parsed SPARQL CONSTRUCT query
function SparqlConstructIterator(query, options) {
  assert.equal(query.type, 'CONSTRUCT');
  if (!(this instanceof SparqlConstructIterator))
    return new SparqlConstructIterator(query, options);
  SparqlIterator.call(this, query, options);

  this._template = query.template;
  this.setSource(new BGPIterator(query.pattern, options));
}
SparqlIterator.inherits(SparqlConstructIterator);

// Executes the CONSTRUCT projection
SparqlConstructIterator.prototype._transform = function (bindings, push, done) {
  this._template.forEach(function (triplePattern) {
    push(rdf.applyBindings(bindings, triplePattern));
  });
  done();
};



// Creates an iterator for a basic graph pattern
function BGPIterator(queryToken, options) {
  assert.equal(queryToken.type, 'BGP');
  return new GraphPatternIterator(Iterator.single({}), queryToken.triples, options);
}
Iterator.inherits(BGPIterator);

module.exports = SparqlIterator;
