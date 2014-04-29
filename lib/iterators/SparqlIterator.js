/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** An SparqlIterator returns the results of a SPARQL query. */

var SparqlParser = require('sparql-parser'),
    Iterator = require('./Iterator'),
    TransformIterator = Iterator.TransformIterator,
    GraphPatternIterator = require('./GraphPatternIterator'),
    UnionIterator = require('./UnionIterator'),
    FilterIterator = require('./FilterIterator'),
    SortIterator = require('./SortIterator'),
    LimitIterator = require('./LimitIterator'),
    DistinctIterator = require('./DistinctIterator'),
    SparqlExpressionEvaluator = require('../util/SparqlExpressionEvaluator'),
    N3 = require('n3'),
    _ = require('lodash'),
    util = require('util'),
    rdf = require('../util/RdfUtil');

// Creates an iterator from a SPARQL query
function SparqlIterator(source, queryText, options) {
  // Shift arguments if `source` was omitted
  if (typeof source === 'string')
    options = queryText, queryText = source, source = null;

  // Transform the query into a cascade of iterators
  try {
    // Parse the query
    var query = new SparqlParser(options.prefixes).parse(queryText);
    // Create an iterator for bindings of the query's graph pattern
    var graphIterator = new SparqlGraphPatternGroupIterator(
                              source || Iterator.single({}), query.patterns, options);
    // Create iterators for each filter
    for (var i = 0, l = query.filters && query.filters.length; i < l; i++) {
      var filter = SparqlExpressionEvaluator(query.filters[i]);
      graphIterator = new FilterIterator(graphIterator, filter, options);
    }
    // Create iterators for each order
    for (i = query.order && (query.order.length - 1); i >= 0; i--) {
      var order = SparqlExpressionEvaluator(query.order[i].expression),
          ascending = !query.order[i].descending;
      graphIterator = new SortIterator(graphIterator, function (a, b) {
        var orderA = order(a), orderB = order(b);
        if (orderA < orderB) return ascending ? -1 :  1;
        if (orderB < orderA) return ascending ?  1 : -1;
        return 0;
      }, options);
    }
    // Create an iterator that projects the bindings according to the query type
    var queryIterator, QueryConstructor = queryConstructors[query.type];
    if (!QueryConstructor)
      throw new Error('No iterator available for query type: ' + query.type);
    queryIterator = new QueryConstructor(graphIterator, query, options);
    // Create iterators for modifiers
    if (query.modifier === 'DISTINCT')
      queryIterator = new DistinctIterator(queryIterator, options);
    // Add offsets and limits if requested
    if ('offset' in query || 'limit' in query)
      queryIterator = new LimitIterator(queryIterator, query.offset, query.limit, options);
    queryIterator.parsedQuery = query;
    return queryIterator;
  }
  catch (error) {
    // Report syntax errors directly
    if (error instanceof SparqlParser.SparqlSyntaxError) throw error;
    // All other types of errors indicate the query is not supported
    throw new UnsupportedQueryError(query, error);
  }
}
TransformIterator.inherits(SparqlIterator);

var queryConstructors = {
  SELECT: SparqlSelectIterator,
  CONSTRUCT: SparqlConstructIterator,
};



// Creates an iterator for a parsed SPARQL SELECT query
function SparqlSelectIterator(source, query, options) {
  TransformIterator.call(this, source, options);
  this._variables = query.variables;
}
SparqlIterator.inherits(SparqlSelectIterator);

// Executes the SELECT projection
SparqlSelectIterator.prototype._transform = function (bindings, done) {
  this._push(this._variables.reduce(function (row, variable) {
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
function SparqlConstructIterator(source, query, options) {
  TransformIterator.call(this, source, options);
  this._template = query.template;
}
SparqlIterator.inherits(SparqlConstructIterator);

// Executes the CONSTRUCT projection
SparqlConstructIterator.prototype._transform = function (bindings, done) {
  this._template.forEach(function (triplePattern) {
    var triple = rdf.applyBindings(bindings, triplePattern);
    if (!rdf.hasVariables(triple))
      this._push(triple);
    // TODO: blank nodes should get different identifiers on each iteration
    // TODO: discard repeated identical bindings of the same variable
  }, this);
  done();
};




// Creates an iterator for a SPARQL group of graph patterns
function SparqlGraphPatternGroupIterator(source, patterns, options) {
  // Collect patterns by type to check if we can do structural simplifications
  var patternsByType = _.groupBy(patterns, 'type'),
      types = Object.keys(patternsByType);

  // Convert multiple BGPs into a single BGP, and put them first
  if (patternsByType.BGP && patterns.length > 1) {
    patterns = _.difference(patterns, patternsByType.BGP);
    if (patternsByType.BGP.length > 1)
      patternsByType.BGP = [{
        type: 'BGP',
        triples: _.flatten(_.map(patternsByType.BGP, 'triples'), true),
      }];
    patterns.unshift(patternsByType.BGP[0]);
  }

  // Chain iterators for each of the graphs in the group
  return patterns.reduce(function (source, pattern) {
     return new SparqlGraphPatternIterator(source, pattern, options);
  }, source);
}
Iterator.inherits(SparqlGraphPatternIterator);




// Creates an iterator for a SPARQL graph pattern
function SparqlGraphPatternIterator(source, queryToken, options) {
  // Reset flags on the options for child iterators
  var childOptions = options.optional ? _.create(options, { optional: false }) : options;

  switch (queryToken.type) {
  case 'BGP':
    return new GraphPatternIterator(source, queryToken.triples, options);
  case 'OPTIONAL':
    options = _.create(options, { optional: true });
    return new SparqlGraphPatternGroupIterator(source, queryToken.patterns, options);
  case 'UNION':
    return new UnionIterator(queryToken.patterns.map(function (patternToken) {
      return new SparqlGraphPatternIterator(source.clone(), patternToken, childOptions);
    }), options);
  default:
    throw new Error('Unsupported graph pattern type: ' + queryToken.type);
  }
}
Iterator.inherits(SparqlGraphPatternIterator);



// Error thrown when no combination of iterators can solve the query
function UnsupportedQueryError(query, source) {
  Error.call(this, source);
  this.name = 'UnsupportedQueryError';
  this.query = query;
  this.source = source;
  this.message = 'The following query is not (yet) supported:\n' + query.replace(/^/mg, '  ');
}
util.inherits(UnsupportedQueryError, Error);



module.exports = SparqlIterator;
SparqlIterator.UnsupportedQueryError = UnsupportedQueryError;
