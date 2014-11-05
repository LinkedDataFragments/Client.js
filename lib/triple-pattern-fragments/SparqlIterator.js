/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* An SparqlIterator returns the results of a SPARQL query. */

var SparqlParser = require('sparqljs').Parser,
    Iterator = require('../iterators/Iterator'),
    TransformIterator = Iterator.TransformIterator,
    ReorderingGraphPatternIterator = require('./ReorderingGraphPatternIterator'),
    UnionIterator = require('../iterators/UnionIterator'),
    FilterIterator = require('../iterators/FilterIterator'),
    SortIterator = require('../iterators/SortIterator'),
    LimitIterator = require('../iterators/LimitIterator'),
    DistinctIterator = require('../iterators/DistinctIterator'),
    SparqlExpressionEvaluator = require('../util/SparqlExpressionEvaluator'),
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
    // Create an iterator that projects the bindings according to the query type
    var query = new SparqlParser(options.prefixes).parse(queryText),
        queryIterator, QueryConstructor = queryConstructors[query.queryType];
    if (!QueryConstructor)
      throw new Error('No iterator available for query type: ' + query.queryType);
    queryIterator = new QueryConstructor(true, query, options);

    // Create an iterator for bindings of the query's graph pattern
    var graphIterator = new SparqlGroupsIterator(source || Iterator.single({}),
                              queryIterator.patterns || query.where, options);
    // Create iterators for each order
    for (var i = query.order && (query.order.length - 1); i >= 0; i--) {
      var order = SparqlExpressionEvaluator(query.order[i].expression),
          ascending = !query.order[i].descending;
      graphIterator = new SortIterator(graphIterator, function (a, b) {
        var orderA = order(a), orderB = order(b);
        if (orderA < orderB) return ascending ? -1 :  1;
        if (orderB < orderA) return ascending ?  1 : -1;
        return 0;
      }, options);
    }
    queryIterator.setSource(graphIterator);

    // Create iterators for modifiers
    if (query.distinct)
      queryIterator = new DistinctIterator(queryIterator, options);
    // Add offsets and limits if requested
    if ('offset' in query || 'limit' in query)
      queryIterator = new LimitIterator(queryIterator, query.offset, query.limit, options);
    queryIterator.queryType = query.queryType;
    return queryIterator;
  }
  catch (error) {
    if (/Parse error/.test(error.message))
      error = new InvalidQueryError(queryText, error);
    else
      error = new UnsupportedQueryError(queryText, error);
    throw error;
  }
}
TransformIterator.inherits(SparqlIterator);

var queryConstructors = {
  SELECT: SparqlSelectIterator,
  CONSTRUCT: SparqlConstructIterator,
  DESCRIBE: SparqlDescribeIterator,
  ASK: SparqlAskIterator,
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



// Creates an iterator for a parsed SPARQL DESCRIBE query
function SparqlDescribeIterator(source, query, options) {
  SparqlConstructIterator.call(this, source, query, options);

  // Create a template with `?var ?p ?o` patterns for each variable
  var variables = query.variables, template = this._template = [];
  for (var i = 0, l = variables.length; i < l; i++)
    template.push(rdf.triple(variables[i], '?__predicate' + i, '?__object' + i));
  // Add the template to this query's patterns
  this.patterns = query.where.concat({ type: 'bgp', triples: template });
}
SparqlConstructIterator.inherits(SparqlDescribeIterator);

// Creates an iterator for a parsed SPARQL ASK query
function SparqlAskIterator(source, query, options) {
  TransformIterator.call(this, source, options);
  this._result = false;
}
SparqlIterator.inherits(SparqlAskIterator);

SparqlAskIterator.prototype._transform = function (bindings, done) {
  this._result = true;
  this._end();
  done();
};

SparqlAskIterator.prototype._end = function () {
  this._push(this._result);
  SparqlIterator.prototype._end.call(this);
};


// Creates an iterator for a list of SPARQL groups
function SparqlGroupsIterator(source, groups, options) {
  // Chain iterators for each of the graphs in the group
  return groups.reduce(function (source, group) {
    return new SparqlGroupIterator(source, group, options);
  }, source);
}
Iterator.inherits(SparqlGroupIterator);




// Creates an iterator for a SPARQL group
function SparqlGroupIterator(source, group, options) {
  // Reset flags on the options for child iterators
  var childOptions = options.optional ? _.create(options, { optional: false }) : options;

  switch (group.type) {
  case 'bgp':
    return new ReorderingGraphPatternIterator(source, group.triples, options);
  case 'optional':
    options = _.create(options, { optional: true });
    return new SparqlGroupsIterator(source, group.patterns, options);
  case 'union':
    return new UnionIterator(group.patterns.map(function (patternToken) {
      return new SparqlGroupIterator(source.clone(), patternToken, childOptions);
    }), options);
  case 'filter':
    // An set of bindings matches the filter if it doesn't evaluate to 0 or false
    var evaluate = SparqlExpressionEvaluator(group.expression);
    return new FilterIterator(source, function (bindings) {
      return !/^"false"|^"0"/.test(evaluate(bindings));
    }, options);
  default:
    throw new Error('Unsupported group type: ' + group.type);
  }
}
Iterator.inherits(SparqlGroupIterator);



// Error thrown when the query has a syntax error
function InvalidQueryError(query, cause) {
  this.name = 'InvalidQueryError';
  this.query = query;
  this.cause = cause;
  this.message = 'Syntax error in query\n' + cause.message;
}
util.inherits(InvalidQueryError, Error);

// Error thrown when no combination of iterators can solve the query
function UnsupportedQueryError(query, cause) {
  this.name = 'UnsupportedQueryError';
  this.query = query;
  this.cause = cause;
  this.message = 'The query is not yet supported\n' + cause.message;
}
util.inherits(UnsupportedQueryError, Error);



module.exports = SparqlIterator;
SparqlIterator.InvalidQueryError = InvalidQueryError;
SparqlIterator.UnsupportedQueryError = UnsupportedQueryError;
