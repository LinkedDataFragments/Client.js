/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SparqlQuerySolver finds results to a SPARQL query. */

var SparqlQueryParser = require('./SparqlQueryParser'),
    log = new (require('./Logger'))('SparqlQuerySolver'),
    _ = require('underscore');

// Creates a new SparqlQuerySolver
function SparqlQuerySolver(bindingsGenerator, prefixes) {
  var self = this;
  this._generator = bindingsGenerator;
  this._parser = new SparqlQueryParser(prefixes);
}

SparqlQuerySolver.prototype = {
  // Gets a promise for the results of the given SPARQL query
  getQueryResults: function (sparql) {
    var self = this;
    log.info('Received query:', sparql);

    // Parse the query
    return this._parser.parse(sparql)
    // Get the possible bindings for the WHERE pattern
    .then(function (query) {
      return self._generator.getBindings(query.wherePattern)
      .then(function (bindings) {
        var rows = self._bindingsToRows(bindings, query.variables);
        if (query.type === 'SELECT')
          return { rows: rows };
        if (query.type === 'CONSTRUCT')
          return { triples: self._rowsToTriples(rows, query.constructPattern) };
      });
    });
  },

  // Converts a hierarchical bindings structure to rows of bindings,
  // adding only variables that occur in the given list.
  _bindingsToRows: function (bindings, variables) {
    var rows = [], currentRow = {};
    // Recursively flatten the bindings
    (function flattenOptions(options) {
      // Go through all possible binding options
      options.forEach(function (option) {
        // Apply the bindings of the current options to the row
        var bindings = option.bindings;
        variables.forEach(function (variable) {
          if (variable in bindings)
            currentRow[variable] = bindings[variable];
        });
        // If there are suboptions, iterate over them
        if (option.options)
          flattenOptions(option.options);
        // If not, the row is finished
        else
          rows.push(_.clone(currentRow));
      });
    })(bindings.options || []);
    return rows;
  },

  // Converts the variable rows to triples using the given graph pattern.
  _rowsToTriples: function (rows, graphPattern) {
    var triples = [];
    rows.forEach(function (row) {
      graphPattern.forEach(function (pattern) {
        var triple = {};
        triple.subject   = pattern.subject   in row ? row[pattern.subject]   : pattern.subject;
        triple.predicate = pattern.predicate in row ? row[pattern.predicate] : pattern.predicate;
        triple.object    = pattern.object    in row ? row[pattern.object]    : pattern.object;
        triples.push(triple);
      });
    });
    return triples;
  },
};

module.exports = SparqlQuerySolver;
