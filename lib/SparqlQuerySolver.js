/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SparqlQuerySolver finds results to a SPARQL query. */

var SparqlQueryParser = require('./SparqlQueryParser'),
    log = new (require('./Logger'))('SparqlQuerySolver'),
    _ = require('underscore');

var VARIABLE_PREFIX_LENGTH = SparqlQueryParser.VARIABLE_PREFIX.length;

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
        return self._bindingsToRows(bindings, query.variables);
      });
    });
  },

  // Converts a hierarchical bindings structure to rows of bindings,
  // adding only variables that occur in the given list.
  _bindingsToRows: function (bindings, variables) {
    // Recursively flatten the bindings
    return (function flattenOptions(options, rows, currentRow) {
      // Go through all possible binding options
      options.forEach(function (option) {
        // Apply the bindings of the current options to the row
        var bindings = option.bindings;
        variables.forEach(function (variable) {
          if (variable in bindings)
            currentRow[variable.substr(VARIABLE_PREFIX_LENGTH)] = bindings[variable];
        });
        // If there are suboptions, iterate over them
        if (option.options)
          flattenOptions(option.options, rows, currentRow);
        // If not, the row is finished
        else
          rows.push(_.clone(currentRow));
      });
      return rows;
    })(bindings.options, [], {});
  },
};

module.exports = SparqlQuerySolver;
