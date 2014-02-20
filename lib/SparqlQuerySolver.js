/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SparqlQuerySolver finds results to a SPARQL query. */

var SparqlQueryParser = require('./SparqlQueryParser'),
    log = new (require('./Logger'))('SparqlQuerySolver');

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
      return self._generator.getBindings(query.wherePattern);
    });
  },
};

module.exports = SparqlQuerySolver;
