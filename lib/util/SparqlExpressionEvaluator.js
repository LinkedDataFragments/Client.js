/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SparqlExpressionEvaluator evaluates SPARQL expressions into values. */

// Creates a function that evaluates the given expression
function SparqlExpressionEvaluator(expression) {
  if (!expression) return noop;
  var evaluator = evaluators[expression ? expression.type : null];
  if (!evaluator) throw new Error('Unsupported expression type: ' + expression.type);
  return evaluator(expression);
}

// The null operation
function noop () { }

// Creates an identity function for the given value
function createIdentityFunction(value) {
  return function () { return value; };
}

// Evaluators for each of the expression types
var evaluators = {
  // Does nothing
  null: function () { return noop; },

  // Evaluates a constant number
  number:   function (expression) { return createIdentityFunction(expression.value); },

  // Evaluates a constant literal
  literal:  function (expression) { return createIdentityFunction(expression.value); },

  // Evaluates a variable
  variable: function (expression) {
    return (function (varName) {
      return function (bindings) {
        if (!bindings || !(varName in bindings))
          throw new Error('Cannot evaluate variable ' + varName + ' because it is not bound.');
        return bindings[varName];
      };
    })(expression.value);
  }
};

module.exports = SparqlExpressionEvaluator;
