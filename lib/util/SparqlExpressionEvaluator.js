/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A SparqlExpressionEvaluator evaluates SPARQL expressions into values. */

var N3Util = require('n3').Util;

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
  },

  // Evaluates an operator
  operator: function (expression) {
    // Find the operator and check the number of arguments matches the expression
    var operator = operators[expression.operator];
    if (!operator)
      throw new Error('Unsupported operator: ' + expression.operator + '.');
    if (operator.length !== expression.arguments.length)
      throw new Error('Invalid number of arguments for ' + expression.operator +
                      ': ' + expression.arguments.length +
                      ' (expected: ' + operator.length + ').');

    // Create expressions for each of the arguments
    var argumentExpressions = new Array(expression.arguments.length);
    for (var i = 0; i < expression.arguments.length; i++)
      argumentExpressions[i] = SparqlExpressionEvaluator(expression.arguments[i]);

    // Create a function that evaluates the operator with the arguments and bindings
    return (function (operator, argumentExpressions) {
      return function (bindings) {
        var args = new Array(argumentExpressions.length);
        for (var i = 0; i < argumentExpressions.length; i++)
          args[i] = argumentExpressions[i](bindings);
        return operator.apply(null, args);
      };
    })(operator, argumentExpressions);
  },
};

// Operators for each of the operator types
var operators = {
  // Calculates the sum of two numbers
  sum: function (a, b) {
    if (!isFinite(a)) a = parseFloat(N3Util.getLiteralValue(a));
    if (!isFinite(b)) b = parseFloat(N3Util.getLiteralValue(b));
    return a + b;
  },

  // Calculates the difference of two numbers
  difference: function (a, b) {
    if (!isFinite(a)) a = parseFloat(N3Util.getLiteralValue(a));
    if (!isFinite(b)) b = parseFloat(N3Util.getLiteralValue(b));
    return a - b;
  },

  // Calculates the product of two numbers
  product: function (a, b) {
    if (!isFinite(a)) a = parseFloat(N3Util.getLiteralValue(a));
    if (!isFinite(b)) b = parseFloat(N3Util.getLiteralValue(b));
    return a * b;
  },

  // Calculates the quotient of two numbers
  quotient: function (a, b) {
    if (!isFinite(a)) a = parseFloat(N3Util.getLiteralValue(a));
    if (!isFinite(b)) b = parseFloat(N3Util.getLiteralValue(b));
    return a / b;
  },
};

module.exports = SparqlExpressionEvaluator;
