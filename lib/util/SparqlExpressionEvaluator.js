/*! @license MIT ©2014-2016 Ruben Verborgh, Miel Vander Sande, Ghent University - imec */

var N3Util = require('n3').Util,
    UUID = require('uuid'),
    crypto = require('cryptojs');

var XSD = 'http://www.w3.org/2001/XMLSchema#',
    XSD_BOOLEAN = XSD + 'boolean',
    XSD_STRING = XSD + 'string',
    XSD_DATETIME = XSD + 'dateTime',
    XSD_TRUE = '"true"^^' + XSD_BOOLEAN,
    XSD_FALSE = '"false"^^' + XSD_BOOLEAN;

var operators, evaluators;

// Add all numeric values
var numericTypes = [
  'integer',
  'decimal',
  'float',
  'double',
  'nonPositiveInteger',
  'negativeInteger',
  'long',
  'int',
  'short',
  'byte',
  'nonNegativeInteger',
  'unsignedLong',
  'unsignedInt',
  'unsignedShort',
  'unsignedByte',
  'positiveInteger',
].map(function (type) {
  return XSD + type;
});
// value must remain constant to in the query execution for spec compliancy
var now = new Date();

// The null operation
function noop() {}

/**
 * Creates a function that evaluates the given SPARQL expression.
 * @constructor
 * @param expression a SPARQL expression
 * @returns {Function} a function that evaluates the SPARQL expression.
 */
function SparqlExpressionEvaluator(expression) {
  if (!expression) return noop;
  var expressionType = expression && expression.type || typeof expression,
      evaluator = evaluators[expressionType];
  if (!evaluator) throw new Error('Unsupported expression type: ' + expressionType);
  return evaluator(expression);
}

// Evaluates the expression with the given bindings
SparqlExpressionEvaluator.evaluate = function (expression, bindings) {
  return new SparqlExpressionEvaluator(expression)(bindings);
};

// Evaluators for each of the expression types
evaluators = {
  // Does nothing
  null: function () {
    return noop;
  },

  // Evaluates an IRI, literal, or variable
  string: function (expression) {
    // Evaluate a IRIs or literal to its own value
    if (expression[0] !== '?') {
      return function () {
        return expression;
      };
    }
    // Evaluate a variable to its value
    else {
      return function (bindings) {
        if (!bindings || !(expression in bindings))
          throw new Error('Cannot evaluate variable ' + expression + ' because it is not bound.');
        return bindings[expression];
      };
    }
  },

  // Evaluates an operation
  operation: function (expression) {
    // Find the operator and check the number of arguments matches the expression
    var operatorName = expression.operator,
        operator = operators[operatorName];
    if (!operator)
      throw new Error('Unsupported operator: ' + operatorName.toUpperCase() + '.');
    if (operator.argBounds) {
      var min = operator.argBounds.min, max = operator.argBounds.max;
      if ((min && max && (min > expression.args.length || max < expression.args.length)) ||
        (min && (min > expression.args.length)) ||
        (max && (max < expression.args.length))) {
        throw new Error('Invalid number of arguments for ' + operatorName.toUpperCase() +
          ': ' + expression.args.length +
          ' (expected between bounds: ' + JSON.stringify(operator.argBounds) + ').');
      }
    }
    else if (operator.length !== expression.args.length) {
      throw new Error('Invalid number of arguments for ' + operatorName.toUpperCase() +
        ': ' + expression.args.length +
        ' (expected: ' + operator.length + ').');
    }
    // Special case: some operators accept expressions instead of evaluated expressions
    if (operator.acceptsExpressions) {
      return (function (operator, args) {
        return function (bindings) {
          return operator.apply(bindings, args);
        };
      })(operator, expression.args);
    }

    // Parse the expressions for each of the arguments
    var argumentExpressions = new Array(expression.args.length);
    for (var i = 0; i < expression.args.length; i++)
      argumentExpressions[i] = new SparqlExpressionEvaluator(expression.args[i]);

    // Create a function that evaluates the operator with the arguments and bindings
    return (function (operator, argumentExpressions) {
      return function (bindings) {
        // Evaluate the arguments
        var args = new Array(argumentExpressions.length),
            origArgs = new Array(argumentExpressions.length);
        for (var i = 0; i < argumentExpressions.length; i++) {
          var arg = args[i] = origArgs[i] = argumentExpressions[i](bindings);
          // Convert the arguments if necessary
          switch (operator.type) {
          case 'numeric':
            if (!isNumeric(arg)) throw Error('Argument for ' + arg + ' must be numeric');
            args[i] = parseFloat(N3Util.getLiteralValue(arg));
            break;
          case 'boolean':
            args[i] = arg !== XSD_FALSE &&
              (!N3Util.isLiteral(arg) || N3Util.getLiteralValue(arg) !== '0');
            break;
          case 'dateTime':
            if (!isDatetime(arg)) throw Error('Argument for ' + arg + ' must be of type xsd:datetime');
            args[i] = new Date(N3Util.getLiteralValue(arg));
            break;
          case 'string-literal':
            if (!isStringLiteral(arg)) throw Error('Argument ' + arg + ' must be a string literal');
            break;
          case 'simple-literal-or-string':
            if (!isSimpleLiteralOrString(arg)) throw Error('Argument ' + arg + ' must be of type xsd:string or a simpleliteral');
            args[i] = N3Util.getLiteralValue(arg);
            break;

          }
        }
        // Call the operator on the evaluated arguments
        var result = operator.apply(null, args);
        // Convert result if necessary
        switch (operator.resultType) {
        case 'numeric':
          // TODO: determine type instead of taking the type of the first argument
          var type = isNumeric(origArgs[0]) ? N3Util.getLiteralType(origArgs[0]) : XSD + 'integer';
          return '"' + result + '"^^' + type;
        case 'boolean':
          return result ? XSD_TRUE : XSD_FALSE;
        case 'string':
          return '"' + result + '"^^' + XSD_STRING;
        default:
          return result;
        }
      };
    })(operator, argumentExpressions);
  },
};

// Operators for each of the operator types
operators = {
  // 17.4.1 Functional Forms
  '+': function (a, b) {
    return a + b;
  },
  '-': function (a, b) {
    return a - b;
  },
  '*': function (a, b) {
    return a * b;
  },
  '/': function (a, b) {
    return a / b;
  },
  '=': function (a, b) {
    return a === b;
  },
  '!=': function (a, b) {
    return a !== b;
  },
  '<': function (a, b) {
    return a < b;
  },
  '<=': function (a, b) {
    return a <= b;
  },
  '>': function (a, b) {
    return a > b;
  },
  '>=': function (a, b) {
    return a >= b;
  },
  '!': function (a) {
    return !a;
  },
  '&&': function (a, b) {
    return a && b;
  },
  '||': function (a, b) {
    return a || b;
  },
  'bound': function (a) {
    if (a[0] !== '?')
      throw new Error('BOUND expects a variable but got: ' + a);
    return a in this ? XSD_TRUE : XSD_FALSE;
  },
  'if': function (expression1, expression2, expression3) {
    return evaluators.operation(expression1) ? expression2 : expression3;
  },
  'coalesce': function (a, b) {
    return operators.bound.call(this, a) === XSD_TRUE ? this[a] : b;
  },
  'exists': function (a) {
    throw new Error('EXISTS not yet supported');
  },
  'not exists': function (a) {
    throw new Error('NOT EXISTS not yet supported');
  },
  'sameterm': function (a, b) {
    if (N3Util.isBlank(a) || N3Util.isBlank(b))
      return false;
    return a === b;
  },
  'in': function () {
    throw new Error('IN not yet supported');
  },
  'not in': function () {
    throw new Error('NOT IN not yet supported');
  },
  // 17.4.2 Functions on RDF Terms
  'isiri': function (a) {
    return N3Util.isIRI(a);
  },
  'isblank': function (a) {
    return N3Util.isBlank(a);
  },
  'isliteral': function (a) {
    return N3Util.isLiteral(a);
  },
  'isnumeric': isNumeric,
  'str': function (a) {
    return N3Util.isLiteral(a) ? '"' + N3Util.getLiteralValue(a) + '"' : '"' + a + '"';
  },
  'lang': function (a) {
    return '"' + N3Util.getLiteralLanguage(a).toLowerCase() + '"';
  },
  'datatype': function (a) {
    return N3Util.getLiteralType(a);
  },
  'iri': function (a) {
    if (N3Util.isIRI(a))
      return a;
    if (isStringLiteral(a)) {
      var val = N3Util.getLiteralValue(a);
      if (N3Util.isIRI(val)) return val;
    }
    throw new Error('IRI expects an simple literal, xsd:string or an IRI');
  },
  'uri': this.iri,
  'bnode': function (a) {
    throw new Error('BNODE not yet supported');
  },
  'strdt': function (lexicalForm, datatypeIRI) {
    if (!N3Util.isIRI(datatypeIRI))
      throw new Error('Datatype ' + datatypeIRI + ' is not a valid IRI');
    return '"' + lexicalForm + '"^^' + datatypeIRI;
  },
  'strlang': function (lexicalForm, langTag) {
    return '"' + lexicalForm + '"@' + langTag;
  },
  'uuid': function () {
    return 'urn:uuid:' + UUID.v4() + '';
  },
  'struuid': function () {
    return UUID.v4();
  },
  // 17.4.3 String functions
  'strlen': function (str) {
    return operators.strdt(N3Util.getLiteralValue(str)
      .length, XSD + 'integer');
  },
  'substr': function (str, startingLoc, length) {
    if (!isStringLiteral(str)) throw Error('Argument ' + str + 'for SUBSTR must be a string literal');
    if (!isInteger(startingLoc)) throw Error('Argument ' + startingLoc + ' for SUBSTR must be a string literal');

    var lexicalForm = N3Util.getLiteralValue(str)
      .substr(startingLoc, length);
    return constructLiteral(lexicalForm, str);
  },
  'ucase': function (str) {
    var lexicalForm = N3Util.getLiteralValue(str)
      .toUpperCase();
    return constructLiteral(lexicalForm, str);
  },
  'lcase': function (str) {
    var lexicalForm = N3Util.getLiteralValue(str)
      .toLowerCase();
    return constructLiteral(lexicalForm, str);
  },
  'strstarts': function (arg1, arg2) {
    if (!compatibleArguments(arg1, arg2))
      throw new Error('STRSTARTS requires compatible arguments');
    return N3Util.getLiteralValue(arg1).indexOf(N3Util.getLiteralValue(arg2)) === 0;
  },
  'strends': function (arg1, arg2) {
    if (!compatibleArguments(arg1, arg2))
      throw new Error('STRENDS requires compatible arguments');
    var a = N3Util.getLiteralValue(arg1),
        b = N3Util.getLiteralValue(arg2);
    return a.indexOf(b) === (a.length - b.length);
  },
  'contains': function (arg1, arg2) {
    if (!compatibleArguments(arg1, arg2))
      throw new Error('CONTAINS requires compatible arguments');
    return N3Util.getLiteralValue(arg1)
      .indexOf(N3Util.getLiteralValue(arg2)) > -1;
  },
  'strbefore': function (arg1, arg2) {
    if (!compatibleArguments(arg1, arg2))
      throw new Error('STRBEFORE requires compatible arguments');
    var index = N3Util.getLiteralValue(arg1)
      .indexOf(N3Util.getLiteralValue(arg2));
    var lexicalForm = index > -1 ? arg1.substr(index - 1, 1) : '';
    return constructLiteral(lexicalForm, arg1);
  },
  'strafter': function (arg1, arg2) {
    if (!compatibleArguments(arg1, arg2))
      throw new Error('STRAFTER requires compatible arguments');

    var a = N3Util.getLiteralValue(arg1),
        b = N3Util.getLiteralValue(arg2);

    var index = a.indexOf(b);
    var lexicalForm = b === '' ? a : (index > -1 && (index + b.length) < a.length ? a.substr(index + b.length, 1) : '');
    return constructLiteral(lexicalForm, arg1);
  },
  'encode_for_uri': function (str) {
    return '"' + encodeURI(N3Util.getLiteralValue(str)) + '"';
  },
  'concat': function () {
    var lexicalForm = '';
    var string = true,
        lang = null;
    arguments.forEach(function (arg) {
      lexicalForm += N3Util.getLiteralValue(arg);
      if (string)
        string = (N3Util.getLiteralType(arg) === XSD_STRING);

      if (lang === null)
        lang = N3Util.getLiteralLanguage(arg);

      if (lang !== false && lang !== N3Util.getLiteralLanguage(arg))
        lang = false;
    });

    if (lang) return operators.stlang(lexicalForm, lang);
    if (string) return operators.stdt(lexicalForm, XSD_STRING);

    return '"' + lexicalForm + '"';
  },
  'langmatches': function (a, b) {
    return a.toLowerCase() === b.toLowerCase();
  },
  'regex': function (subject, pattern, flags) {
    if (isStringLiteral(subject))
      subject = N3Util.getLiteralValue(subject);

    return new RegExp(N3Util.getLiteralValue(pattern), flags ? N3Util.getLiteralValue(flags) : undefined).test(subject);
  },
  'replace': function (subject, pattern, replacement, flags) {
    if (isStringLiteral(subject))
      subject = N3Util.getLiteralValue(subject);

    return subject.replace(N3Util.getLiteralValue(pattern), N3Util.getLiteralValue(replacement), flags ? N3Util.getLiteralValue(flags) : undefined);
  },
  // 17.4.4 Functions on Numerics
  'abs': function (term) {
    return Math.abs(term);
  },
  'round': function (term) {
    return Math.abs(term);
  },
  'ceil': function (term) {
    return Math.ceil(term);
  },
  'floor': function (term) {
    return Math.floor(term);
  },
  'rand': function () {
    return operators.stdt(Math.random(), XSD + 'double');
  },
  // 17.4.5 Functions on Dates and Times
  'now': function () {
    return operators.stdt(now.toISOString(), XSD_DATETIME);
  },
  'year': function (dt) {
    return dt.getYear();
  },
  'month': function (dt) {
    return dt.getMonth();
  },
  'day': function (dt) {
    return dt.getDay();
  },
  'hours': function (dt) {
    return dt.getHours();
  },
  'minutes': function (dt) {
    return dt.getMinutes();
  },
  'seconds': function (dt) {
    return operators.stdt(dt.getSeconds(), XSD + 'decimal');
  },
  'timezone': function (dt) {
    throw new Error('TIMEZONE not yet supported');
  },
  'tz': function (dt) {
    var s = N3Util.getLiteralValue(dt);
    return '"' + (s.match(/\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z)/)[0] || '') + '"';
  },
  // 17.4.6 Hash Functions
  'md5': function (arg) {
    return new crypto.MD5(arg);
  },
  'sha1': function (arg) {
    return new crypto.SHA1(arg);
  },
  'sha256': function (arg) {
    return new crypto.SHA256(arg);
  },
  'sha384': function (arg) {
    return new crypto.SHASHA3(arg);
  },
  'sha512': function (arg) {
    return new crypto.SHA512(arg);
  },
  // 17.5 XPath Constructor Functions
  'http://www.w3.org/2001/XMLSchema#integer': function (a) {
    if (isDatetime(a) || N3Util.isIRI(a))
      throw new Error('xsd:integer() does not allow argument of type xsd:datetime or IRI');
    var value = N3Util.getLiteralValue(a);
    a = parseInt(value, 10);
    return operators.strdt(a, XSD + 'integer');
  },
  'http://www.w3.org/2001/XMLSchema#decimal': function (a) {
    if (isDatetime(a) || N3Util.isIRI(a))
      throw new Error('xsd:decimal() does not allow argument of type xsd:datetime or IRI');
    return operators.strdt(constructFloat(a), XSD + 'decimal');
  },
  'http://www.w3.org/2001/XMLSchema#float': function (a) {
    if (isDatetime(a) || N3Util.isIRI(a))
      throw new Error('xsd:float() does not allow argument of type xsd:dateTime or IRI');
    return operators.strdt(constructFloat(a), XSD + 'float');
  },
  'http://www.w3.org/2001/XMLSchema#double': function (a) {
    if (isDatetime(a) || N3Util.isIRI(a))
      throw new Error('xsd:double() does not allow argument of type xsd:dateTime or IRI');
    return operators.strdt(constructFloat(a), XSD + 'double');
  },
  'http://www.w3.org/2001/XMLSchema#string': function (a) {
    operators.strdt(operators.str(a), XSD_STRING);
  },
  'http://www.w3.org/2001/XMLSchema#boolean': function (a) {
    if (isDatetime(a) || N3Util.isIRI(a))
      throw new Error('xsd:boolean() does not allow argument of type xsd:dateTime or IRI');
    var value = N3Util.getLiteralValue(a);

    return operators.strdt(Boolean(value), XSD_BOOLEAN);
  },
  'http://www.w3.org/2001/XMLSchema#dateTime': function (a) {
    if (!isDatetime(a) && !!isSimpleLiteralOrString(a))
      throw new Error('xsd:dateTime() only allows an argument of type xsd:datetime, xsd:string or simple literal');
    var value = new Date(N3Util.getLiteralValue(a));
    return operators.strdt(value.toISOString, XSD_DATETIME);
  },
};

// Set bounds for operators with dynamic argument length
operators.concat.argBounds = {
  min: 0,
};
operators.regex.argBounds = {
  min: 2,
  max: 3,
};
operators.replace.argBounds = {
  min: 3,
  max: 4,
};

// Tag all operators that expect their arguments to be numeric
[
  '+', '-', '*', '/', '<', '<=', '>', '>=',
  'abs', 'round', 'ceil', 'floor',
]
.forEach(function (operatorName) {
  operators[operatorName].type = 'numeric';
});

// Tag all operators that expect their arguments to be string literals (17.4.3.1.1)
[
  'strlen', 'ucase', 'lcase', 'strstarts', 'strends', 'contains', 'strbefore', 'strafter', 'encode_for_uri', 'concat',
].forEach(function (operatorName) {
  operators[operatorName].type = 'string-literal';
});

// Tag all operators that expect their arguments to be xsd:string or simple literals
[
  'md5',
  'sha1',
  'sha256',
  'sha384',
  'sha512',
].forEach(function (operatorName) {
  operators[operatorName].type = 'simple-literal-or-string';
});

// Tag all operators that expect their arguments to be boolean
[
  '!', '&&', '||',
].forEach(function (operatorName) {
  operators[operatorName].type = 'boolean';
});

// Tag all operators that expect their arguments to be xsd:dateTime
[
  'year', 'month', 'day', 'hours', 'minutes', 'seconds',
].forEach(function (operatorName) {
  operators[operatorName].type = 'dateTime';
});

// Tag all operators that have numeric results
[
  '+', '-', '*', '/', '<', '<=', '>', '>=', 'abs', 'round', 'ceil', 'floor',
  'year', 'month', 'day', 'hours', 'minutes',
].forEach(function (operatorName) {
  operators[operatorName].type = operators[operatorName].resultType = 'numeric';
});

// Tag all operators that have string results
[
  'md5',
  'sha1',
  'sha256',
  'sha384',
  'sha512',
].forEach(function (operatorName) {
  operators[operatorName].type = operators[operatorName].resultType = 'string';
});

// Tag all operators that have boolean results
[
  '!', '&&', '||', '=', '<', '<=', '>', '>=', 'langmatches', 'regex', 'bound',
  'sameterm', 'isiri', 'isliteral', 'isblank', 'isnumeric', 'strstarts', 'strends', 'contains',
].forEach(function (operatorName) {
  operators[operatorName].resultType = 'boolean';
});

// Tag all operators that take expressions instead of evaluated expressions
[
  'bound', 'coalesce',
].forEach(function (operatorName) {
  operators[operatorName].acceptsExpressions = true;
});

// Helper functions
function constructLiteral(lexicalForm, literal) {
  var lang = N3Util.getLiteralLanguage(literal),
      datatype = N3Util.getLiteralType(literal);
  if (lang)
    return operators.strlang(lexicalForm, lang);

  if (datatype)
    return operators.strdt(lexicalForm, datatype);

  return '"' + lexicalForm + '"';
}

// Check wether argument complies to 17.4.3.1.1 String arguments
function isString(a) {
  return N3Util.getLiteralType(a) === XSD_STRING;
}

function isSimpleLiteral(a) {
  return N3Util.isLiteral(a) && !N3Util.getLiteralType(a) && !N3Util.getLiteralLanguage(a);
}

function isSimpleLiteralOrString(a) {
  return isSimpleLiteral(a) || isString(a);
}

function isStringLiteral(a) {
  return N3Util.isLiteral(a) && (!N3Util.getLiteralType(a) || isString(a));
}

function isNumeric(a) {
  // TODO: fine grained datatype validation
  return N3Util.isLiteral(a) && numericTypes.indexOf(N3Util.getLiteralType(a)) > -1;
}

function isInteger(a) {
  return N3Util.getLiteralType(a) === XSD + 'integer=';
}

function isDatetime(a) {
  return N3Util.getLiteralType(a) === XSD_DATETIME;
}

function constructFloat(a) {
  var value = N3Util.getLiteralValue(a);
  a = parseFloat(value);

  if (isNaN(a))
    throw new Error(value + ' is not a valid value');

  a = a.toPrecision();
  if (a.indexOf('.') < 0) a += '.0';
  return a;
}

// check wether string arguments comply to '17.4.3.1.2 Argument Compatibility Rules'
function compatibleArguments(arg1, arg2) {
  if (!N3Util.isLiteral(arg1) || !N3Util.isLiteral(arg2)) return false;

  var l1 = N3Util.getLiteralLanguage(arg1),
      l2 = N3Util.getLiteralLanguage(arg2);

  return (l1 && (!l2 || l1 === l2)) || (!l1 && !l2);
}

module.exports = SparqlExpressionEvaluator;
