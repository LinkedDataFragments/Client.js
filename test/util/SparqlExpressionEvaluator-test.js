/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
var SparqlExpressionEvaluator = require('../../lib/util/SparqlExpressionEvaluator');

describe('SparqlExpressionEvaluator', function () {
  describe('The SparqlExpressionEvaluator module', function () {
    it('should be a function', function () {
      SparqlExpressionEvaluator.should.be.a('function');
    });
  });

  describe('A SparqlExpressionEvaluator', function () {
    describe('of a falsy value', function () {
      it('should return undefined', function () {
        expect(SparqlExpressionEvaluator()()).to.be.undefined;
      });
    });

    describe('of a number', function () {
      var evaluator = SparqlExpressionEvaluator({ type: 'number', value: 43 });
      it('should return the numeric value', function () {
        evaluator({ '?a': 'a' }).should.equal(43);
      });
    });

    describe('of a literal', function () {
      var evaluator = SparqlExpressionEvaluator({ type: 'literal', value: '"abc"' });
      it('should return the numeric value', function () {
        evaluator({ '?a': 'a' }).should.equal('"abc"');
      });
    });

    describe('of a literal', function () {
      var evaluator = SparqlExpressionEvaluator({ type: 'literal', value: '"abc"' });
      it('should return the numeric value', function () {
        evaluator({ '?a': 'a' }).should.equal('"abc"');
      });
    });

    describe('of a variable', function () {
      var evaluator = SparqlExpressionEvaluator({ type: 'variable', value: '?a' });
      it("should return variable's value if it is bound", function () {
        evaluator({ '?a': 'a' }).should.equal('a');
      });
      it('should throw an error of the variable is not bound', function () {
        (function () { evaluator({ '?b': 'b' }); })
          .should.throw('Cannot evaluate variable ?a because it is not bound.');
      });
    });

    describe('of a sum', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'sum',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 2,
          }
        ]
      });
      it('should return the sum of the expressions', function () {
        evaluator({ '?a': '"3"' }).should.equal(5);
      });
    });

    describe('of a difference', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'difference',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 2,
          }
        ]
      });
      it('should return the difference of the expressions', function () {
        evaluator({ '?a': '"5"' }).should.equal(3);
      });
    });

    describe('of a product', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'product',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 2,
          }
        ]
      });
      it('should return the product of the expressions', function () {
        evaluator({ '?a': '"5"' }).should.equal(10);
      });
    });

    describe('of a quotient', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'quotient',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 2,
          }
        ]
      });
      it('should return the quotient of the expressions', function () {
        evaluator({ '?a': '"10"' }).should.equal(5);
      });
    });

    describe('of an equality comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'equals',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 'a',
          }
        ]
      });

      it('should return true if the arguments match', function () {
        evaluator({ '?a': 'a' }).should.equal.true;
      });

      it("should return false if the arguments don't match", function () {
        evaluator({ '?a': 'b' }).should.equal.false;
      });
    });

    describe('of a non-equality comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'notEquals',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 'a',
          }
        ]
      });

      it('should return false if the arguments match', function () {
        evaluator({ '?a': 'a' }).should.equal.false;
      });

      it("should return true if the arguments don't match", function () {
        evaluator({ '?a': 'b' }).should.equal.true;
      });
    });

    describe('of a less-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'lessThan',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 20,
          }
        ]
      });

      it('should return true if a < b', function () {
        evaluator({ '?a': '3' }).should.be.true;
      });

      it('should return false if a == b', function () {
        evaluator({ '?a': '20' }).should.be.false;
      });

      it('should return false if a > b', function () {
        evaluator({ '?a': '120' }).should.be.false;
      });
    });

    describe('of a less-or-equal-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'lessThanOrEqual',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 20,
          }
        ]
      });

      it('should return true if a < b', function () {
        evaluator({ '?a': '3' }).should.be.true;
      });

      it('should return true if a == b', function () {
        evaluator({ '?a': '20' }).should.be.true;
      });

      it('should return false if a > b', function () {
        evaluator({ '?a': '120' }).should.be.false;
      });
    });

    describe('of a greater-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'greaterThan',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 20,
          }
        ]
      });

      it('should return false if a < b', function () {
        evaluator({ '?a': '3' }).should.be.false;
      });

      it('should return false if a == b', function () {
        evaluator({ '?a': '20' }).should.be.false;
      });

      it('should return true if a > b', function () {
        evaluator({ '?a': '120' }).should.be.true;
      });
    });

    describe('of a greater-or-equal-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'greaterThanOrEqual',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'number',
            value: 20,
          }
        ]
      });

      it('should return false if a < b', function () {
        evaluator({ '?a': '3' }).should.be.false;
      });

      it('should return true if a == b', function () {
        evaluator({ '?a': '20' }).should.be.true;
      });

      it('should return true if a > b', function () {
        evaluator({ '?a': '120' }).should.be.true;
      });
    });

    describe('of the getLanguage operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'getLanguage',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
        ]
      });

      it('should return the lowercase language of a string', function () {
        evaluator({ '?a': '"hello"@EN' }).should.equal('en');
      });
    });

    describe('of the languageMatches operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'languageMatches',
        arguments: [
          {
            type: 'variable',
            value: '?l',
          },
          {
            type: 'literal',
            value: '"EN"',
          }
        ]
      });

      it('should return true if the language matches', function () {
        evaluator({ '?l': '"hello"@en' }).should.be.true;
      });

      it("should return false if the language doesn't match", function () {
        evaluator({ '?l': '"bonjour"@fr' }).should.be.false;
      });
    });

    describe('of an unsuppported expression type', function () {
      it('should throw an error', function () {
        (function () { SparqlExpressionEvaluator({ type: 'invalid' }); })
        .should.throw('Unsupported expression type: invalid');
      });
    });

    describe('of an unsupported operator', function () {
      it('should throw an error', function () {
        (function () { SparqlExpressionEvaluator({ type: 'operator', operator: 'invalid' }); })
          .should.throw('Unsupported operator: invalid.');
      });
    });

    describe('of an operator with an invalid number of arguments', function () {
      it('should throw an error', function () {
        (function () {
          SparqlExpressionEvaluator({
            type: 'operator',
            operator: 'sum',
            arguments: [1, 2, 3],
          });
        })
        .should.throw('Invalid number of arguments for sum: 3 (expected: 2).');
      });
    });
  });
});
