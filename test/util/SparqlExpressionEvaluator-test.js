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
