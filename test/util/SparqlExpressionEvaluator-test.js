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

    describe('of an unsuppported expression type', function () {
      it('should throw an error', function () {
        (function () { SparqlExpressionEvaluator({ type: 'invalid' }); })
        .should.throw('Unsupported expression type: invalid');
      });
    });
  });
});
