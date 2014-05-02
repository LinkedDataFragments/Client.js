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
        operator: '+',
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
        operator: '-',
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
        operator: '*',
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
        operator: '/',
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
        operator: '=',
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
        operator: '!=',
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
        operator: '<',
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
        operator: '<=',
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
        operator: '>',
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
        operator: '>=',
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

    describe('of the not operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: '!',
        arguments: [
          {
            type: 'operator',
            operator: '=',
            arguments: [
              { type: 'variable', value: '?a' },
              { type: 'number',   value: 'a' },
            ]
          }
        ]
      });

      it('should return false if the child expression is true', function () {
        evaluator({ '?a': 'a' }).should.be.false;
      });

      it('should return true if the child expression is false', function () {
        evaluator({ '?a': 'b' }).should.be.true;
      });

      it('should throw an error if the argument is not a boolean', function () {
        var evaluator = SparqlExpressionEvaluator({
          type: 'operator',
          operator: '!',
          arguments: [ { type: 'number', value: 3 } ]
        });
        (function () { evaluator({ '?a': 'a' }); })
          .should.throw('! needs a boolean but got: 3.');
      });
    });

    describe('of the and operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'and',
        arguments: [
          { type: 'variable', value: '?a' },
          { type: 'variable', value: '?b' },
        ]
      });

      it('should return false with arguments false, false', function () {
        evaluator({ '?a': false, '?b': false }).should.be.false;
      });


      it('should return false with arguments true, false', function () {
        evaluator({ '?a': true,  '?b': false }).should.be.false;
      });


      it('should return false with arguments false, true', function () {
        evaluator({ '?a': false, '?b': true  }).should.be.false;
      });


      it('should return true with arguments true, true', function () {
        evaluator({ '?a': true,  '?b': true  }).should.be.true;
      });

      it('should throw an error if the first argument is not a boolean', function () {
        (function () { evaluator({ '?a': 'a', '?b': false }); })
          .should.throw('and needs a boolean but got: a.');
      });

      it('should throw an error if the second argument is not a boolean', function () {
        (function () { evaluator({ '?a': true, '?b': 'b' }); })
          .should.throw('and needs a boolean but got: b.');
      });
    });

    describe('of the or operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'or',
        arguments: [
          { type: 'variable', value: '?a' },
          { type: 'variable', value: '?b' },
        ]
      });

      it('should return false with arguments false, false', function () {
        evaluator({ '?a': false, '?b': false }).should.be.false;
      });


      it('should return true with arguments true, false', function () {
        evaluator({ '?a': true,  '?b': false }).should.be.true;
      });


      it('should return true with arguments false, true', function () {
        evaluator({ '?a': false, '?b': true  }).should.be.true;
      });


      it('should return true with arguments true, true', function () {
        evaluator({ '?a': true,  '?b': true  }).should.be.true;
      });

      it('should throw an error if the first argument is not a boolean', function () {
        (function () { evaluator({ '?a': 'a', '?b': false }); })
          .should.throw('or needs a boolean but got: a.');
      });

      it('should throw an error if the second argument is not a boolean', function () {
        (function () { evaluator({ '?a': true, '?b': 'b' }); })
          .should.throw('or needs a boolean but got: b.');
      });
    });

    describe('of the lang operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'lang',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
        ]
      });

      it('should return the lowercase language of a string', function () {
        evaluator({ '?a': '"hello"@EN' }).should.equal('"en"');
      });
    });

    describe('of the langmatches operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'langmatches',
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

    describe('of the regex operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'regex',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          },
          {
            type: 'literal',
            value: '"a+b"',
          }
        ]
      });

      it('should return true if the argument matches the regular expression', function () {
        evaluator({ '?a': 'aaaaaab' }).should.be.true;
      });

      it("should return false if the argument doesn't match the regular expression", function () {
        evaluator({ '?a': 'bbbb' }).should.be.false;
      });

      it('should throw an error if the argument is not a variable', function () {
        var evaluator = SparqlExpressionEvaluator({
          type: 'operator',
          operator: 'bound',
          arguments: [ { type: 'number' } ]
        });
        (function () { evaluator({ '?a': 'a' }); })
          .should.throw('BOUND expects a variable but got: number.');
      });
    });

    describe('of the str operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'str',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          }
        ]
      });

      it('should return the literal if passed a literal', function () {
        evaluator({ '?a': '"a"' }).should.equal('"a"');
      });

      it("should return a stringified version if passed a number", function () {
        evaluator({ '?a': 3 }).should.equal('"3"');
      });
    });

    describe('of the xsd:double operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'http://www.w3.org/2001/XMLSchema#double',
        arguments: [
          {
            type: 'literal',
            value: '"123"',
          }
        ]
      });

      it('should return the literal as a double', function () {
        evaluator({}).should.equal('"123.0"^^<http://www.w3.org/2001/XMLSchema#double>');
      });
    });

    describe('of the bound operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operator',
        operator: 'bound',
        arguments: [
          {
            type: 'variable',
            value: '?a',
          }
        ]
      });

      it('should return true if the variable is bound', function () {
        evaluator({ '?a': 'a' }).should.be.true;
      });

      it('should return false if the variable is not bound', function () {
        evaluator({ '?b': 'b' }).should.be.false;
      });

      it('should throw an error if the argument is not a variable', function () {
        var evaluator = SparqlExpressionEvaluator({
          type: 'operator',
          operator: 'bound',
          arguments: [ { type: 'number' } ]
        });
        (function () { evaluator({ '?a': 'a' }); })
          .should.throw('BOUND expects a variable but got: number.');
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
            operator: '+',
            arguments: [1, 2, 3],
          });
        })
        .should.throw('Invalid number of arguments for +: 3 (expected: 2).');
      });
    });
  });
});

describe('SparqlExpressionEvaluator.evaluate', function () {
  it('should return the evaluation of an expression for the given bindings', function () {
    SparqlExpressionEvaluator.evaluate({
      type: 'operator',
      operator: '+',
      arguments: [{
          type: 'variable',
          value: '?a',
        },
        {
          type: 'variable',
          value: '?b',
        }
      ]
    },
    {
      '?a': 1,
      '?b': 2,
    })
    .should.equal(3);
  });

  it('should throw an error when not all bindings are present', function () {
    (function () {
      SparqlExpressionEvaluator.evaluate({
        type: 'operator',
        operator: '+',
        arguments: [{
            type: 'variable',
            value: '?a',
          },
          {
            type: 'variable',
            value: '?b',
          }
        ]
      },
      {
        '?a': 1,
      });
    }).should.throw('Cannot evaluate variable ?b because it is not bound.');
  });
});
