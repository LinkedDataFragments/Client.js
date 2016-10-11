/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
var SparqlExpressionEvaluator = require('../../lib/util/SparqlExpressionEvaluator');

var TRUE =  '"true"^^http://www.w3.org/2001/XMLSchema#boolean';
var FALSE = '"false"^^http://www.w3.org/2001/XMLSchema#boolean';

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

    describe('of a literal', function () {
      var evaluator = SparqlExpressionEvaluator('"abc"');
      it('should return the literal value', function () {
        evaluator({ '?a': 'a' }).should.equal('"abc"');
      });
    });

    describe('of a numeric literal', function () {
      var evaluator = SparqlExpressionEvaluator('"43"^^http://www.w3.org/2001/XMLSchema#integer');
      it('should return the literal value', function () {
        evaluator({ '?a': 'a' }).should.equal('"43"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of a variable', function () {
      var evaluator = SparqlExpressionEvaluator('?a');
      it('should return the variable\'s value if it is bound', function () {
        evaluator({ '?a': '"x"' }).should.equal('"x"');
      });
      it('should return undefined if the variable is not bound', function () {
        expect(evaluator({ '?b': 'b' })).to.be.undefined;
      });
    });

    describe('of a sum', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '+',
        args: [
          '?a',
          '"2"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });
      it('should return the sum of the expressions', function () {
        evaluator({ '?a': '"3"^^http://www.w3.org/2001/XMLSchema#integer' })
          .should.equal('"5"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of a difference', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '-',
        args: [
          '"5"^^http://www.w3.org/2001/XMLSchema#integer',
          '?a',
        ],
      });
      it('should return the difference of the expressions', function () {
        evaluator({ '?a': '"3"^^http://www.w3.org/2001/XMLSchema#integer' })
          .should.equal('"2"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of a product', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '*',
        args: [
          '?a',
          '"2"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });
      it('should return the product of the expressions', function () {
        evaluator({ '?a': '"3"^^http://www.w3.org/2001/XMLSchema#integer' })
          .should.equal('"6"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of a quotient', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '/',
        args: [
          '"6"^^http://www.w3.org/2001/XMLSchema#integer',
          '?a',
        ],
      });
      it('should return the quotient of the expressions', function () {
        evaluator({ '?a': '"2"^^http://www.w3.org/2001/XMLSchema#integer' })
          .should.equal('"3"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of an equality comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '=',
        args: ['?a', '"b"'],
      });

      it('should return true if the arguments match', function () {
        evaluator({ '?a': '"b"' }).should.equal.true;
      });

      it("should return false if the arguments don't match", function () {
        evaluator({ '?a': '"c"' }).should.equal.false;
      });
    });

    describe('of a non-equality comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '=',
        args: ['?a', '"b"'],
      });

      it('should return false if the arguments match', function () {
        evaluator({ '?a': '"b"' }).should.equal.false;
      });

      it("should return true if the arguments don't match", function () {
        evaluator({ '?a': '"c"' }).should.equal.true;
      });
    });

    describe('of a less-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '<',
        args: [
          '?a',
          '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });

      it('should return true if a < b', function () {
        evaluator({ '?a': '"3"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal(TRUE);
      });

      it('should return false if a == b', function () {
        evaluator({ '?a': '"20"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if a > b', function () {
        evaluator({ '?a': '"120"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of a less-or-equal-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '<=',
        args: [
          '?a',
          '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });

      it('should return true if a < b', function () {
        evaluator({ '?a': '"3"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal(TRUE);
      });

      it('should return true if a == b', function () {
        evaluator({ '?a': '"20"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal(TRUE);
      });

      it('should return false if a > b', function () {
        evaluator({ '?a': '"120"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of a greater-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '>',
        args: [
          '?a',
          '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });

      it('should return false if a < b', function () {
        evaluator({ '?a': '"3"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal(FALSE);
      });

      it('should return false if a == b', function () {
        evaluator({ '?a': '"20"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return true if a > b', function () {
        evaluator({ '?a': '"120"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of a greater-or-equal-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '>=',
        args: [
          '?a',
          '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });

      it('should return false if a < b', function () {
        evaluator({ '?a': '"3"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal(FALSE);
      });

      it('should return true if a == b', function () {
        evaluator({ '?a': '"20"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal(TRUE);
      });

      it('should return true if a > b', function () {
        evaluator({ '?a': '"120"^^http://www.w3.org/2001/XMLSchema#integer' }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of the not operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '!',
        args: [
          {
            type: 'operation',
            operator: '=',
            args: ['?a', '"a"'],
          },
        ],
      });

      it('should return false if the child expression is true', function () {
        evaluator({ '?a': '"a"' }).should.equal(FALSE);
      });

      it('should return true if the child expression is false', function () {
        evaluator({ '?a': '"b"' }).should.equal(TRUE);
      });

      it('should return false on non-boolean arguments', function () {
        var evaluator = SparqlExpressionEvaluator({
          type: 'operation',
          operator: '!',
          args: ['"3"^^http://www.w3.org/2001/XMLSchema#integer'],
        });
        evaluator({ '?a': '"a"' }).should.equal(FALSE);
      });
    });

    describe('of the and operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '&&',
        args: ['?a', '?b'],
      });

      it('should return false with arguments false, false', function () {
        evaluator({ '?a': FALSE, '?b': FALSE }).should.equal(FALSE);
      });


      it('should return false with arguments true, false', function () {
        evaluator({ '?a': TRUE,  '?b': FALSE }).should.equal(FALSE);
      });


      it('should return false with arguments false, true', function () {
        evaluator({ '?a': FALSE, '?b': TRUE  }).should.equal(FALSE);
      });


      it('should return true with arguments true, true', function () {
        evaluator({ '?a': TRUE,  '?b': TRUE  }).should.equal(TRUE);
      });

      it('should treat the first argument as true if it is non-boolean', function () {
        evaluator({ '?a': 'a', '?b': TRUE }).should.equal(TRUE);
      });

      it('should treat the second argument as true if it is non-boolean', function () {
        evaluator({ '?a': TRUE, '?b': 'a' }).should.equal(TRUE);
      });
    });

    describe('of the or operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: '||',
        args: ['?a', '?b'],
      });

      it('should return false with arguments false, false', function () {
        evaluator({ '?a': FALSE, '?b': FALSE }).should.equal(FALSE);
      });


      it('should return true with arguments true, false', function () {
        evaluator({ '?a': TRUE,  '?b': FALSE }).should.equal(TRUE);
      });


      it('should return true with arguments false, true', function () {
        evaluator({ '?a': FALSE, '?b': TRUE  }).should.equal(TRUE);
      });


      it('should return true with arguments true, true', function () {
        evaluator({ '?a': TRUE,  '?b': TRUE  }).should.equal(TRUE);
      });

      it('should treat the first argument as true if it is non-boolean', function () {
        evaluator({ '?a': 'a', '?b': FALSE }).should.equal(TRUE);
      });

      it('should treat the second argument as true if it is non-boolean', function () {
        evaluator({ '?a': FALSE, '?b': 'a' }).should.equal(TRUE);
      });
    });

    describe('of the lang operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: 'lang',
        args: ['?a'],
      });

      it('should return the lowercase language of a string', function () {
        evaluator({ '?a': '"hello"@EN' }).should.equal('"en"');
      });
    });

    describe('of the langmatches operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: 'langmatches',
        args: ['?l', '"EN"'],
      });

      it('should return true if the language matches', function () {
        evaluator({ '?l': '"en"' }).should.equal(TRUE);
      });

      it("should return false if the language doesn't match", function () {
        evaluator({ '?l': '"fr"' }).should.equal(FALSE);
      });
    });

    describe('of the CONTAINS operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: 'contains',
        args: [
          '"defgh"',
          '?a',
        ],
      });

      it('should return true if the substring is part of the string', function () {
        evaluator({ '?a': '"efg"' }).should.equal(TRUE);
      });

      it('should return true if the substring is equal to the string', function () {
        evaluator({ '?a': '"defgh"^^<urn:type>' }).should.equal(TRUE);
      });

      it('should return false if the substring is not part of the string', function () {
        evaluator({ '?a': '"abc"' }).should.equal(FALSE);
      });
    });

    describe('of the regex operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: 'regex',
        args: ['?a', '"a+b"'],
      });

      it('should return true if the argument matches the regular expression', function () {
        evaluator({ '?a': '"aaaaaab"' }).should.equal(TRUE);
      });

      it("should return false if the argument doesn't match the regular expression", function () {
        evaluator({ '?a': '"bbbb"' }).should.equal(FALSE);
      });
    });

    describe('of the str operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: 'str',
        args: ['?a'],
      });

      it('should return the literal if passed a literal', function () {
        evaluator({ '?a': '"a"' }).should.equal('"a"');
      });

      it('should return a stringified version if passed a number', function () {
        evaluator({ '?a': '"3"^^http://www.w3.org/2001/XMLSchema#double' })
          .should.equal('"3"^^http://www.w3.org/2001/XMLSchema#double');
      });
    });

    describe('of the xsd:integer function', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'functionCall',
        operator: 'http://www.w3.org/2001/XMLSchema#integer',
        args: ['"123.67"'],
      });

      it('should return the literal as an integer', function () {
        evaluator({}).should.equal('"123"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of the xsd:double function', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'functionCall',
        operator: 'http://www.w3.org/2001/XMLSchema#double',
        args: ['"123"'],
      });

      it('should return the literal as a double', function () {
        evaluator({}).should.equal('"123.0"^^http://www.w3.org/2001/XMLSchema#double');
      });
    });

    describe('of the bound operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: 'bound',
        args: ['?a'],
      });

      it('should return true if the variable is bound', function () {
        evaluator({ '?a': 'a' }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if the variable is not bound', function () {
        evaluator({ '?b': 'b' }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should throw an error if the argument is not a variable', function () {
        var evaluator = SparqlExpressionEvaluator({
          type: 'operation',
          operator: 'bound',
          args: ['"a"'],
        });
        (function () { evaluator({ '?a': 'a' }); })
          .should.throw('BOUND expects a variable but got: "a"');
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
        (function () { SparqlExpressionEvaluator({ type: 'operation', operator: 'invalid' }); })
          .should.throw('Unsupported operator: invalid.');
      });
    });

    describe('of an operator with an incorrect number of arguments', function () {
      it('should throw an error', function () {
        (function () { SparqlExpressionEvaluator({ type: 'operation', operator: 'regex', args: [1] }); })
          .should.throw('Invalid number of arguments for regex: 1 (expected: 2).');
      });
    });

    describe('of an operator with an invalid number of arguments', function () {
      it('should throw an error', function () {
        (function () {
          SparqlExpressionEvaluator({
            type: 'operation',
            operator: '+',
            args: ['"a"', '"b"', '"c"'],
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
      type: 'operation',
      operator: '+',
      args: ['?a', '?b'],
    }, {
      '?a': '"1"^^http://www.w3.org/2001/XMLSchema#integer',
      '?b': '"2"^^http://www.w3.org/2001/XMLSchema#integer',
    })
    .should.equal('"3"^^http://www.w3.org/2001/XMLSchema#integer');
  });

  it('should return undefined when not all bindings are present', function () {
    expect(SparqlExpressionEvaluator.evaluate({
      type: 'operation',
      operator: '+',
      args: ['?a', '?b'],
    }, {
      '?a': '"1"^^http://www.w3.org/2001/XMLSchema#integer',
    }))
    .to.be.undefined;
  });
});
