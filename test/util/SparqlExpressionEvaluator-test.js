/*! @license MIT ©2014-2016 Ruben Verborgh, Miel Vander Sande, Ghent University - imec */
var SparqlExpressionEvaluator = require('../../lib/util/SparqlExpressionEvaluator');

var TRUE = '"true"^^http://www.w3.org/2001/XMLSchema#boolean';
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
        evaluator({
          '?a' : 'a',
        }).should.equal('"abc"');
      });
    });

    describe('of a numeric literal', function () {
      var evaluator = SparqlExpressionEvaluator('"43"^^http://www.w3.org/2001/XMLSchema#integer');
      it('should return the literal value', function () {
        evaluator({
          '?a' : 'a',
        }).should.equal('"43"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of a variable', function () {
      var evaluator = SparqlExpressionEvaluator('?a');
      it("should return the variable's value if it is bound", function () {
        evaluator({
          '?a' : '"x"',
        }).should.equal('"x"');
      });
      it('should throw an error of the variable is not bound', function () {
        (function () {
          evaluator({
            '?b' : 'b',
          });
        })
        .should.throw('Cannot evaluate variable ?a because it is not bound.');
      });
    });

    describe('of a sum', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '+',
        args : [
          '?a',
          '"2"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });
      it('should return the sum of the expressions', function () {
        evaluator({
          '?a' : '"3"^^http://www.w3.org/2001/XMLSchema#integer',
        })
        .should.equal('"5"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of a difference', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '-',
        args : [
          '"5"^^http://www.w3.org/2001/XMLSchema#integer',
          '?a',
        ],
      });
      it('should return the difference of the expressions', function () {
        evaluator({
          '?a' : '"3"^^http://www.w3.org/2001/XMLSchema#integer',
        })
        .should.equal('"2"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of a product', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '*',
        args : [
          '?a',
          '"2"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });
      it('should return the product of the expressions', function () {
        evaluator({
          '?a' : '"3"^^http://www.w3.org/2001/XMLSchema#integer',
        })
        .should.equal('"6"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of a quotient', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '/',
        args : [
          '"6"^^http://www.w3.org/2001/XMLSchema#integer',
          '?a',
        ],
      });
      it('should return the quotient of the expressions', function () {
        evaluator({
          '?a' : '"2"^^http://www.w3.org/2001/XMLSchema#integer',
        })
        .should.equal('"3"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of an equality comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '=',
        args : [
          '?a',
          '"b"',
        ],
      });

      it('should return true if the arguments match', function () {
        evaluator({
          '?a' : '"b"',
        }).should.equal.true;
      });

      it("should return false if the arguments don't match", function () {
        evaluator({
          '?a' : '"c"',
        }).should.equal.false;
      });
    });

    describe('of a non-equality comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '=',
        args : [
          '?a',
          '"b"',
        ],
      });

      it('should return false if the arguments match', function () {
        evaluator({
          '?a' : '"b"',
        }).should.equal.false;
      });

      it("should return true if the arguments don't match", function () {
        evaluator({
          '?a' : '"c"',
        }).should.equal.true;
      });
    });

    describe('of a less-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '<',
        args : [
          '?a',
          '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });

      it('should return true if a < b', function () {
        evaluator({
          '?a' : '"3"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal(TRUE);
      });

      it('should return false if a == b', function () {
        evaluator({
          '?a' : '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if a > b', function () {
        evaluator({
          '?a' : '"120"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of a less-or-equal-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '<=',
        args : [
          '?a',
          '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });

      it('should return true if a < b', function () {
        evaluator({
          '?a' : '"3"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal(TRUE);
      });

      it('should return true if a == b', function () {
        evaluator({
          '?a' : '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal(TRUE);
      });

      it('should return false if a > b', function () {
        evaluator({
          '?a' : '"120"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of a greater-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '>',
        args : [
          '?a',
          '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });

      it('should return false if a < b', function () {
        evaluator({
          '?a' : '"3"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal(FALSE);
      });

      it('should return false if a == b', function () {
        evaluator({
          '?a' : '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return true if a > b', function () {
        evaluator({
          '?a' : '"120"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of a greater-or-equal-than comparison', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '>=',
        args : [
          '?a',
          '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });

      it('should return false if a < b', function () {
        evaluator({
          '?a' : '"3"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal(FALSE);
      });

      it('should return true if a == b', function () {
        evaluator({
          '?a' : '"20"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal(TRUE);
      });

      it('should return true if a > b', function () {
        evaluator({
          '?a' : '"120"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of the not operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '!',
        args : [{
          type : 'operation',
          operator : '=',
          args : [
            '?a',
            '"a"',
          ],
        },
        ],
      });

      it('should return false if the child expression is true', function () {
        evaluator({
          '?a' : '"a"',
        }).should.equal(FALSE);
      });

      it('should return true if the child expression is false', function () {
        evaluator({
          '?a' : '"b"',
        }).should.equal(TRUE);
      });

      it('should return false on non-boolean arguments', function () {
        var evaluator = SparqlExpressionEvaluator({
          type : 'operation',
          operator : '!',
          args : [
            '"3"^^http://www.w3.org/2001/XMLSchema#integer',
          ],
        });
        evaluator({
          '?a' : '"a"',
        }).should.equal(FALSE);
      });
    });

    describe('of the and operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '&&',
        args : [
          '?a',
          '?b',
        ],
      });

      it('should return false with arguments false, false', function () {
        evaluator({
          '?a' : FALSE,
          '?b' : FALSE,
        }).should.equal(FALSE);
      });

      it('should return false with arguments true, false', function () {
        evaluator({
          '?a' : TRUE,
          '?b' : FALSE,
        }).should.equal(FALSE);
      });

      it('should return false with arguments false, true', function () {
        evaluator({
          '?a' : FALSE,
          '?b' : TRUE,
        }).should.equal(FALSE);
      });

      it('should return true with arguments true, true', function () {
        evaluator({
          '?a' : TRUE,
          '?b' : TRUE,
        }).should.equal(TRUE);
      });

      it('should treat the first argument as true if it is non-boolean', function () {
        evaluator({
          '?a' : 'a',
          '?b' : TRUE,
        }).should.equal(TRUE);
      });

      it('should treat the second argument as true if it is non-boolean', function () {
        evaluator({
          '?a' : TRUE,
          '?b' : 'a',
        }).should.equal(TRUE);
      });
    });

    describe('of the or operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : '||',
        args : [
          '?a',
          '?b',
        ],
      });

      it('should return false with arguments false, false', function () {
        evaluator({
          '?a' : FALSE,
          '?b' : FALSE,
        }).should.equal(FALSE);
      });

      it('should return true with arguments true, false', function () {
        evaluator({
          '?a' : TRUE,
          '?b' : FALSE,
        }).should.equal(TRUE);
      });

      it('should return true with arguments false, true', function () {
        evaluator({
          '?a' : FALSE,
          '?b' : TRUE,
        }).should.equal(TRUE);
      });

      it('should return true with arguments true, true', function () {
        evaluator({
          '?a' : TRUE,
          '?b' : TRUE,
        }).should.equal(TRUE);
      });

      it('should treat the first argument as true if it is non-boolean', function () {
        evaluator({
          '?a' : 'a',
          '?b' : FALSE,
        }).should.equal(TRUE);
      });

      it('should treat the second argument as true if it is non-boolean', function () {
        evaluator({
          '?a' : FALSE,
          '?b' : 'a',
        }).should.equal(TRUE);
      });
    });

    describe('of the lang operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'lang',
        args : [
          '?a',
        ],
      });

      it('should return the lowercase language of a string', function () {
        evaluator({
          '?a' : '"hello"@EN',
        }).should.equal('"en"');
      });
    });

    describe('of the langmatches operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type: 'operation',
        operator: 'langmatches',
        args: ['"de-DE-1996"', '?l'],
      });

      it('should return true if the language is equal', function () {
        evaluator({ '?l': '"de-de-1996"' }).should.equal(TRUE);
      });

      it('should return true if the language has the same prefix', function () {
        evaluator({ '?l': '"de"' }).should.equal(TRUE);
        evaluator({ '?l': '"de-DE"' }).should.equal(TRUE);
      });

      it('should return true on *', function () {
        evaluator({ '?l': '"de-de-1996"' }).should.equal(TRUE);
      });

      it("should return false if the language doesn't match", function () {
        evaluator({
          '?l' : '"fr"',
        }).should.equal(FALSE);
      });
    });

    describe('of the regex operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'regex',
        args : [
          '?a',
          '"a+b"',
        ],
      });

      it('should return true if the argument matches the regular expression', function () {
        evaluator({
          '?a' : '"aaaaaab"',
        }).should.equal(TRUE);
      });

      it("should return false if the argument doesn't match the regular expression", function () {
        evaluator({
          '?a' : '"bbbb"',
        }).should.equal(FALSE);
      });
    });

    describe('of the str operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'str',
        args : [
          '?a',
        ],
      });

      it('should return the literal if passed a literal', function () {
        evaluator({
          '?a' : '"a"',
        }).should.equal('"a"');
      });

      it('should return a stringified version if passed a number', function () {
        evaluator({
          '?a' : '"3"^^http://www.w3.org/2001/XMLSchema#double',
        })
        .should.equal('"3"');
      });

      /* it('should throw error when non parseable value is passed', function () {
            evaluator({'?a': '"aaa"'}).should.throw('aaa is not a valid xsd:double');
      });*/
    });

    describe('of the xsd:double operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'http://www.w3.org/2001/XMLSchema#double',
        args : [
          '?a',
        ],
      });

      it('should return the literal as a double', function () {
        evaluator({
          '?a' : '"123"',
        }).should.equal('"123.0"^^http://www.w3.org/2001/XMLSchema#double');
      });

      it('should return the literal as a double', function () {
        evaluator({
          '?a' : '"1.23"',
        }).should.equal('"1.23"^^http://www.w3.org/2001/XMLSchema#double');
      });
    });

    describe('of the bound operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'bound',
        args : [
          '?a',
        ],
      });

      it('should return true if the variable is bound', function () {
        evaluator({
          '?a' : 'a',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if the variable is not bound', function () {
        evaluator({
          '?b' : 'b',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should throw an error if the argument is not a variable', function () {
        var evaluator = SparqlExpressionEvaluator({
          type : 'operation',
          operator : 'bound',
          args : [
            '"a"',
          ],
        });
        (function () {
          evaluator({
            '?a' : 'a',
          });
        })
        .should.throw('BOUND expects a variable but got: "a"');
      });
    });

    describe('of the COALESCE operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'coalesce',
        args : [
          '?a',
          '"3"^^http://www.w3.org/2001/XMLSchema#integer',
        ],
      });

      it('should return 2 if variable is bound', function () {
        evaluator({
          '?a' : '"2"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal('"2"^^http://www.w3.org/2001/XMLSchema#integer');
      });

      it('should return 3 if variable is not bound', function () {
        evaluator({}).should.equal('"3"^^http://www.w3.org/2001/XMLSchema#integer');
      });
    });

    describe('of the ISIRI operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'isiri',
        args : [
          '?a',
        ],
      });

      it('should return true if is uri', function () {
        evaluator({
          '?a' : 'mailto:alice@work.example',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if is literal', function () {
        evaluator({
          '?a' : '"alice@work.example"',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if is blank', function () {
        evaluator({
          '?a' : '_:b1',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of the ISBLANK operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'isblank',
        args : [
          '?a',
        ],
      });

      it('should return true if is blank', function () {
        evaluator({
          '?a' : '_:b1',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if is literal', function () {
        evaluator({
          '?a' : '"alice@work.example"',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if is iri', function () {
        evaluator({
          '?a' : 'http://example.org/a',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of the ISLITERAL operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'isliteral',
        args : [
          '?a',
        ],
      });

      it('should return true if is a literal', function () {
        evaluator({
          '?a' : '"alice"',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if is blank', function () {
        evaluator({
          '?a' : '_:b1',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

        // TODO: fine grained datatype validation
        /* it('should return false for "1200"^^xsd:byte', function () {
            evaluator({ '?a': '"1200"^^http://www.w3.org/2001/XMLSchema#byte' }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
        });*/

      it('should return false if is iri', function () {
        evaluator({
          '?a' : 'http://example.org/a',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of the ISNUMERIC operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'isnumeric',
        args : [
          '?a',
        ],
      });

      it('should return true for 12', function () {
        evaluator({
          '?a' : '"12"^^http://www.w3.org/2001/XMLSchema#integer',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false for "12"', function () {
        evaluator({
          '?a' : '"12"',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return true for "12"^^xsd:nonNegativeInteger', function () {
        evaluator({
          '?a' : '"12"^^http://www.w3.org/2001/XMLSchema#nonNegativeInteger',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false for <http://example/>', function () {
        evaluator({
          '?a' : 'http://example/',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of the IRI operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'iri',
        args : [
          '?a',
        ],
      });

      it('should return iri for literal', function () {
        evaluator({
          '?a' : '"http://example/"',
        }).should.equal('http://example/');
      });

      it('should return iri for iri', function () {
        evaluator({
          '?a' : 'http://example/',
        }).should.equal('http://example/');
      });

      it('should throw an error if language tag is present', function () {
        (function () {
          evaluator({
            '?a' : '"http://example/"@en',
          });
        }).should.throw('IRI expects an simple literal, xsd:string or an IRI');
      });
    });

      /* it('should throw an error if literal value is not valid', function () {
          (function () {
          evaluator({
          '?a': '"aaaa"'
          });
          })
          .should.throw('IRI expects an simple literal, xsd:string or an IRI');
      });*/

    describe('of the sameTerm operator', function () {
      var evaluator = SparqlExpressionEvaluator({
        type : 'operation',
        operator : 'sameterm',
        args : [
          '?a',
          '?b',
        ],
      });

      it('should return true if literal variables are the same term', function () {
        evaluator({
          '?a' : '"literal"',
          '?b' : '"literal"',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return true if uri variables are the same term', function () {
        evaluator({
          '?a' : 'http://example.org/a',
          '?b' : 'http://example.org/a',
        }).should.equal('"true"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if literal variables are not the same term', function () {
        evaluator({
          '?a' : '"a"',
          '?b' : '"b"',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if uri variables are not the same term', function () {
        evaluator({
          '?a' : 'http://example.org/a',
          '?b' : 'http://example.org/b',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });

      it('should return false if blank nodes are variables', function () {
        evaluator({
          '?a' : '_:b1',
          '?b' : '_:b1',
        }).should.equal('"false"^^http://www.w3.org/2001/XMLSchema#boolean');
      });
    });

    describe('of an unsuppported expression type', function () {
      it('should throw an error', function () {
        (function () {
          SparqlExpressionEvaluator({
            type : 'invalid',
          });
        })
        .should.throw('Unsupported expression type: invalid');
      });
    });

    describe('of an unsupported operator', function () {
      it('should throw an error', function () {
        (function () {
          SparqlExpressionEvaluator({
            type : 'operation',
            operator : 'invalid',
          });
        })
        .should.throw('UnsupportedExpressionError: Unsupported operator: invalid.');
      });
    });

    describe('of an operator with an incorrect number of arguments', function () {
      it('should throw an error', function () {
        (function () {
          SparqlExpressionEvaluator({
            type : 'operation',
            operator : 'regex',
            args : [
              1,
            ],
          });
        })
        .should.throw('InvalidArgumentsNumberError: Invalid number of arguments for regex: 1 (expected between bounds: 2 - 3).');
      });
    });

    describe('of an operator with an invalid number of arguments', function () {
      it('should throw an error', function () {
        (function () {
          SparqlExpressionEvaluator({
            type : 'operation',
            operator : '+',
            args : [
              '"a"',
              '"b"',
              '"c"',
            ],
          });
        })
        .should.throw('InvalidArgumentsNumberError: Invalid number of arguments for +: 3 (expected 2).');
      });
    });
  });
});

describe('SparqlExpressionEvaluator.evaluate', function () {
  it('should return the evaluation of an expression for the given bindings', function () {
    SparqlExpressionEvaluator.evaluate({
      type : 'operation',
      operator : '+',
      args : [
        '?a',
        '?b',
      ],
    }, {
      '?a' : '"1"^^http://www.w3.org/2001/XMLSchema#integer',
      '?b' : '"2"^^http://www.w3.org/2001/XMLSchema#integer',
    })
    .should.equal('"3"^^http://www.w3.org/2001/XMLSchema#integer');
  });

  it('should throw an error when not all bindings are present', function () {
    (function () {
      SparqlExpressionEvaluator.evaluate({
        type : 'operation',
        operator : '+',
        args : [
          '?a',
          '?b',
        ],
      }, {
        '?a' : '"1"^^http://www.w3.org/2001/XMLSchema#integer',
      });
    }).should.throw('Cannot evaluate variable ?b because it is not bound.');
  });
});
