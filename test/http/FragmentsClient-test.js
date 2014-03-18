/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
var FragmentsClient = require('../../lib/http/FragmentsClient');

var UriTemplate = require('uritemplate'),
    rdf = require('../../lib/rdf/RdfUtil');

describe('FragmentsClient', function () {
  describe('The FragmentsClient module', function () {
    it('should be a function', function () {
      FragmentsClient.should.be.a('function');
    });

    it('should make FragmentsClient objects', function () {
      FragmentsClient().should.be.an.instanceof(FragmentsClient);
    });

    it('should be a FragmentsClient constructor', function () {
      new FragmentsClient().should.be.an.instanceof(FragmentsClient);
    });
  });

  describe('A FragmentsClient with a URI template', function () {
    var fragment = 'fragment',
        httpClient = { get: sinon.stub().returns(fragment) },
        template = UriTemplate.parse('http://example.org/dataset{?subject,predicate,object}');
    var client = new FragmentsClient(template, { httpClient: httpClient });

    describe('when asked for ?s ?o York', function () {
      var pattern = rdf.triple('?s', '?o', rdf.DBPEDIA + 'York');
      var result = client.getFragmentByPattern(pattern);

      it('should GET the corresponding fragment', function () {
        httpClient.get.should.have.been.calledOnce;
        httpClient.get.should.have.been.calledWith('http://example.org/dataset?object=http%3A%2F%2Fdbpedia.org%2Fresource%2FYork');
      });

      it('should return the corresponding fragment', function () {
        result.should.equal(fragment);
      });
    });
  });
});
