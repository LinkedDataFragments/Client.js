/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
var FragmentsClient = require('../../lib/http/FragmentsClient');

var Iterator = require('../../lib/iterators/Iterator'),
    rdf = require('../../lib/util/RdfUtil'),
    fs = require('fs');

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

  describe('A FragmentsClient with a start fragment', function () {
    var startFragment = Iterator.passthrough();
    startFragment.setProperty('controls', {
      getFragmentUrl: function (pattern) {
        var encode = encodeURIComponent;
        return 'http://fragment/?s=' + encode(pattern.subject || '') +
               '&p=' + encode(pattern.predicate || '') + '&o=' + encode(pattern.object || '');
      },
    });
    function createClient(httpClient) {
      return new FragmentsClient(startFragment, { httpClient: httpClient });
    }
    function createHttpClient(fragment) {
      return { get: sinon.stub().returns(fragment) };
    }

    describe('when asked for a pattern', function () {
      var pattern = rdf.triple('?s', '?o', rdf.DBPEDIA + 'York');

      describe('and receiving a Turtle response', function () {
        var fragment = Iterator.fromStream(fs.createReadStream(__dirname + '/../data/fragments/$-birthplace-york.ttl'));
        var httpClient = createHttpClient(fragment);
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern);
        fragment.setProperty('contentType', 'text/turtle');

        it('should GET the corresponding fragment', function () {
          httpClient.get.should.have.been.calledOnce;
          httpClient.get.should.have.been.calledWith('http://fragment/?s=&p=&o=http%3A%2F%2Fdbpedia.org%2Fresource%2FYork');
        });

        it('should stream the triples in the fragment', function (done) {
          result.should.be.a.iteratorWithLength(39, done);
        });
      });

      describe('and receiving a non-supported response', function () {
        var fragment = Iterator.fromStream(fs.createReadStream(__filename));
        var httpClient = createHttpClient(fragment);
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern), resultError;
        result.on('error', function (error) { resultError = error; });
        fragment.setProperty('contentType', 'application/javascript');

        it('should emit an error', function () {
          resultError.should.deep.equal(new Error('No parser for application/javascript'));
        });
      });
    });
  });
});
