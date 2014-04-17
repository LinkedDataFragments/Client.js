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
        return 'http://data.linkeddatafragments.org/dbpedia' +
               '?subject='   + encode(pattern.subject || '') +
               '&predicate=' + encode(pattern.predicate || '') +
               '&object='    + encode(pattern.object || '');
      },
    });
    function createClient(httpClient) {
      return new FragmentsClient(startFragment, { httpClient: httpClient });
    }
    function createHttpClient(fragment) {
      return { get: sinon.stub().returns(fragment) };
    }

    describe('when asked for ?s ?o dbpedia:York', function () {
      var pattern = rdf.triple('?s', '?o', rdf.DBPEDIA + 'York');

      describe('and receiving a Turtle response', function () {
        var fragment = Iterator.fromStream(fs.createReadStream(__dirname + '/../data/fragments/$-birthplace-york.ttl'));
        var httpClient = createHttpClient(fragment);
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern);
        fragment.setProperty('contentType', 'text/turtle');

        it('should GET the corresponding fragment', function () {
          httpClient.get.should.have.been.calledOnce;
          httpClient.get.should.have.been.calledWith('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=&object=http%3A%2F%2Fdbpedia.org%2Fresource%2FYork');
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

    describe('when asked for ?p a dbpedia:Artist', function () {
      var pattern = rdf.triple('?p', rdf.RDF_TYPE, rdf.DBPEDIA + 'Artist');

      describe('and receiving a multi-page response', function () {
        // Initialize pages
        var page1 = Iterator.fromStream(fs.createReadStream(__dirname + '/../data/fragments/$-type-artist.ttl'));
        var page2 = Iterator.fromStream(fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.ttl'));
        var page3 = Iterator.empty();
        // Set content types
        page1.setProperty('contentType', 'text/turtle');
        page2.setProperty('contentType', 'text/turtle');
        page3.setProperty('contentType', 'text/turtle');
        // Stub HTTP client so it returns the pages
        var httpClient = { get: sinon.stub() };
        httpClient.get.onCall(0).returns(page1);
        httpClient.get.onCall(1).returns(page2);
        httpClient.get.onCall(2).returns(page3);
        httpClient.get.onCall(3).throws();
        // Create fragments client
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern);

        it('should GET the corresponding fragment pages', function () {
          result.on('end', function () {
            httpClient.get.should.have.been.calledThrice;
            httpClient.get.getCall(0).args[0].should.equal('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&object=http%3A%2F%2Fdbpedia.org%2Fresource%2FArtist');
            httpClient.get.getCall(1).args[0].should.equal('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
            httpClient.get.getCall(2).args[0].should.equal('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=3');
          });
        });

        it('should stream the triples in all pages of the fragment', function (done) {
          result.should.be.a.iteratorWithLength(87, done);
        });
      });
    });
  });
});
