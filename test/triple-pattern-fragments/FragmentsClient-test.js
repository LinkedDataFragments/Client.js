/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
var FragmentsClient = require('../../lib/triple-pattern-fragments/FragmentsClient');

var AsyncIterator = require('asynciterator'),
    rdf = require('../../lib/util/RdfUtil'),
    fs = require('fs'),
    path = require('path');

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
    var startFragment = new AsyncIterator();
    startFragment.setProperty('controls', {
      getFragmentUrl: function (pattern) {
        var encode = encodeURIComponent;
        return 'http://data.linkeddatafragments.org/dbpedia' +
               '?subject='   + encode(pattern.subject || '') +
               '&predicate=' + encode(pattern.predicate || '') +
               '&object='    + encode(pattern.object || '');
      },
    });
    function createClient(httpClient) {
      return new FragmentsClient(startFragment, { httpClient: httpClient });
    }
    function createHttpClient(fragment) {
      return { get: sinon.stub().returns(fragment) };
    }

    describe('when asked for ?s ?o dbpedia:York', function () {
      var pattern = rdf.triple('?s', 'dbpedia-owl:birthPlace', 'dbpedia:York');

      describe('and receiving a Turtle response', function () {
        var fragment = fromFile(path.join(__dirname, '/../data/fragments/$-birthplace-york.ttl'));
        var httpClient = createHttpClient(fragment);
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern);
        fragment.setProperty('statusCode',  200);
        fragment.setProperty('contentType', 'text/turtle');

        it('should GET the corresponding fragment', function () {
          httpClient.get.should.have.been.calledOnce;
          httpClient.get.should.have.been.calledWith('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=dbpedia-owl%3AbirthPlace&object=dbpedia%3AYork');
        });

        it('should stream the data triples in the fragment', function (done) {
          result.should.be.an.iteratorWithLength(19, done);
        });

        it('should emit the fragment metadata', function (done) {
          result.getProperty('metadata', function (metadata) {
            metadata.should.deep.equal({ totalTriples: 169 });
            done();
          });
        });
      });

      describe('and receiving a non-supported response', function () {
        var fragment = fromFile(__filename);
        var httpClient = createHttpClient(fragment);
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern), resultError;
        result.on('error', function (error) { resultError = error; });
        fragment.setProperty('statusCode',  200);
        fragment.setProperty('contentType', 'application/javascript');

        it('should emit an error', function () {
          resultError.should.deep.equal(new Error('No parser for application/javascript'));
        });
      });
    });

    describe('when asked for ?p a dbpedia:Artist', function () {
      var pattern = rdf.triple('?p', rdf.RDF_TYPE, rdf.DBPEDIA + 'Artist');

      describe('and receiving a multi-page response', function () {
        // Stub HTTP client so it returns the pages
        var calls = 0, page,
            httpClient = {
              get: sinon.spy(function () {
                calls++;
                if      (calls === 1) page = fromFile(path.join(__dirname, '/../data/fragments/$-type-artist.ttl'));
                else if (calls === 2) page = fromFile(path.join(__dirname, '/../data/fragments/$-type-artist-page2.ttl'));
                else if (calls === 3) page = AsyncIterator.empty(); // no third page
                else throw new Error('The HTTP client should only be called 3 times');
                page.setProperty('statusCode',  200);
                page.setProperty('contentType', 'text/turtle');
                return page;
              }),
            },
            client = createClient(httpClient),
            result = client.getFragmentByPattern(pattern);

        it('should GET the corresponding fragment pages', function () {
          result.on('end', function () {
            httpClient.get.should.have.been.calledThrice;
            httpClient.get.getCall(0).args[0].should.equal('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&object=http%3A%2F%2Fdbpedia.org%2Fresource%2FArtist');
            httpClient.get.getCall(1).args[0].should.equal('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
            httpClient.get.getCall(2).args[0].should.equal('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=3');
          });
        });

        it('should stream the data triples in all pages of the fragment', function (done) {
          result.should.be.an.iteratorWithLength(44, done);
        });

        it('should emit the fragment metadata', function (done) {
          result.getProperty('metadata', function (metadata) {
            metadata.should.deep.equal({ totalTriples: 61073 });
            done();
          });
        });
      });
    });

    describe('when asked for a non-existing fragment', function () {
      var pattern = rdf.triple('?p', rdf.RDF_TYPE, rdf.DBPEDIA + 'UnknownArtist');

      describe('and receiving a 404 response', function () {
        var fragment = AsyncIterator.empty();
        var httpClient = createHttpClient(fragment);
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern), resultError;
        result.on('error', function (error) { resultError = error; });
        fragment.setProperty('statusCode',  404);
        fragment.setProperty('contentType', 'text/turtle');

        it('should GET the corresponding fragment', function () {
          httpClient.get.should.have.been.calledOnce;
          httpClient.get.should.have.been.calledWith('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&object=http%3A%2F%2Fdbpedia.org%2Fresource%2FUnknownArtist');
        });

        it('should not return any triples', function (done) {
          result.should.be.an.iteratorWithLength(0, done);
        });

        it('should emit an error', function () {
          expect(resultError).to.deep.equal(new Error('Could not retrieve http://data.linkeddatafragments.org/dbpedia?subject=&predicate=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&object=http%3A%2F%2Fdbpedia.org%2Fresource%2FUnknownArtist (404)'));
        });
      });
    });

    describe('when asked for a fragment with a literal as subject', function () {
      var pattern = rdf.triple('"a"', 'b', 'c');
      var httpClient = createHttpClient();
      var client = createClient(httpClient);
      var result = client.getFragmentByPattern(pattern);

      it('should not GET the corresponding fragment', function () {
        httpClient.get.should.not.have.been.called;
      });

      it('should not return any triples', function (done) {
        result.should.be.an.iteratorWithLength(0, done);
      });

      it('should emit the fragment metadata', function (done) {
        result.getProperty('metadata', function (metadata) {
          metadata.should.deep.equal({ totalTriples: 0 });
          done();
        });
      });
    });

    describe('when asked for a fragment with a literal as predicate', function () {
      var pattern = rdf.triple('a', '"b"', 'c');
      var httpClient = createHttpClient();
      var client = createClient(httpClient);
      var result = client.getFragmentByPattern(pattern);

      it('should not GET the corresponding fragment', function () {
        httpClient.get.should.not.have.been.called;
      });

      it('should not return any triples', function (done) {
        result.should.be.an.iteratorWithLength(0, done);
      });

      it('should emit the fragment metadata', function (done) {
        result.getProperty('metadata', function (metadata) {
          metadata.should.deep.equal({ totalTriples: 0 });
          done();
        });
      });
    });
  });

  describe('A FragmentsClient with a start fragment that errors', function () {
    var startFragment = new AsyncIterator();
    var emittedError = new Error('startfragment error');
    var client = new FragmentsClient(startFragment);
    var pattern = rdf.triple('?s', 'dbpedia-owl:birthPlace', 'dbpedia:York');

    describe('when asked for ?s ?o dbpedia:York', function () {
      var result, resultError;
      before(function (done) {
        result = client.getFragmentByPattern(pattern);
        result.on('error', function (error) { resultError = error; done(); });
        startFragment.emit('error', emittedError);
      });

      it('should not return any triples', function (done) {
        result.should.be.an.iteratorWithLength(0, done);
      });

      it('should emit the error', function () {
        expect(resultError).to.equal(emittedError);
      });
    });

    describe('when asked for ?s ?o dbpedia:York a second time', function () {
      var result, resultError;
      before(function (done) {
        result = client.getFragmentByPattern(pattern);
        result.on('error', function (error) { resultError = error; done(); });
      });

      it('should not return any triples', function (done) {
        result.should.be.an.iteratorWithLength(0, done);
      });

      it('should emit the error', function () {
        expect(resultError).to.equal(emittedError);
      });
    });
  });
});

// Creates an iterator from the given file
function fromFile(filename) {
  return AsyncIterator.wrap(fs.createReadStream(filename));
}
