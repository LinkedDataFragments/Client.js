/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
var FederatedFragmentsClient = require('../../../lib/triple-pattern-fragments/federated/FederatedFragmentsClient');

var Iterator = require('../../../lib/iterators/Iterator'),
    rdf = require('../../../lib/util/RdfUtil'),
    fs = require('fs');

describe('FederatedFragmentsClient', function () {
  describe('The FederatedFragmentsClient module', function () {
    it('should be a function', function () {
      FederatedFragmentsClient.should.be.a('function');
    });

    it('should make FragmentsClient objects', function () {
      FederatedFragmentsClient().should.be.an.instanceof(FederatedFragmentsClient);
    });

    it('should be a FragmentsClient constructor', function () {
      new FederatedFragmentsClient().should.be.an.instanceof(FederatedFragmentsClient);
    });
  });
  
  describe('A FederatedFragmentsClient with a start fragments', function () {
  
    var startFragments = ['dbpedia', 'yago2s'].map(function (val) {
      var startFragment = Iterator.passthrough();
      startFragment.setProperty('controls', {
        getFragmentUrl: function (pattern) {
          var encode = encodeURIComponent;
          return 'http://data.linkeddatafragments.org/' + val +
                 '?subject='   + encode(pattern.subject || '') +
                 '&predicate=' + encode(pattern.predicate || '') +
                 '&object='    + encode(pattern.object || '');
        },
      });
      return startFragment;
    });
    
    function createClient(httpClient) {
      return new FederatedFragmentsClient(startFragments, { httpClient: httpClient });
    }
    function createHttpClient(fragment) {
      return { get: sinon.stub().returns(fragment) };
    }
    
    describe('when asked for ?s ?o dbpedia:York', function () {
      var pattern = rdf.triple('?s', 'dbpedia-owl:birthPlace', 'dbpedia:York');

      describe('and receiving a Turtle response', function () {
        var fragment = Iterator.fromStream(fs.createReadStream(__dirname + '/../../data/fragments/$-birthplace-york.ttl'));
        var httpClient = createHttpClient(fragment);
        
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern);
        fragment.setProperty('statusCode',  200);
        fragment.setProperty('contentType', 'text/turtle');

        it('should GET the corresponding fragment', function () {
          httpClient.get.should.have.been.calledTwice;
          httpClient.get.should.have.been.calledWith('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=dbpedia-owl%3AbirthPlace&object=dbpedia%3AYork');
          httpClient.get.should.have.been.calledWith('http://data.linkeddatafragments.org/yago2s?subject=&predicate=dbpedia-owl%3AbirthPlace&object=dbpedia%3AYork');
        });

        it('should stream the triples in the fragment', function (done) {
          result.should.be.a.iteratorWithLength(78, done);
        });

        it('should emit the fragment metadata', function (done) {
          result.getProperty('metadata', function (metadata) {
            metadata.should.deep.equal({ totalTriples: 169 });
            done();
          });
        });
      });
    
    });
  });
/*
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
      var pattern = rdf.triple('?s', 'dbpedia-owl:birthPlace', 'dbpedia:York');

      describe('and receiving a Turtle response', function () {
        var fragment = Iterator.fromStream(fs.createReadStream(__dirname + '/../data/fragments/$-birthplace-york.ttl'));
        var httpClient = createHttpClient(fragment);
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern);
        fragment.setProperty('statusCode',  200);
        fragment.setProperty('contentType', 'text/turtle');

        it('should GET the corresponding fragment', function () {
          httpClient.get.should.have.been.calledOnce;
          httpClient.get.should.have.been.calledWith('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=dbpedia-owl%3AbirthPlace&object=dbpedia%3AYork');
        });

        it('should stream the triples in the fragment', function (done) {
          result.should.be.a.iteratorWithLength(39, done);
        });

        it('should emit the fragment metadata', function (done) {
          result.getProperty('metadata', function (metadata) {
            metadata.should.deep.equal({ totalTriples: 169 });
            done();
          });
        });
      });

      describe('and receiving a non-supported response', function () {
        var fragment = Iterator.fromStream(fs.createReadStream(__filename));
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
        // Initialize pages
        var page1 = Iterator.fromStream(fs.createReadStream(__dirname + '/../data/fragments/$-type-artist.ttl'));
        var page2 = Iterator.fromStream(fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.ttl'));
        var page3 = Iterator.empty();
        // Set content types
        page1.setProperty('statusCode',  200);
        page1.setProperty('contentType', 'text/turtle');
        page2.setProperty('statusCode',  200);
        page2.setProperty('contentType', 'text/turtle');
        page3.setProperty('statusCode',  200);
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
        var fragment = Iterator.empty();
        var httpClient = createHttpClient(fragment);
        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern);
        fragment.setProperty('statusCode',  404);
        fragment.setProperty('contentType', 'text/turtle');

        it('should GET the corresponding fragment', function () {
          httpClient.get.should.have.been.calledOnce;
          httpClient.get.should.have.been.calledWith('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&object=http%3A%2F%2Fdbpedia.org%2Fresource%2FUnknownArtist');
        });

        it('should not return any triples', function (done) {
          result.should.be.a.iteratorWithLength(0, done);
        });

        it('should emit the fragment metadata', function (done) {
          result.getProperty('metadata', function (metadata) {
            metadata.should.deep.equal({ totalTriples: 0 });
            done();
          });
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
        result.should.be.a.iteratorWithLength(0, done);
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
        result.should.be.a.iteratorWithLength(0, done);
      });

      it('should emit the fragment metadata', function (done) {
        result.getProperty('metadata', function (metadata) {
          metadata.should.deep.equal({ totalTriples: 0 });
          done();
        });
      });
    });
  });*/
});
