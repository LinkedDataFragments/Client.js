/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
var FederatedFragmentsClient = require('../../../lib/triple-pattern-fragments/federated/FederatedFragmentsClient');

var FragmentsClient = require('../../../lib/triple-pattern-fragments/FragmentsClient'),
    AsyncIterator = require('asynciterator'),
    rdf = require('../../../lib/util/RdfUtil'),
    fs = require('fs'),
    path = require('path');

describe('FederatedFragmentsClient', function () {
  describe('The FederatedFragmentsClient module', function () {
    it('should be a function', function () {
      FederatedFragmentsClient.should.be.a('function');
    });

    it('should make FragmentsClient objects if only one start fragment', function () {
      FederatedFragmentsClient(['http://data.linkeddatafragments.org/a']).should.be.an.instanceof(FragmentsClient);
    });

    it('should be a FragmentsClient constructor if only one start fragment', function () {
      new FederatedFragmentsClient(['http://data.linkeddatafragments.org/a']).should.be.an.instanceof(FragmentsClient);
    });

    it('should make FederatedFragmentsClient objects if more than one start fragment', function () {
      FederatedFragmentsClient(['http://data.linkeddatafragments.org/a', 'http://data.linkeddatafragments.org/b']).should.be.an.instanceof(FederatedFragmentsClient);
    });

    it('should be a FederatedFragmentsClient constructor if more than one start fragment', function () {
      new FederatedFragmentsClient(['http://data.linkeddatafragments.org/a', 'http://data.linkeddatafragments.org/b']).should.be.an.instanceof(FederatedFragmentsClient);
    });
  });

  describe('A FederatedFragmentsClient with no start fragments', function () {
    var client =  new FederatedFragmentsClient([], { });
    var result = client.getFragmentByPattern({});

    it('should end', function () {
      expect(result.ended).to.be.true;
    });

    it('should emit the fragment metadata with count zero', function (done) {
      result.getProperty('metadata', function (metadata) {
        metadata.should.deep.equal({ totalTriples: 0 });
        done();
      });
    });
  });

  describe('A FederatedFragmentsClient with a start fragments', function () {
    var startFragments = ['dbpedia', 'dbpedia-live'].map(function (val) {
      var startFragment = new AsyncIterator.TransformIterator();
      startFragment.setProperty('controls', {
        getFragmentUrl: function (pattern) {
          var encode = encodeURIComponent;
          return 'http://data.linkeddatafragments.org/' + val +
                 '?subject='   + encode(pattern.subject || '') +
                 '&predicate=' + encode(pattern.predicate || '') +
                 '&object='    + encode(pattern.object || '');
        },
      });
      return startFragment;
    });

    function createClient(httpClient) {
      return new FederatedFragmentsClient(startFragments, { httpClient: httpClient });
    }
    function createHttpClient(fragment) {
      return { get: sinon.spy(function () { return fragment.clone(); }) };
    }
    function createComplexHttpClient(fragments) {
      return { get: sinon.spy(function (uri) {
        return fragments[uri].clone();
      }) };
    }

    describe('when asked for ?s ?o dbpedia:York', function () {
      var pattern = rdf.triple('?s', 'dbpedia-owl:birthPlace', 'dbpedia:York');

      describe('and receiving a Turtle response', function () {
        var fragment = AsyncIterator.wrap(fs.createReadStream(path.join(__dirname, '/../../data/fragments/$-birthplace-york.ttl')));
        var fragmentDbplive = AsyncIterator.wrap(fs.createReadStream(path.join(__dirname, '/../../data/fragments/$-birthplace-york-dbplive.ttl')));
        var httpClient = createComplexHttpClient({
          'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=dbpedia-owl%3AbirthPlace&object=dbpedia%3AYork': fragment,
          'http://data.linkeddatafragments.org/dbpedia-live?subject=&predicate=dbpedia-owl%3AbirthPlace&object=dbpedia%3AYork': fragmentDbplive,
        });

        var client = createClient(httpClient);
        var result = client.getFragmentByPattern(pattern);

        fragment.setProperty('statusCode',  200);
        fragment.setProperty('contentType', 'text/turtle');
        fragment.setProperty('responseTime', 0);
        fragmentDbplive.setProperty('statusCode',  200);
        fragmentDbplive.setProperty('contentType', 'text/turtle');
        fragmentDbplive.setProperty('responseTime', 0);

        it('should GET the corresponding fragment', function () {
          httpClient.get.should.have.been.calledTwice;
          httpClient.get.should.have.been.calledWith('http://data.linkeddatafragments.org/dbpedia?subject=&predicate=dbpedia-owl%3AbirthPlace&object=dbpedia%3AYork');
          httpClient.get.should.have.been.calledWith('http://data.linkeddatafragments.org/dbpedia-live?subject=&predicate=dbpedia-owl%3AbirthPlace&object=dbpedia%3AYork');
        });

        it('should stream the triples in the fragment', function (done) {
          result.should.be.an.iteratorWithLength(38, done);
        });

        it('should emit the fragment metadata', function (done) {
          result.getProperty('metadata', function (metadata) {
            metadata.should.deep.equal({ totalTriples: 338 });
            done();
          });
        });
      });

      describe('and receiving a non-supported response', function () {
        var fragment = AsyncIterator.wrap(fs.createReadStream(__filename));
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
  });
});
