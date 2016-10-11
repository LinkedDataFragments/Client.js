/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
var ControlsExtractor = require('../../lib/extractors/ControlsExtractor');

var AsyncIterator = require('asynciterator'),
    rdf = require('../../lib/util/RdfUtil'),
    N3 = require('n3'),
    fs = require('fs'),
    path = require('path');

describe('ControlsExtractor', function () {
  describe('The ControlsExtractor module', function () {
    it('should be a function', function () {
      ControlsExtractor.should.be.a('function');
    });

    it('should make ControlsExtractor objects', function () {
      ControlsExtractor().should.be.an.instanceof(ControlsExtractor);
    });

    it('should be an ControlsExtractor constructor', function () {
      new ControlsExtractor().should.be.an.instanceof(ControlsExtractor);
    });
  });

  describe('A ControlsExtractor instance', function () {
    var controlsExtractor = new ControlsExtractor();

    describe('extracting from an empty stream', function () {
      var controls;
      before(function (done) {
        controlsExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, AsyncIterator.empty(),
                                  function (error, c) { controls = c, done(error); });
      });

      it('should indicate the fragment URL on the controls object', function () {
        controls.should.have.property('fragment', 'http://example.org/fragment');
      });
    });

    describe('extracting from a stream without controls information', function () {
      var controls;
      before(function (done) {
        var iterator = AsyncIterator.fromArray([
          rdf.triple('http://example.org/fragment', 'otherProperty', '"1234"'),
        ]);
        controlsExtractor.extract({ fragmentUrl: 'http://example.org/fragment' }, iterator,
                                  function (error, c) { controls = c, done(error); });
      });

      it('should indicate the fragment URL on the controls object', function () {
        controls.should.have.property('fragment', 'http://example.org/fragment');
      });
    });

    describe('extracting from a triple pattern fragment', function () {
      var controls;
      before(function (done) {
        var fragment = N3.StreamParser(),
            fragmentUrl = 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2';
        fs.createReadStream(path.join(__dirname, '/../data/fragments/$-type-artist-page2.ttl')).pipe(fragment);
        controlsExtractor.extract({ fragmentUrl: fragmentUrl }, fragment,
                                  function (error, c) { controls = c, done(error); });
      });

      it('should indicate the fragment URL on the controls object', function () {
        controls.should.have.property('fragment', 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
      });

      it('should indicate the first link', function () {
        controls.should.have.property('first', 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=1');
      });

      it('should indicate the previous link', function () {
        controls.should.have.property('previous', 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=1');
      });

      it('should indicate the next link', function () {
        controls.should.have.property('next', 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=3');
      });

      it('should give a getFragmentUrl function that gives the URL for a triple pattern', function () {
        controls.should.have.property('getFragmentUrl');
        controls.getFragmentUrl.should.be.a('function');

        var result = controls.getFragmentUrl({ subject: 'a', object: 'b' });
        result.should.equal('http://data.linkeddatafragments.org/dbpedia?subject=a&object=b');
      });
    });
  });
});
