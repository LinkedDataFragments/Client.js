/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var TurtleFragmentParser = require('../../lib/fragments/TurtleFragmentParser');
var Iterator = require('../../lib/iterators/Iterator'),
    fs = require('fs');

describe('TurtleFragmentParser', function () {
  describe('The TurtleFragmentParser module', function () {
    it('should be a function', function () {
      TurtleFragmentParser.should.be.a('function');
    });

    it('should make TurtleFragmentParser objects', function () {
      TurtleFragmentParser().should.be.an.instanceof(TurtleFragmentParser);
    });

    it('should be a TurtleFragmentParser constructor', function () {
      new TurtleFragmentParser().should.be.an.instanceof(TurtleFragmentParser);
    });

    it('should make Iterator objects', function () {
      TurtleFragmentParser().should.be.an.instanceof(Iterator);
    });

    it('should be an Iterator constructor', function () {
      new TurtleFragmentParser().should.be.an.instanceof(Iterator);
    });
  });

  describe('The TurtleFragmentParser class', function () {
    it('should support Turtle', function () {
      TurtleFragmentParser.supportsContentType('text/turtle').should.be.true;
    });

    it('should support Notation3', function () {
      TurtleFragmentParser.supportsContentType('text/n3').should.be.true;
    });

    it('should support N-Triples', function () {
      TurtleFragmentParser.supportsContentType('application/n-triples').should.be.true;
    });

    it('should not support HTML', function () {
      TurtleFragmentParser.supportsContentType('text/html').should.be.false;
    });
  });

  describe('A TurtleFragmentParser for a fragment', function () {
    var fragment = fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.ttl');
    fragment.on('error', console.error);
    var parser = new TurtleFragmentParser(fragment, 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');

    it('should return all triples in the fragment', function (done) {
      parser.should.be.an.iteratorWithLength(44, done);
    });

    it('should give access to fragment metadata', function (done) {
      parser.getProperty('metadata', function (metadata) {
        metadata.should.deep.equal({ totalTriples: 61073 });
        done();
      });
    });

    describe('its fragment control set', function (done) {
      var controls;
      before(function (done) {
        parser.getProperty('controls', function (result) {
          controls = result;
          expect(controls).to.exist;
          done();
        });
      });

      it('should contain the firstPage link', function () {
        controls.should.have.property('firstPage', 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=1');
      });

      it('should contain the previousPage link', function () {
        controls.should.have.property('previousPage', 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=1');
      });

      it('should contain the nextPage link', function () {
        controls.should.have.property('nextPage', 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=3');
      });

      describe('the getFragmentUrl function', function () {
        it('should be present', function () {
          controls.should.have.property('getFragmentUrl');
          controls.getFragmentUrl.should.be.a('function');
        });

        it('should give the URL for the given triple pattern', function () {
          var result = controls.getFragmentUrl({ subject: 'a', object: 'b' });
          result.should.equal('http://data.linkeddatafragments.org/dbpedia?subject=a&object=b');
        });
      });
    });
  });

  describe('A TurtleFragmentParser for a fragment that is not read', function () {
    function createParser() {
      var fragment = fs.createReadStream(__dirname + '/../data/fragments/$-type-artist-page2.ttl');
      return new TurtleFragmentParser(fragment, 'http://data.linkeddatafragments.org/dbpedia?subject=&predicate=rdf%3Atype&object=dbpedia-owl%3AArtist&page=2');
    }

    it('should give access to fragment metadata', function (done) {
      createParser().getProperty('metadata', function (metadata) {
        metadata.should.deep.equal({ totalTriples: 61073 });
        done();
      });
    });

    it('should give access to fragment controls', function (done) {
      createParser().getProperty('controls', function (controls) {
        expect(controls).to.exist;
        controls.should.have.property('firstPage');
        controls.should.have.property('previousPage');
        controls.should.have.property('nextPage');
        controls.should.have.property('getFragmentUrl');
        done();
      });
    });
  });
});
