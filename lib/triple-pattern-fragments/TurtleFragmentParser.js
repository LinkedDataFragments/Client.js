/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A TurtleFragmentParser parses Triple Pattern Fragments in Turtle. */

var TransformIterator = require('../iterators/Iterator').TransformIterator,
    BufferIterator = require('../iterators/Iterator').BufferIterator,
    TriplesIterator = require('../iterators/TriplesIterator'),
    rdf = require('../util/RdfUtil');

// Creates a new TurtleFragmentParser
function TurtleFragmentParser(source, fragmentUrl) {
  if (!(this instanceof TurtleFragmentParser))
    return new TurtleFragmentParser(source, fragmentUrl);
  TransformIterator.call(this, new TriplesIterator(source, { documentURI: fragmentUrl }));
  this._fragmentUrl = fragmentUrl;
  this.metadataStream = new BufferIterator();
  if (source && source.ended) return this.metadataStream._push(null);

  // When a metadata listener is added, drain the source to read metadata
  var parser = this;
  this.metadataStream.on('newListener', function metadataListenerAdded(event) {
    if (event === 'data' || event === 'end') {
      this.removeListener('newListener', metadataListenerAdded);
      parser._bufferAll();
    }
  });
}
TransformIterator.inherits(TurtleFragmentParser);

// Processes a triple from the Turtle parser
TurtleFragmentParser.prototype._transform = function (triple, done) {
  // Route non-data triples to the metadata stream
  var destination = this;
  if (triple.predicate === rdf.VOID_TRIPLES || triple.predicate.indexOf(rdf.HYDRA) === 0)
    destination = this.metadataStream;
  destination._push(triple), done();
};

// Closes the metadata stream after the source has ended
TurtleFragmentParser.prototype._flush = function () {
  this.metadataStream._end();
  this._end();
};

// Indicates whether the class supports the content type
TurtleFragmentParser.supportsContentType = function (contentType) {
  return (/^(?:text\/turtle|text\/n3|application\/n-triples)$/).test(contentType);
};

module.exports = TurtleFragmentParser;
