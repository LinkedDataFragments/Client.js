/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TurtleFragmentParser parses basic Linked Data Fragment in Turtle. */

var TransformIterator = require('../iterators/Iterator').TransformIterator,
    TriplesIterator = require('../iterators/TriplesIterator'),
    rdf = require('../util/RdfUtil');

// Creates a new TurtleFragmentParser
function TurtleFragmentParser(source, documentUrl) {
  if (!(this instanceof TurtleFragmentParser))
    return new TurtleFragmentParser(source, documentUrl);
  TransformIterator.call(this, new TriplesIterator(source, { documentURI: documentUrl }));

  this._documentUrl = documentUrl;
  // When a metadata listener is added, drain the source to read metadata
  this.on('newListener', function metadataListenerAdded(event) {
    if (event === 'metadataSet') {
      var self = this;
      // Load the entire source into the buffer, so we can extract metadata
      this._bufferSize = Infinity;
      this._fillBufferOrEmitEnd();
      this.on('readable', this._fillBufferOrEmitEnd.bind(this));
      this.removeListener('newListener', metadataListenerAdded);
    }
  });
}
TransformIterator.inherits(TurtleFragmentParser);

// Processes a triple from the Turtle parser
TurtleFragmentParser.prototype._transform = function (triple, push, done) {
  push(triple), done();
  // Inspect control triples
  if (!this.hasProperty('metadata')) {
    // Parse total triple count
    if (triple.predicate === rdf.VOID_TRIPLES &&
        rdf.decodedURIEquals(this._documentUrl, triple.subject)) {
      var totalTriples = parseInt(rdf.getLiteralValue(triple.object), 10);
      this.setProperty('metadata', { totalTriples: totalTriples });
    }
  }
};

// Indicates whether the class supports the content type
TurtleFragmentParser.supportsContentType = function (contentType) {
  return /^(?:text\/turtle|text\/n3|application\/n-triples)$/.test(contentType);
};

module.exports = TurtleFragmentParser;
