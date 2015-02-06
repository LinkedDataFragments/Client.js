/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A TurtleFragmentIterator reads data and metadata from Linked Data Fragments in Turtle. */

var TransformIterator = require('../iterators/Iterator').TransformIterator,
    BufferIterator = require('../iterators/Iterator').BufferIterator,
    rdf = require('../util/RdfUtil'),
    N3 = require('n3');

// Creates a new TurtleFragmentIterator
function TurtleFragmentIterator(source, fragmentUrl) {
  if (!(this instanceof TurtleFragmentIterator))
    return new TurtleFragmentIterator(source, fragmentUrl);
  TransformIterator.call(this, source);

  // Expose an additional metadata stream
  this.metadataStream = new BufferIterator();
  if (source && source.ended) return this.metadataStream._push(null);
  // When a metadata listener is added, drain the source to read metadata
  this.metadataStream.on('newListener', function metadataListenerAdded(event) {
    if (event === 'data' || event === 'end') {
      this.removeListener('newListener', metadataListenerAdded);
      self._bufferAll();
    }
  });

  // Convert Turtle into triples using the N3 parser
  var self = this;
  this._parser = new N3.Parser({ documentURI: fragmentUrl });
  this._parser.parse(function (error, triple) {
    triple && self._push(self._processTriple(triple)) ||
    error  && self.emit('error', error);
  });
  this._fragmentUrl = fragmentUrl;
}
TransformIterator.inherits(TurtleFragmentIterator);

// Sends a chunk of Turtle to the N3 parser to convert it to triples
TurtleFragmentIterator.prototype._transform = function (chunk, done) {
  this._parser.addChunk(chunk), done();
};

// Sends the given parsed triple to the data or metadata stream
TurtleFragmentIterator.prototype._processTriple = function (triple) {
  // This separation between data and metadata/controls is an approximation;
  // for a proper separation, use an RDF format with graph support (see TrigFragmentParser).
  if (triple.subject !== this._fragmentUrl && triple.predicate.indexOf(rdf.HYDRA) !== 0)
    this._push(triple);
  else
    this.metadataStream._push(triple);
};

// Closes the streams after the source has ended
TurtleFragmentIterator.prototype._flush = function () {
  // Ensure the parser processes possible pending triples
  this._parser.end();
  // Once all triples have been processed, end both streams
  this.metadataStream._end();
  this._end();
};

// Indicates whether the class supports the content type
TurtleFragmentIterator.supportsContentType = function (contentType) {
  return (/^(?:text\/turtle|text\/n3|application\/n-triples)$/).test(contentType);
};

module.exports = TurtleFragmentIterator;
