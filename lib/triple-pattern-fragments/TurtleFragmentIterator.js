/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A TurtleFragmentIterator reads data and metadata from Linked Data Fragments in Turtle. */

var TransformIterator = require('asynciterator').TransformIterator,
    BufferedIterator = require('asynciterator').BufferedIterator,
    rdf = require('../util/RdfUtil'),
    N3 = require('n3');

// Creates a new TurtleFragmentIterator
function TurtleFragmentIterator(source, fragmentUrl) {
  if (!(this instanceof TurtleFragmentIterator))
    return new TurtleFragmentIterator(source, fragmentUrl);
  TransformIterator.call(this, source);

  // Expose an additional metadata stream
  var self = this;
  this.metadataStream = new BufferedIterator();
  if (source && source.ended) return this.metadataStream._push(null);
  // When a metadata listener is added, drain the source to read metadata
  this.metadataStream.on('newListener', function metadataListenerAdded(event) {
    if (event === 'data' || event === 'end') {
      this.removeListener('newListener', metadataListenerAdded);
      // TODO: drain the source in a proper way
      self._bufferSize = 1E10;
      self._fillBuffer();
    }
  });

  // Convert Turtle into triples using the N3 parser
  this._parser = new N3.Parser({ documentURI: fragmentUrl });
  this._parser.parse(function (error, triple) {
    if (error)
      self.emit('error', error);
    else if (triple)
      self._processTriple(triple);
  });
  this._fragmentUrl = fragmentUrl;
}
TransformIterator.subclass(TurtleFragmentIterator);

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
TurtleFragmentIterator.prototype._flush = function (done) {
  // Ensure the parser processes possible pending triples
  this._parser && this._parser.end();
  // Once all triples have been processed, close both streams
  this.metadataStream.close();
  done();
};

// Indicates whether the class supports the content type
TurtleFragmentIterator.supportsContentType = function (contentType) {
  return (/^(?:text\/turtle|text\/n3|application\/n-triples)$/).test(contentType);
};

module.exports = TurtleFragmentIterator;
