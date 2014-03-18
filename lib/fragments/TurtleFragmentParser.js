/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A TurtleFragmentParser parses basic Linked Data Fragment in Turtle. */

var Transform = require('stream').Transform,
    _ = require('lodash'),
    N3 = require('n3'),
    rdf = require('../rdf/RdfUtil');

// Creates a new TurtleFragmentParser
function TurtleFragmentParser(documentUrl) {
  if (!(this instanceof TurtleFragmentParser))
    return new TurtleFragmentParser(documentUrl);
  Transform.call(this, { decodeStrings: true });
  this._readableState.objectMode = true;
  this._readableState.highWaterMark = 4;

  // Pass data chunks to the Turtle parser
  var self = this, parser = N3.Parser({ documentURI: documentUrl });
  parser.parse(function (error, triple) { self._processTriple(error, triple); });
  this._transform = function (data, encoding, done) { parser.addChunk(data); done(); };
  this._flush = function (done) { parser.end(); done(); };
}
TurtleFragmentParser.prototype = _.create(Transform.prototype);

// Processes a triple from the Turtle parser
TurtleFragmentParser.prototype._processTriple = function (error, triple) {
  if (error)
    return this.emit('error', error);
  this.push(triple);
};

// Indicates whether the class supports the content type
TurtleFragmentParser.supportsContentType = function (contentType) {
  return /^(?:text\/turtle|text\/n3|application\/n-triples)$/.test(contentType);
};

module.exports = TurtleFragmentParser;
