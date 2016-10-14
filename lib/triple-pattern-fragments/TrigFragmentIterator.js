/*! @license MIT ©2013-2016 Ruben Verborgh, Ghent University - imec */
/* A TrigFragmentIterator reads data and metadata from Linked Data Fragments in TriG. */

var TurtleFragmentIterator = require('./TurtleFragmentIterator'),
    rdf = require('../util/RdfUtil');

// Creates a new TrigFragmentIterator
function TrigFragmentIterator(source, fragmentUrl) {
  if (!(this instanceof TrigFragmentIterator))
    return new TrigFragmentIterator(source, fragmentUrl);
  TurtleFragmentIterator.call(this, source, fragmentUrl);
  this._metadataGraph = null;
}
TurtleFragmentIterator.subclass(TrigFragmentIterator);

// Sends the given parsed quad to the data or metadata stream
TrigFragmentIterator.prototype._processTriple = function (quad) {
  // Quads with a non-default graph are considered metadata
  if (!this._metadataGraph && quad.predicate === rdf.FOAF + 'primaryTopic')
    this._metadataGraph = quad.subject;
  if (!this._metadataGraph || quad.graph !== this._metadataGraph) this._push(quad);
  else this.metadataStream._push(quad);
};

// Indicates whether the class supports the content type
TrigFragmentIterator.supportsContentType = function (contentType) {
  return (/^application\/(?:trig|n-quads)$/).test(contentType);
};

module.exports = TrigFragmentIterator;
