/*! @license ©2015 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* A FilterExtractor extracts filter metadata from a triple stream. */

var MetadataExtractor = require('./MetadataExtractor'),
  rdf = require('../util/RdfUtil'),
  atob = require('atob');

/**
 * Creates a new `FilterExtractor`.
 * @classdesc A `FilterExtractor` extracts filter metadata from a triple stream.
 * @param {object} options
 * @param {string[]} [options.request=void:triples and hydra:totalItems] The filter predicates to look for.
 * @constructor
 * @augments MetadataExtractor
 */
function FilterExtractor(options) {
  if (!(this instanceof FilterExtractor))
    return new FilterExtractor(options);
  MetadataExtractor.call(this);
}
MetadataExtractor.inherits(FilterExtractor);

/* Extracts metadata from the stream of triples. */
FilterExtractor.prototype._extract = function (metadata, tripleStream, callback) {
  var filterPredicates = this._filterPredicates;
  tripleStream.on('end', sendMetadata);
  tripleStream.on('data', extractFilter);

  // Tries to extract filter information from the triple
  function extractFilter(triple) {
      if (triple.subject === metadata.fragmentUrl) {
        switch (triple.predicate) {
        case rdf.AMQ_FILTER:
          var buffer = base64ToArrayBuffer(rdf.getLiteralValue(triple.object));
          sendMetadata({
            filter: buffer
          });
          break;
        }
      }
    }
    // Sends the metadata through the callback and disables further extraction
  function sendMetadata(metadata) {
    tripleStream.removeListener('end', sendMetadata);
    tripleStream.removeListener('data', extractFilter);
    callback(null, metadata || {});
  }
};

function base64ToArrayBuffer(base64) {
  var binary_string = atob(base64);
  var len = binary_string.length;
  //var bytes = new Uint8Array( len );
  var bytes = new Int32Array(len);
  for (var i = 0; i < len; i++) {
    bytes[i] = binary_string.charCodeAt(i);
  }
  //return bytes.buffer;
  return bytes;
}

module.exports = FilterExtractor;
