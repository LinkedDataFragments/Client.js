/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* A CountExtractor extracts count metadata from a triple stream. */

var MetadataExtractor = require('./MetadataExtractor'),
    rdf = require('../util/RdfUtil');

var DEFAULT_COUNT_PREDICATES = toHash([rdf.VOID_TRIPLES, rdf.HYDRA_TOTALITEMS]);

/**
 * Creates a new `CountExtractor`.
 * @classdesc A `CountExtractor` extracts count metadata from a triple stream.
 * @param {object} options
 * @param {string[]} [options.request=void:triples and hydra:totalItems] The count predicates to look for.
 * @constructor
 * @augments MetadataExtractor
 */
function CountExtractor(options) {
  if (!(this instanceof CountExtractor))
    return new CountExtractor(options);
  MetadataExtractor.call(this);
  this._countPredicates = toHash(options && options.countPredicates) || DEFAULT_COUNT_PREDICATES;
}
MetadataExtractor.inherits(CountExtractor);

/* Extracts metadata from the stream of triples. */
CountExtractor.prototype._extract = function (metadata, tripleStream, callback) {
  var countPredicates = this._countPredicates;
  tripleStream.on('end', sendMetadata);
  tripleStream.on('data', extractCount);

  // Tries to extract count information from the triple
  function extractCount(triple) {
    if (triple.predicate in countPredicates &&
        rdf.decodedURIEquals(triple.subject, metadata.fragmentUrl)) {
      var count = triple.object.match(/\d+/);
      count && sendMetadata({ totalTriples: parseInt(count[0], 10) });
    }
  }
  // Sends the metadata through the callback and disables further extraction
  function sendMetadata(metadata) {
    tripleStream.removeListener('end', sendMetadata);
    tripleStream.removeListener('data', extractCount);
    callback(null, metadata || {});
  }
};

/**
 * Converts an array into an object with its values as keys.
 * @param {Array} array The keys for the object
 * @returns {Object} An object with the array's values as keys
 * @private
 */
function toHash(array) {
  return array && array.reduce(function (hash, key) { return hash[key] = true, hash; }, Object.create(null));
}

module.exports = CountExtractor;
