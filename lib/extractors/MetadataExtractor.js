/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* MetadataExtractor is a base class for objects that extract metadata from a triple stream. */

var util = require('util');

/**
 * Creates a new `MetadataExtractor`.
 * @classdesc A `MetadataExtractor` is a base class for metadata extractors.
 * @constructor
 */
function MetadataExtractor() {
  if (!(this instanceof MetadataExtractor))
    return new MetadataExtractor();
}

/**
 * Makes the specified class inherit from the current class.
 * @param {child} child The class that will inherit from the current class.
 */
MetadataExtractor.inherits = function (child) {
  util.inherits(child, this);
  child.inherits = this.inherits;
};

/**
 * Extracts metadata from the stream of triples.
 * @param {?object} metadata Existing metadata about the triples.
 * @param {String=} metadata.fragmentUrl URL of the fragment the triples belong to.
 * @param {?Iterator} tripleStream The stream of triples to extract from.
 * @param {?metadataCallback} callback The callback through which metadata will be sent.
 */
MetadataExtractor.prototype.extract = function (metadata, tripleStream, callback) {
  if (!callback) return;
  if (!tripleStream) return callback(null, {});
  this._extract(metadata || {}, tripleStream, callback);
};

/**
 * Extracts metadata from the stream of triples (with checked arguments).
 * @param {object} metadata Existing metadata about the triples.
 * @param {String=} metadata.fragmentUrl URL of the fragment the triples belong to.
 * @param {Iterator} tripleStream The stream of triples to extract from.
 * @param {metadataCallback} callback The callback through which metadata will be sent.
 * @private
 */
MetadataExtractor.prototype._extract = function (metadata, tripleStream, callback) {
  throw new Error('Not implemented');
};

/**
 * Callback that returns metadata.
 * @callback metadataCallback
 * @param {?Error} error The error that occurred during metadata extraction.
 * @param {object} metadata The metadata as an object.
*/

module.exports = MetadataExtractor;
