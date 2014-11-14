/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A CompositeExtractor combines metadata from different extractors. */

var _ = require('lodash');

/**
 * Creates a new `CompositeExtractor`.
 * @classdesc A `CompositeExtractor` combines metadata from different extractors.
 * @param {object.<string, MetadataExtractor>} extractors Metadata extractors per type;
 *        the extraction callback will be invoked once for each type
 * @constructor
 * @augments MetadataExtractor
 */
function CompositeExtractor(extractors) {
  if (!(this instanceof CompositeExtractor))
    return new CompositeExtractor(extractors);
  this._extractors = extractors || {};
}

/* Extracts metadata from the stream of triples. */
CompositeExtractor.prototype.extract = function (metadata, tripleStream, callback) {
  var hasExtractors = false;
  _.each(this._extractors, function (extractors, type) {
    var combined = {}, pending = extractors.length;
    hasExtractors = hasExtractors || pending !== 0;
    function addMetadata(error, metadata) {
      error || _.defaults(combined, metadata);
      if (--pending === 0) {
        var result = {};
        result[type] = combined;
        callback(null, result);
      }
    }
    _.each(extractors, function (e) { e.extract(metadata, tripleStream, addMetadata); });
  });
  if (!hasExtractors) callback(null, {});
};

module.exports = CompositeExtractor;
