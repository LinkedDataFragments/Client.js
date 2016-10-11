/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* A CompositeExtractor combines metadata from different extractors. */

var MetadataExtractor = require('./MetadataExtractor'),
    _ = require('lodash');

/**
 * Creates a new `CompositeExtractor`.
 * @classdesc A `CompositeExtractor` combines metadata from different extractors.
 * @param {object.<string, MetadataExtractor>|Extractor[]} extractors Metadata extractors (per type);
 *        the extraction callback will be invoked once for each type
 * @constructor
 * @augments MetadataExtractor
 */
function CompositeExtractor(extractors) {
  if (!(this instanceof CompositeExtractor))
    return new CompositeExtractor(extractors);
  MetadataExtractor.call(this);

  // Extractors are grouped per type.
  // If the extractors were passed as an array, assume they all have an empty type.
  this._extractors = _.isArray(extractors) ? { '': extractors } : extractors;
  // Disable extraction if no type has any extractors
  if (!_.any(this._extractors, 'length')) this._extractors = null;
}
MetadataExtractor.inherits(CompositeExtractor);

/* Extracts metadata from the stream of triples. */
CompositeExtractor.prototype._extract = function (metadata, tripleStream, callback) {
  // Collect and emit metadata per type
  _.each(this._extractors || callback(null, {}) && null, function (extractorsForType, type) {
    // Combine metadata from all extractors of the type
    var combined, pending = extractorsForType.length;
    _.each(extractorsForType, function (e) { e.extract(metadata, tripleStream, collect); });

    // Collects metadata of a specific extractor
    function collect(error, metadata) {
      // Incorporate the metadata only if no extraction error occurred
      if (!error)
        combined = combined ? _.defaults(combined, metadata) : metadata;
      // If all extractors of this type have completed, emit the combined metadata
      if (--pending === 0) {
        // If emitting combined metadata for a specific type,
        // wrap the result in an object with the type as key ({ type: combined })
        if (type)
          metadata = combined, combined = {}, combined[type] = metadata;
        callback(null, combined || {});
      }
    }
  });
};

module.exports = CompositeExtractor;
