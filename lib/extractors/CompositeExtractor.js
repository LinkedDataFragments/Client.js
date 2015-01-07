/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
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

  this._extractors = _.isArray(extractors) ? { '': extractors } : extractors;
  if (!_.any(this._extractors, 'length')) this._extractors = null;
}
MetadataExtractor.inherits(CompositeExtractor);

/* Extracts metadata from the stream of triples. */
CompositeExtractor.prototype._extract = function (metadata, tripleStream, callback) {
  _.each(this._extractors || void callback(null, {}), function (extractors, type) {
    var combined, pending = extractors.length;
    function addMetadata(error, metadata) {
      if (!error)
        combined = combined ? _.defaults(combined, metadata) : metadata;
      if (--pending === 0) {
        var result = combined;
        if (type) result = {}, result[type] = combined;
        callback(null, result);
      }
    }
    _.each(extractors, function (e) { e.extract(metadata, tripleStream, addMetadata); });
  });
};

module.exports = CompositeExtractor;
