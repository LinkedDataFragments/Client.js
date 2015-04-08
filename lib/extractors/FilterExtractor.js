/*! @license ©2015 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* A FilterExtractor extracts filter metadata from a triple stream. */

var MetadataExtractor = require('./MetadataExtractor'),
  rdf = require('../util/RdfUtil');

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
  tripleStream.on('end', sendMetadata);
  tripleStream.on('data', extractFilter);

  var filterData = {};
  // Tries to extract filter information from the triple
  function extractFilter(triple) {
    if (triple.predicate.indexOf(rdf.AMQ) === 0) {
      switch (triple.predicate) {
      case rdf.AMQ_FILTER:
        filterData.filter = new Buffer(rdf.getLiteralValue(triple.object), 'base64');
        break;
      case rdf.AMQ_VARIABLE:
        filterData.variable = rdf.getLiteralValue(triple.object);
        break;
      default:
        var property = triple.predicate.substr(rdf.AMQ.length),
          value = triple.object.match(/\d+/);
        filterData[property] = parseInt(value[0], 10);
      }
      if (filterData.filter && filterData.variable && filterData.hashes && filterData.bits)
        sendMetadata({
          filter: filterData
        });
    }
  }

  // Sends the metadata through the callback and disables further extraction
  function sendMetadata(metadata) {
    tripleStream.removeListener('end', sendMetadata);
    tripleStream.removeListener('data', extractFilter);
    callback(null, metadata || {});
  }
};

module.exports = FilterExtractor;
