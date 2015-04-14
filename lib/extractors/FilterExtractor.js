/*! @license ©2015 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* A FilterExtractor extracts filter metadata from a triple stream. */

var MetadataExtractor = require('./MetadataExtractor'),
  rdf = require('../util/RdfUtil'),
  BloomFilter = require('bloem').Bloem,
  GCSQuery = require('golombcodedsets').GCSQuery,
  murmurhash = require('murmurhash'),
  base64 = require('base64-arraybuffer').decode;

var filters = {};

filters[rdf.AMQ_GCSFILTER] = {
  properties: [],
  create: function (filterData) {
    filterData.filter = base64(filterData.filter);
    return {
      _filter: new GCSQuery(filterData.filter, murmurhash.v3), //Initialize GCS filter
      contains: function (item) {
        return this._filter.query(item);
      }
    };
  }
};

filters[rdf.AMQ_BLOOMFILTER] = {
  properties: ['hashes', 'bits'],
  create: function (filterData) {
    filterData.filter = new Buffer(filterData.filter, 'base64');
    return {
      _filter: new BloomFilter(filterData.bits, filterData.hashes, filterData.filter), //Initialize bloom filter
      contains: function (item) {
        return this._filter.has(Buffer(item));
      }
    };
  }
};

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

  var filterData = {}, filter;

  function check(filter) {
    for (var i = 0; i < filter.properties.length; i++)
      if (!(filterData[filter.properties[i]])) return false;
    return true;
  }

  // Tries to extract filter information from the triple
  function extractFilter(triple) {
    if (triple.predicate === rdf.RDF_TYPE && triple.object.indexOf(rdf.AMQ) === 0) {
      filter = filters[triple.object];
      if (filterData.filter && filterData.variable && filter && check(filter))
        sendMetadata({ variable: filterData.variable, filter: filter.create(filterData) });
    } else if (triple.predicate.indexOf(rdf.AMQ) === 0) {
      switch (triple.predicate) {
      case rdf.AMQ_FILTER:
        filterData.filter = rdf.getLiteralValue(triple.object);
        break;
      case rdf.AMQ_VARIABLE:
        filterData.variable = rdf.getLiteralValue(triple.object);
        break;
      default:
        var property = triple.predicate.substr(rdf.AMQ.length),
          value = triple.object.match(/\d+/);
        filterData[property] = parseInt(value[0], 10);
      }
      if (filterData.filter && filterData.variable && filter && check(filter))
        sendMetadata({ variable: filterData.variable, filter: filter.create(filterData) });
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
