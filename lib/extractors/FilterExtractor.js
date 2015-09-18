/*! @license ©2015 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* A FilterExtractor extracts filter metadata from a triple stream. */

var MetadataExtractor = require('./MetadataExtractor'),
  rdf = require('../util/RdfUtil'),
  BloomFilter = require('bloem').Bloem,
  GCSQuery = require('golombcodedsets').GCSQuery,
  murmurhash = require('murmurhash'),
  base64 = require('base64-arraybuffer').decode,
  HttpClient = require('../util/HttpClient'),
  _ = require('lodash'),
  Cache = require('lru-cache');

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

var DEFAULT_ACCEPT = 'application/trig;q=1.0,application/n-quads;q=0.7,' +
  'text/turtle;q=0.6,application/n-triples;q=0.3,text/n3;q=0.2';

var parserTypes = [
  require('../triple-pattern-fragments/TrigFragmentIterator'),
  require('../triple-pattern-fragments/TurtleFragmentIterator'),
];

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

  this._client = (options && options.httpClient) || new HttpClient(options);
  this._cache = new Cache({
    max: 50
  });
}
MetadataExtractor.inherits(FilterExtractor);

/* Extracts metadata from the stream of triples. */
FilterExtractor.prototype._extract = function (metadata, tripleStream, callback) {
  var self = this,
    cache = this._cache,
    filterData = {},
    filter,
    count,
    filterUrl,
    filterStream;

  tripleStream.on('end', function () {
    if (!(filterUrl && withinThreshold())) {
      sendMetadata();
    }
  });
  tripleStream.on('data', extractFilter);

  function withinThreshold() {
    //TODO: fix optimization later
    return count > 1 && count < 1000; // don't request filters over 1000 elements
  }

  function check(filter) {
    for (var i = 0; i < filter.properties.length; i++)
      if (!(filterData[filter.properties[i]])) return false;
    return true;
  }

  // Tries to extract filter information from the triple
  function extractFilter(triple) {
    if (triple.predicate === rdf.VOID_TRIPLES) {
      count = triple.object.match(/\d+/);
      count = count && parseInt(count[0], 10);
      (filterUrl && withinThreshold()) && downloadFilter(filterUrl);
    } else if (triple.predicate === rdf.AMQ_MEMBERSHIPFILTER && rdf.isIRI(triple.object)) {
      filterUrl = triple.object;
      (count && withinThreshold()) && downloadFilter(filterUrl); // Download it
    } else if (triple.predicate === rdf.RDF_TYPE && triple.object.indexOf(rdf.AMQ) === 0) {
      filter = filters[triple.object];
      if (filterData.filter && filterData.variable && filter && check(filter)) {
        sendMetadata({
          variable: filterData.variable,
          filter: filter.create(filterData)
        });
      }
    } else if (triple.predicate.indexOf(rdf.AMQ) === 0) {
      switch (triple.predicate) {
      case rdf.AMQ_FILTER:
        filterData.filter = rdf.getLiteralValue(triple.object);
        break;
      case rdf.AMQ_VARIABLE:
        filterData.variable = triple.object.replace(rdf.RDF, '');
        break;
      default:
        var property = triple.predicate.substr(rdf.AMQ.length),
          value = triple.object.match(/\d+/);
        filterData[property] = parseInt(value[0], 10);
      }
      if (filterData.filter && filterData.variable && filter && check(filter)) {
        sendMetadata({
          variable: filterData.variable,
          filter: filter.create(filterData)
        });
      }
    }
  }

  // Sends the metadata through the callback and disables further extraction
  function sendMetadata(metadata) {
    tripleStream.removeListener('end', sendMetadata);
    tripleStream.removeListener('data', extractFilter);
    (filterStream) && filterStream.removeListener('end', sendMetadata);
    (filterStream) && filterStream.removeListener('data', extractFilter);
    // If filter is not cached, cache it
    if (filterUrl && !cache.has(filterUrl))
      cache.set(filterUrl, metadata);

    callback(null, metadata || {});
  }

  // Downloads filter out of band
  function downloadFilter(uri) {
    // Check whether the filter was cached
    if (cache.has(uri)) {
      sendMetadata(cache.get(uri));
      return;
    }

    var filterFragment = self._client.get(uri);

    filterFragment.on('error', function (error) {
      self.emit('error', error);
    });

    filterFragment.getProperty('statusCode', function (statusCode) {
      // Don't parse the page if its retrieval was unsuccessful
      if (statusCode !== 200) {
        callback(new Error('Could not retrieve ' + uri +
          ' (' + statusCode + ')'));
        return self._end();
      }

      // Obtain the summary data
      filterFragment.getProperty('contentType', function (contentType) {
        // Parse the page using the appropriate parser for the content type
        var Parser = _.find(parserTypes, function (P) {
          return P.supportsContentType(contentType);
        });
        if (!Parser)
          callback(new Error('No parser for ' + contentType + ' at ' + uri));

        var parsedFilter = new Parser(filterFragment, uri);
        filterStream = parsedFilter.metadataStream;

        // Tries to extract summary information from the triple
        filterStream.on('data', extractFilter);
        filterStream.on('end', sendMetadata);

        parsedFilter.on('error', function (error) {
          callback(error);
        });
      });
    });
  }

};


module.exports = FilterExtractor;
