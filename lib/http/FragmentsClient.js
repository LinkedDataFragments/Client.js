/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A FragmentsClient fetches basic Linked Data Fragments through HTTP. */

var HttpClient = require('./HttpClient'),
    Iterator = require('../iterators/Iterator'),
    rdf = require('../util/RdfUtil'),
    UriTemplate = require('uritemplate'),
    _ = require('lodash');

var parserTypes = [
  require('../fragments/TurtleFragmentParser'),
];
var accept = 'text/turtle;q=1.0,application/n-triples;q=0.7,text/n3,q=0.6';

// Creates a new FragmentsClient
function FragmentsClient(startFragment, options) {
  if (!(this instanceof FragmentsClient))
    return new FragmentsClient(startFragment, options);

  options = _.defaults(options || {}, { contentType: accept });
  this._cache = {};
  this._client = options.httpClient || new HttpClient(options);
  this._startFragment = typeof startFragment === 'string' ?
                        this._getFragmentByUrl(startFragment) : startFragment;
}

// Returns the basic Linked Data Fragment for the given triple pattern
FragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  // Check whether the fragment was cached
  var cache = this._cache, key = JSON.stringify(pattern);
  if (key in cache)
    return cache[key].clone();

  // Create a dummy iterator until the fragment is loaded
  var triples = cache[key] = new Iterator.CloneableIterator(true);
  this._startFragment.getProperty('controls', function (controls) {
    // Determine the URL and fetch the fragment
    var url = controls.getFragmentUrl({
      subject:   rdf.isVariable(pattern.subject)   ? null: pattern.subject,
      predicate: rdf.isVariable(pattern.predicate) ? null: pattern.predicate,
      object:    rdf.isVariable(pattern.object)    ? null: pattern.object,
    });
    this._getFragmentByUrl(url, triples);
  }, this);
  return triples;
};

// Returns the basic Linked Data Fragment located at the given URL
FragmentsClient.prototype._getFragmentByUrl = function (url, destination) {
  var fragment = this._client.get(url);
  if (!destination)
    destination = new Iterator.PassthroughIterator(true);
  // Initialize the parser when the content type is known
  fragment.getProperty('contentType', function (contentType) {
    var hasParser = parserTypes.some(function (Parser) {
      if (Parser.supportsContentType(contentType))
        return destination.setSource(new Parser(fragment, url)), true;
    });
    if (!hasParser)
      destination.emit('error', new Error('No parser for ' + contentType));
  });
  // If an error occurs, assume the fragment is empty
  fragment.on('error', function () {
    if (!destination.getProperty('metadata'))
      destination.setProperty('metadata', { totalTriples: 0 });
    destination._end();
  });
  return destination;
};

module.exports = FragmentsClient;
