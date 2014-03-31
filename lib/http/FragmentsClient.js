/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A FragmentsClient fetches basic Linked Data Fragments through HTTP. */

var HttpClient = require('./HttpClient'),
    Iterator = require('../iterators/Iterator'),
    rdf = require('../rdf/RdfUtil'),
    UriTemplate = require('uritemplate');

var parserTypes = [
  require('../fragments/TurtleFragmentParser'),
];
var accept = 'text/turtle;q=1.0,application/n-triples;q=0.7,text/n3,q=0.6';

// Creates a new FragmentsClient
function FragmentsClient(uriTemplate, options) {
  if (!(this instanceof FragmentsClient))
    return new FragmentsClient(uriTemplate, options);

  if (typeof uriTemplate === 'string')
    uriTemplate = UriTemplate.parse(uriTemplate);
  this._uriTemplate = uriTemplate;
  this._client = (options && options.httpClient) || new HttpClient({ contentType: accept });
  this._cache = {};
}

// Returns the basic Linked Data Fragment for the given triple pattern
FragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  // Determine the fragment's URL
  var url = this._uriTemplate.expand({
    subject:   rdf.isVariable(pattern.subject)   ? null: pattern.subject,
    predicate: rdf.isVariable(pattern.predicate) ? null: pattern.predicate,
    object:    rdf.isVariable(pattern.object)    ? null: pattern.object,
  });

  // Check whether the fragment was cached
  var cache = this._cache;
  if (url in cache)
    return cache[url].clone();

  // Create a dummy iterator until the fragment is loaded
  var triples = cache[url] = new Iterator.CloneableIterator(true),
      fragment = this._client.get(url);

  // Initialize the parser when the content type is known
  fragment.getProperty('contentType', function (contentType) {
    var hasParser = parserTypes.some(function (Parser) {
      if (Parser.supportsContentType(contentType))
        return triples.setSource(new Parser(fragment, url)), true;
    });
    if (!hasParser)
      triples.emit('error', new Error('No parser for ' + contentType));
  });
  // If an error occurs, assume the fragment is empty
  fragment.on('error', function () {
    if (!triples.getProperty('metadata'))
      triples.setProperty('metadata', { totalTriples: 0 });
    triples._end();
  });
  return triples;
};

module.exports = FragmentsClient;
