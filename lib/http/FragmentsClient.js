/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A FragmentsClient fetches basic Linked Data Fragments through HTTP. */

var HttpClient = require('./HttpClient'),
    PassThrough = require('stream').PassThrough,
    rdf = require('../rdf/RdfUtil');

var parserTypes = [
  require('../fragments/TurtleFragmentParser'),
];
var accept = 'text/turtle;q=1.0,application/n-triples;q=0.7,text/n3,q=0.6';

// Creates a new FragmentsClient
function FragmentsClient(uriTemplate, options) {
  if (!(this instanceof FragmentsClient))
    return new FragmentsClient(uriTemplate, options);

  this._uriTemplate = uriTemplate;
  this._client = (options && options.httpClient) || new HttpClient({ contentType: accept });
}

// Returns the basic Linked Data Fragment for the given triple pattern
FragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  // Create a dummy stream until the fragment is loaded
  var fragment, tripleStream = new PassThrough({ objectMode: true });
  tripleStream.getMetadata = tripleStream.on.bind(tripleStream, 'metadata');

  // Fetch the fragment
  var url = this._uriTemplate.expand({
    subject:   rdf.isVariable(pattern.subject)   ? null: pattern.subject,
    predicate: rdf.isVariable(pattern.predicate) ? null: pattern.predicate,
    object:    rdf.isVariable(pattern.object)    ? null: pattern.object,
  });
  fragment = this._client.get(url);

  // Initialize the parser when the content type is known
  fragment.on('contentType', function (contentType) {
    var hasParser = parserTypes.some(function (Parser) {
      if (Parser.supportsContentType(contentType)) {
        var parser = new Parser(url);
        fragment.pipe(parser).pipe(tripleStream);
        parser.once('metadata', tripleStream.emit.bind(tripleStream, 'metadata'));
        return true;
      }
    });
    if (!hasParser)
      tripleStream.emit('error', new Error('No parser for ' + contentType));
  });
  return tripleStream;
};

module.exports = FragmentsClient;
