/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A FragmentsClient fetches basic Linked Data Fragments through HTTP. */

var HttpClient = require('./HttpClient'),
    rdf = require('../../lib/rdf/RdfUtil');

// Creates a new FragmentsClient
function FragmentsClient(uriTemplate, options) {
  if (!(this instanceof FragmentsClient))
    return new FragmentsClient(uriTemplate, options);

  this._uriTemplate = uriTemplate;
  this._client = (options && options.httpClient) || new HttpClient();
}

// Returns the basic Linked Data Fragment for the given triple pattern
FragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  var url = this._uriTemplate.expand({
    subject:   rdf.isVariable(pattern.subject)   ? null: pattern.subject,
    predicate: rdf.isVariable(pattern.predicate) ? null: pattern.predicate,
    object:    rdf.isVariable(pattern.object)    ? null: pattern.object,
  });
  var fragment = this._client.get(url);
  return fragment;
};

module.exports = FragmentsClient;
