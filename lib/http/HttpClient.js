/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A HttpClient downloads documents through HTTP. */

var request = require('request'),
    _ = require('lodash');

// Creates a new HttpFetcher
function HttpClient(options) {
  if (!(this instanceof HttpClient))
    return new HttpClient(options);
  options = _.defaults(options || {}, { request: request, contentType: '*/*' });
  this._request = options.request;
  this._contentType = options.contentType;
}

// Gets a representation of the resource with the given URL
HttpClient.prototype.get = function (url) {
  return this._request({
    url: url,
    method: 'GET',
    headers: { 'Content-Type': this._contentType },
  });
};

module.exports = HttpClient;
