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
  return this.request(url, 'GET');
};

// Retrieves a representation of the resource with the given URL and method
HttpClient.prototype.request = function (url, method) {
  var request = this._request({
    url: url,
    method: method || 'GET',
    headers: { 'Content-Type': this._contentType },
  });
  // Emit the content type
  request.on('response', function (response) {
    var contentType = response.headers['content-type'] || '';
    this.emit('contentType', contentType.replace(/\s+(?:;.*)?/, ''));
  });
  return request;
};

module.exports = HttpClient;
