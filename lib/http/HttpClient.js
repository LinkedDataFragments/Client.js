/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A HttpClient downloads documents through HTTP. */

var Iterator = require('../iterators/Iterator'),
    request = require('request');

// Creates a new HttpFetcher
function HttpClient(options) {
  if (!(this instanceof HttpClient))
    return new HttpClient(options);

  options = options || {};
  this._request = options.request || request;
  this._contentType = options.contentType || '*/*';
}

// Gets a representation of the resource with the given URL
HttpClient.prototype.get = function (url) {
  return this.request(url, 'GET');
};

// Retrieves a representation of the resource with the given URL and method
HttpClient.prototype.request = function (url, method) {
  var responseIterator = new Iterator.PassthroughIterator(true);
  request = this._request({
    url: url,
    method: method || 'GET',
    headers: { accept: this._contentType },
  })
  .on('response', function (response) {
    // Redirect output to the iterator
    response.pause && response.pause();
    responseIterator.setSource(response);
    // Responses _must_ be entirely consumed,
    // or they can lead to out-of-memory errors (http://nodejs.org/api/http.html)
    responseIterator._bufferSize = Infinity;
    response.on('readable', function () { responseIterator._fillBufferOrEmitEnd(); });
    // Emit the content type
    var contentType = response.headers['content-type'] || '';
    responseIterator.emit('contentType', contentType.replace(/\s*(?:;.*)?$/, ''));
  });
  return responseIterator;
};

module.exports = HttpClient;
