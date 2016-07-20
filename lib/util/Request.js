/*! @license ©2016 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* Single-function HTTP(S) request module */

var EventEmitter = require('events').EventEmitter,
    _ = require('lodash'),
    url = require('url'),
    http = require('follow-redirects').http,
    https = require('follow-redirects').https,
    zlib = require('zlib');

var DECODERS = { gzip: zlib.createGunzip, deflate: zlib.createInflate };

// Creates an HTTP request with the given settings
function createRequest(settings) {
  // Parse the request URL
  if (settings.url)
    _.assign(settings, url.parse(settings.url));

  // Emit the response through a proxy
  var request, requestProxy = new EventEmitter(),
      requester = settings.protocol === 'http:' ? http : https;
  request = requester.request(settings, function (response) {
    response = decode(response);
    response.setEncoding('utf8');
    response.pause(); // exit flow mode
    requestProxy.emit('response', response);
  });
  request.end();
  requestProxy.abort = function () { request.abort(); };
  return requestProxy;
}

// Returns a decompressed stream from the HTTP response
function decode(response) {
  var encoding = response.headers['content-encoding'];
  if (encoding) {
    if (encoding in DECODERS) {
      // Decode the stream
      var decoded = DECODERS[encoding]();
      response.pipe(decoded);
      // Copy response properties
      decoded.statusCode = response.statusCode;
      decoded.headers = response.headers;
      return decoded;
    }
    // Error when no suitable decoder found
    setImmediate(function () {
      response.emit('error', new Error('Unsupported encoding: ' + encoding));
    });
  }
  return response;
}

module.exports = createRequest;
