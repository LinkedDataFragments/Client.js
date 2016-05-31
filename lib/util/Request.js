/*! @license ©2016 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var EventEmitter = require('events').EventEmitter,
    _ = require('lodash'),
    url = require('url'),
    http = require('follow-redirects').http,
    https = require('follow-redirects').https;

// Creates an HTTP request with the given settings
function createRequest(settings) {
  // Parse the request URL
  if (settings.url)
    _.assign(settings, url.parse(settings.url));

  // Emit the response through a proxy
  var request, requestProxy = new EventEmitter(),
      requester = settings.protocol === 'http:' ? http : https;
  request = requester.request(settings, function (res) { requestProxy.emit('response', res); });
  request.end();
  requestProxy.abort = function () { request.abort(); };
  return requestProxy;
}

module.exports = createRequest;
