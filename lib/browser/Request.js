/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Browser replacement for the request module using jQuery. */

var EventEmitter = require('events').EventEmitter,
    SingleIterator = require('../../lib/iterators/Iterator').SingleIterator,
    parseLink = require('parse-link-header'),
    _ = require('lodash');

require('setimmediate');

// Headers we cannot send (see https://www.w3.org/TR/XMLHttpRequest/#the-setrequestheader()-method)
var UNSAFE_REQUEST_HEADERS = ['accept-encoding', 'user-agent', 'referer'];
// Headers we need to obtain
var RESPONSE_HEADERS = ['content-type', 'content-location', 'link', 'memento-datetime'];

// Resources that were already time-negotiated
var negotiatedResources = Object.create(null);

// Creates an HTTP request with the given settings
function createRequest(settings) {
  var request = new EventEmitter();

  // PERFORMANCE HACK:
  // Reduce OPTIONS preflight requests by removing the Accept-Datetime header
  // on requests for resources that are presumed to have been time-negotiated
  if (negotiatedResources[removeQuery(settings.url)])
    delete settings.headers['accept-datetime'];

  // Delegate the request to jQuery's AJAX module
  var jqXHR = jQuery.ajax({
    url: settings.url,
    timeout: settings.timeout,
    type: settings.method,
    headers: _.omit(settings.headers, UNSAFE_REQUEST_HEADERS),
  })
  // Emit the result as a readable response iterator
  .success(function () {
    var response = new SingleIterator(jqXHR.responseText || '');
    response.statusCode = jqXHR.status;
    response.headers = _.object(RESPONSE_HEADERS, RESPONSE_HEADERS.map(jqXHR.getResponseHeader));
    request.emit('response', response);

    // If the resource was time-negotiated, store its "base" URI (= no query string)
    if (settings.headers['accept-datetime'] && response.headers['memento-datetime']) {
      var resource = removeQuery(response.headers['content-location'] || settings.url);
      if (!negotiatedResources[resource]) {
        // Ensure the resource is not a timegate
        var links = response.headers.link && parseLink(response.headers.link),
            timegate = removeQuery(links && links.timegate && links.timegate.url);
        if (resource !== timegate)
          negotiatedResources[resource] = true;
      }
    }
  })
  // Emit an error if the request fails
  .fail(function () {
    request.emit('error', new Error('Error requesting ' + settings.url));
  });
  // Aborts the request
  request.abort = function () { jqXHR.abort(); };

  return request;
}

// Removes the query string from a URL
function removeQuery(url) {
  return url ? url.replace(/\?.*$/, '') : '';
}

module.exports = createRequest;
