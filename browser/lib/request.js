/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Browser replacement for the request module. */

var EventEmitter = require('events').EventEmitter,
    Readable = require('stream').Readable;

require('setimmediate');

// Indicate whether the browser caches HEAD requests
var canCacheHEAD = false;

// Requests the given resource as a stream
function request(settings) {
  var request = new EventEmitter();

  // Delegate the request to jQuery's AJAX module
  var jqXHR = jQuery.ajax({
    url: settings.url,
    timeout: settings.timeout,
    type: canCacheHEAD && settings.method || 'GET',
    headers: { accept: 'text/turtle' },
  })
  // Don't consider Not Found or Gone responses failures
  .fail(function (jqXHR, textStatus, error) {
    if (jqXHR.status !== 404 && jqXHR.status !== 410) {
      jqXHR.failure = error;
      request.emit('error', error);
    }
  })
  // Emit the result as a readable response stream
  .always(function () {
    if (jqXHR.failure) return;
    var response = new Readable();
    response._read = function () { };
    response.statusCode = jqXHR.status;
    response.headers = { 'content-type': jqXHR.getResponseHeader('content-type') };
    response.push(jqXHR.responseText || '');
    response.push(null);
    request.emit('response', response);
  })

  return request;
}

module.exports = request;
