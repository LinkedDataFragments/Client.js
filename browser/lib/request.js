/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Browser replacement for the request module. */

var EventEmitter = require('events').EventEmitter,
    Readable = require('stream').Readable;

require('setimmediate');

// Requests the given resource as a stream
function request(settings) {
  var request = new EventEmitter();

  // Delegate the request to jQuery's AJAX module
  var jqXHR = jQuery.ajax({
    url: settings.url,
    timeout: settings.timeout,
    type: settings.method || 'GET',
    headers: { accept: 'text/turtle' },
  })
  .done(function () {
    var response = new Readable();
    response._read = function () { };
    response.headers = { 'content-type': jqXHR.getResponseHeader('content-type') };
    response.push(jqXHR.responseText || '');
    response.push(null);
    request.emit('response', response);
  })
  .fail(function (jqXHR, textStatus, error) {
    request.emit('error', error);
  });

  return request;
}

module.exports = request;
