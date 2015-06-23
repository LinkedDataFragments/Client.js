/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var Iterator = require('../iterators/Iterator'),
    request = require('request'),
    Logger = require('../util/Logger.js'),
    zlib = require('zlib'),
    _ = require('lodash');

var activeRequests = {}, requestId = 0;

/**
 * Creates a new `HttpClient`.
 * @classdesc An `HttpClient` retrieves representations of resources through HTTP
 * and offers them through an iterator interface.
 * @param {String} [options.request=(the request npm module)] The HTTP request module
 * @param {String} [options.contentType=* / *] The desired content type of representations
 * @constructor
 */
function HttpClient(options) {
  if (!(this instanceof HttpClient))
    return new HttpClient(options);

  options = options || {};
  this._request = options.request || request;
  this._contentType = options.contentType || '*/*';
  this._queue = [];
  this._activeRequests = 0;
  this._maxActiveRequests = options.concurrentRequests || 20;
  this._logger = options.logger || Logger('HttpClient');
}

/**
 * Retrieves a representation of the resource with the given URL.
 * @param {string} url The URL of the resource
 * @param {Object} [headers] Additional HTTP headers to add
 * @param {Object} [options] Additional options for the HTTP request
 * @returns {Iterator} An iterator of the representation
 */
HttpClient.prototype.get = function (url, headers, options) {
  return this.request(url, 'GET', headers, options);
};

/**
 * Retrieves a representation of the resource with the given URL.
 * @param {string} url The URL of the resource
 * @param {string} [method='GET'] method The HTTP method to use
 * @param {Object} [headers] Additional HTTP headers to add
 * @param {Object} [options] Additional options for the HTTP request
 * @returns {Iterator} An iterator of the representation
 */
HttpClient.prototype.request = function (url, method, headers, options) {
  var responseIterator = new Iterator.PassthroughIterator(true), self = this;

  function performRequest() {
    self._logger.info('Requesting', url);
    self._activeRequests++;

    // Create the request
    var acceptHeaders = { accept: self._contentType, 'accept-encoding': 'gzip,deflate' };
    var requestOptions = {
      url: url,
      method: method || 'GET',
      headers: headers ? _.assign(acceptHeaders, headers) : acceptHeaders,
      timeout: 5000,
      followRedirect: true
    }, request, startTime = new Date();
    try { request = self._request(options ? _.assign(requestOptions, options) : requestOptions); }
    catch (error) { return setImmediate(emitError, error), responseIterator; }
    activeRequests[request._id = requestId++] = request;
    function emitError(error) {
      if (!this._aborted || !error || error.code !== 'ETIMEDOUT')
        responseIterator._error(error);
      delete activeRequests[request && request._id];
    }

    // React to a possible response
    request.on('response', function (response) {
      var statusCode = response.statusCode, headers = response.headers,
          responseTime = new Date() - startTime, encoding, contentType, nextRequest;
      // Start a possible queued request
      delete activeRequests[request._id];
      self._activeRequests--;
      (nextRequest = self._queue.shift()) && nextRequest();

      // Decompress the response when necessary
      switch (encoding = headers['content-encoding'] || '') {
      case 'gzip':
        response.pipe(response = zlib.createGunzip());
        break;
      case 'deflate':
        response.pipe(response = zlib.createInflate());
        break;
      case '':
        break;
      default:
        return responseIterator._error(new Error('Unsupported encoding: ' + encoding));
      }
      response.on('error', emitError);

      // Redirect output to the iterator
      response.setEncoding && response.setEncoding('utf8');
      response.pause && response.pause();
      responseIterator.setSource(response);
      // Responses _must_ be entirely consumed,
      // or they can lead to out-of-memory errors (http://nodejs.org/api/http.html)
      responseIterator._bufferAll();

      // Emit the metadata
      contentType = (headers['content-type'] || '').replace(/\s*(?:;.*)?$/, '');
      responseIterator.setProperty('statusCode', statusCode);
      responseIterator.setProperty('contentType', contentType);
      responseIterator.setProperty('responseTime', responseTime);
    })
    .on('error', emitError);
  }

  // Start or enqueue the request
  if (this._activeRequests < this._maxActiveRequests)
    performRequest();
  else
    this._queue.push(performRequest);

  return responseIterator;
};

// Abort all active requests
HttpClient.abortAll = function () {
  for (var id in activeRequests) {
    activeRequests[id].abort();
    delete activeRequests[id];
  }
};

module.exports = HttpClient;
