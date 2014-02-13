/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A HttpFetcher downloads documents through HTTP. */

var q = require('q'),
    request; // expensive, load lazily

// Creates a new HttpFetcher
function HttpFetcher(maxParallel) {
  this._queue = [];    // Queue of request execution functions
  this._active = {};   // Hash of active requests
  this._pending = 0;   // The number of currently active requests
  this._maxParallel = maxParallel || 10; // Only execute this many requests in parallel
}

HttpFetcher.prototype = {
  // Returns a promise for the HTTP GET request's result
  get: function (url) {
    return this.request(url, 'GET');
  },

  // Returns a promise for the HTTP request's result
  request: function (url, methodName) {
    var method = methodName || 'GET', requestId = methodName + url;
    // First check whether the request was already pending
    if (requestId in this._active)
      return this._active[requestId].result;

    // If not, prepare to make a request
    var self = this, deferred = q.defer();
    if (!request)
      request = require('request');

    // Request execution function
    function execute() {
      // Check whether the request is pending in the meantime
      if (requestId in self._active)
        return deferred.resolve(self._active[requestId].result);
      // If not, start the request
      var headers = { 'Accept': 'text/turtle;q=1.0,text/html;q=0.5' },
          settings = { url: url, headers: headers, timeout: 5000, method: method },
          activeRequest = request(settings, onResponse);
      // Mark the request as active
      self._active[requestId] = { request: activeRequest, result: deferred.promise };
      self._pending++;
    }

    // Response callback
    function onResponse(error, response, body) {
      // Remove the request from the active list
      delete self._active[requestId];
      self._pending--;

      // Schedule a possible pending call
      var next = self._queue.shift();
      if (next)
        process.nextTick(next);

      // Return result through the deferred
      if (error)
        return deferred.reject(new Error(error));
      if (response.statusCode >= 500)
        return deferred.reject(new Error('Request failed: ' + url));
      var contentType = /^[^;]+/.exec(response.headers['content-type'] || 'text/html')[0];
      deferred.resolve({ url: url, type: contentType, body: body, status: response.statusCode });
    }

    // Execute if possible, queue otherwise
    if (this._pending < this._maxParallel)
      execute();
    else
      this._queue.push(execute);

    return deferred.promise;
  },

  // Cancels all pending requests
  cancelAll: function () {
    for (var id in this._active)
      this._active[id].request.abort();
    this._active = {};
    this._queue = [];
  }
};

module.exports = HttpFetcher;
