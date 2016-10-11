/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* A FragmentsClient fetches Triple Pattern Fragments through HTTP. */
/* eslint max-nested-callbacks: [2, 3] */

var HttpClient = require('../util/HttpClient'),
    BufferedIterator = require('asynciterator').BufferedIterator,
    rdf = require('../util/RdfUtil'),
    Cache = require('lru-cache'),
    CompositeExtractor = require('../extractors/CompositeExtractor'),
    CountExtractor = require('../extractors/CountExtractor'),
    ControlsExtractor = require('../extractors/ControlsExtractor'),
    _ = require('lodash');

// Prefer quad-based serialization formats (which allow a strict data/metadata separation),
// and prefer less verbose formats. Also, N3 support is only partial.
var DEFAULT_ACCEPT = 'application/trig;q=1.0,application/n-quads;q=0.7,' +
                     'text/turtle;q=0.6,application/n-triples;q=0.3,text/n3;q=0.2';
var parserTypes = [
  require('./TrigFragmentIterator'),
  require('./TurtleFragmentIterator'),
];

// Creates a new FragmentsClient
function FragmentsClient(startFragment, options) {
  if (!(this instanceof FragmentsClient))
    return new FragmentsClient(startFragment, options);

  // Set HTTP and cache options
  options = _.defaults(options || {}, { contentType: DEFAULT_ACCEPT });
  var cache = this._cache = new Cache({ max: 100 });
  this._httpClient = options.httpClient || new HttpClient(options);

  // Extract counts and triple pattern fragments controls by default
  this._metadataExtractor = options.metadataExtractor || new CompositeExtractor({
    metadata: [new CountExtractor()],
    controls: [new ControlsExtractor()],
  });

  if (startFragment) {
    // Fetch the start fragment if necessary
    if (typeof startFragment === 'string') {
      var startFragmentUrl = this._startFragmentUrl = startFragment;
      startFragment = new Fragment(this);
      startFragment.loadFromUrl(startFragmentUrl);
    }
    this._startFragment = startFragment;
    // If the start fragment errors, we cannot fetch any subsequent fragments
    startFragment.setMaxListeners(100); // several error listeners might be attached temporarily
    startFragment.once('error', function (error) {
      cache.reset(); // disable caching if the start fragments fails (would be errors anyway)
      startFragment.error = error; // store the error to return it on all accesses
    });
    // If the controls load, no (relevant) errors can occur anymore
    startFragment.getProperty('controls', function () {
      startFragment.error = null;
      startFragment.removeAllListeners('error');
    });
  }
}

// Returns the Triple Pattern Fragment for the given triple pattern
FragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  // Check whether the fragment was cached
  var cache = this._cache, key = JSON.stringify(pattern);
  if (cache.has(key))
    return cache.get(key).clone();
  // Create a dummy iterator until the fragment is loaded
  var fragment = new Fragment(this, pattern);

  // Check if the start fragment was loaded without error
  var startFragment = this._startFragment;
  if (startFragment.error !== null) { // null means definitely correctly loaded
    if (startFragment.error)          // an error means definitely incorrectly loaded
      return setImmediate(startFragmentError), fragment;
    startFragment.once('error', startFragmentError); // undefined means we don't know yet
  }
  function startFragmentError() { fragment.emit('error', startFragment.error); fragment.close(); }

  // Retrieve the fragment using the start fragment's controls
  startFragment.getProperty('controls', function (controls) {
    // Replace all variables and blanks in the pattern by `null`
    var subject   = rdf.isVariableOrBlank(pattern.subject)   ? null : pattern.subject;
    var predicate = rdf.isVariableOrBlank(pattern.predicate) ? null : pattern.predicate;
    var object    = rdf.isVariableOrBlank(pattern.object)    ? null : pattern.object;

    // Only attempt to fetch the fragment if its components are valid
    if (rdf.isLiteral(subject) || rdf.isLiteral(predicate)) return fragment.empty();

    // Load and cache the fragment
    pattern = { subject: subject, predicate: predicate, object: object };
    fragment.loadFromUrl(controls.getFragmentUrl(pattern));
  });
  cache.set(key, fragment);
  return fragment.clone();
};

/** Aborts all requests. */
FragmentsClient.prototype.abortAll = function () {
  this._httpClient.abortAll();
};

// Creates a new Triple Pattern Fragment
function Fragment(fragmentsClient, pattern) {
  if (!(this instanceof Fragment))
    return new Fragment(fragmentsClient);
  BufferedIterator.call(this);

  this._fragmentsClient = fragmentsClient;
  this._pattern = pattern;
}
BufferedIterator.subclass(Fragment);

// Reads data from the current page of the fragment
Fragment.prototype._read = function (count, done) {
  var item;
  while (count-- > 0 && this._page && (item = this._page.read()))
    this._push(item);
  done();
};

// Loads the Triple Pattern Fragment located at the given URL
Fragment.prototype.loadFromUrl = function (pageUrl) {
  // Fetch a page of the fragment
  var fragment = this, fragmentsClient = this._fragmentsClient, page,
      headers = { 'user-agent': 'Triple Pattern Fragments Client' };
  if (fragmentsClient._startFragmentUrl) headers.referer = fragmentsClient._startFragmentUrl;
  page = fragmentsClient._httpClient.get(pageUrl, headers);
  page.on('error', function (error) { fragment.emit('error', error); });

  page.getProperty('statusCode', function (statusCode) {
    // Don't parse the page if its retrieval was unsuccessful
    if (statusCode !== 200) {
      page.emit('error', new Error('Could not retrieve ' + pageUrl + ' (' + statusCode + ')'));
      return fragment.close();
    }

    // Obtain the page's data, metadata, and controls
    page.getProperty('contentType', function (contentType) {
      // Parse the page using the appropriate parser for the content type
      var Parser = _.find(parserTypes, function (P) { return P.supportsContentType(contentType); });
      if (!Parser)
        return fragment.emit('error', new Error('No parser for ' + contentType + ' at ' + pageUrl));
      var parsedPage = fragment._page = new Parser(page, pageUrl);
      parsedPage.on('readable', function () { fragment.readable = true; });

      // Extract the page's metadata and controls
      var controls = {};
      fragmentsClient._metadataExtractor.extract({ fragmentUrl: pageUrl },
        parsedPage.metadataStream, function (error, metadata) {
          // Emit all new properties
          for (var type in metadata) {
            if (!fragment.getProperty(type))
              fragment.setProperty(type, metadata[type]);
          }
          // Store the controls so we can find the next page
          controls = metadata.controls || controls;
        });

      // Load the next page when this one is finished, using setImmediate to wait for controls
      parsedPage.on('end', function () { setImmediate(loadNextPage); });
      function loadNextPage() {
        // Find the next page's URL through hypermedia controls in the current page
        var nextPage;
        try { nextPage = controls && controls.next; }
        catch (controlError) { /* ignore missing control */ }
        // Load the next page, or end if none was found
        nextPage ? fragment.loadFromUrl(nextPage) : fragment.close();
      }
      parsedPage.on('error', function (error) { page.emit('error', error); });

      // A new page of data has been loaded, so this fragment is readable again
      fragment.readable = true;
    });
  });
};

// Empties the fragment and returns it
Fragment.prototype.empty = function () {
  if (!this.getProperty('metadata'))
    this.setProperty('metadata', { totalTriples: 0 });
  return this.close(), this;
};

Fragment.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + rdf.toQuickString(this._pattern) + ')}';
};

module.exports = FragmentsClient;
