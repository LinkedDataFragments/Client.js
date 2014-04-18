/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A FragmentsClient fetches basic Linked Data Fragments through HTTP. */

var HttpClient = require('./HttpClient'),
    Iterator = require('../iterators/Iterator'),
    rdf = require('../util/RdfUtil'),
    UriTemplate = require('uritemplate'),
    _ = require('lodash');

var parserTypes = [
  require('../fragments/TurtleFragmentParser'),
];
var accept = 'text/turtle;q=1.0,application/n-triples;q=0.7,text/n3,q=0.6';

// Creates a new FragmentsClient
function FragmentsClient(startFragment, options) {
  if (!(this instanceof FragmentsClient))
    return new FragmentsClient(startFragment, options);

  options = _.defaults(options || {}, { contentType: accept });
  this._cache = {};
  this._client = options.httpClient || new HttpClient(options);
  this._startFragment = typeof startFragment === 'string' ?
                        new Fragment(this._client, startFragment) : startFragment;
}

// Returns the basic Linked Data Fragment for the given triple pattern
FragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  // Check whether the fragment was cached
  var cache = this._cache, key = JSON.stringify(pattern);
  if (key in cache)
    return cache[key].clone();

  // Create a dummy iterator until the fragment is loaded
  var triples = cache[key] = new Iterator.CloneableIterator(true);
  this._startFragment.getProperty('controls', function (controls) {
    // Determine the URL and fetch the fragment
    var url = controls.getFragmentUrl({
      subject:   rdf.isVariable(pattern.subject)   ? null: pattern.subject,
      predicate: rdf.isVariable(pattern.predicate) ? null: pattern.predicate,
      object:    rdf.isVariable(pattern.object)    ? null: pattern.object,
    });
    triples.setSource(new Fragment(this._client, url));
  }, this);
  return triples;
};

// Creates a new basic Linked Data Fragment
function Fragment(httpClient, fragmentUrl) {
  if (!(this instanceof Fragment))
    return new Fragment(httpClient, fragmentUrl);
  Iterator.call(this);

  this._client = httpClient;
  this._loadFragmentPage(fragmentUrl);
}
Iterator.inherits(Fragment);

// Reads data from the current page of the fragment
Fragment.prototype._read = function (push) {
  if (this._fragmentPage) {
    var item = this._fragmentPage.read();
    item && push(item);
  }
};

// Loads the page of the basic Linked Data Fragment located at the given URL
Fragment.prototype._loadFragmentPage = function (pageUrl) {
  // Fetch the page of the fragment
  var fragmentPage = this._client.get(pageUrl), self = this;
  // When the content type is known, find a compatible parser
  fragmentPage.getProperty('contentType', function (contentType) {
    var hasParser = parserTypes.some(function (Parser) {
      if (Parser.supportsContentType(contentType)) {
        // Parse the fetched page of the fragment
        var parsedPage = self._fragmentPage = new Parser(fragmentPage, pageUrl);
        parsedPage.on('readable', function () { self.emit('readable'); });

        // When the page ends, try to load the next page
        parsedPage.on('end', function () {
          // Find the next page's URL through hypermedia controls in the current page
          var controls = parsedPage.getProperty('controls'), nextPage;
          try { nextPage = controls && controls.nextPage; }
          catch (controlError) {}
          // If no next page is found, this fragment has ended
          if (!nextPage) return self._end();
          // Otherwise, load the next page
          self._loadFragmentPage(nextPage);
        });
        // Pass errors to the page
        parsedPage.on('error', function (error) { fragmentPage.emit('error', error); });

        // Pass the metadata and controls of the page to the fragment
        if (!self.getProperty('metadata'))
          parsedPage.getProperty('metadata', self.setProperty.bind(self, 'metadata'));
        if (!self.getProperty('controls'))
          parsedPage.getProperty('controls', self.setProperty.bind(self, 'controls'));

        // A new page of data has been loaded, so this fragment is readable again
        return self.emit('readable'), true;
      }
    });
    if (!hasParser)
      self.emit('error', new Error('No parser for ' + contentType + ' at ' + pageUrl));
  });
  // If an error occurs, assume the fragment page is empty
  fragmentPage.on('error', function () {
    if (!self.getProperty('metadata'))
      self.setProperty('metadata', { totalTriples: 0 });
    self._end();
  });
};

module.exports = FragmentsClient;
