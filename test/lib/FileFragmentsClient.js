/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* Dummy implementation of FragmentsClient that reads fragments from disk. */

var TransformIterator = require('asynciterator').TransformIterator,
    fs = require('fs'),
    path = require('path'),
    N3 = require('n3'),
    rdf = require('../../lib/util/RdfUtil');
var fragmentsPath = path.join(__dirname, '/../data/fragments/');

function FileFragmentsClient() {
  this._metadata = {};
}

FileFragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  var filename = this._getPath(pattern) + '.ttl',
      triples = new TransformIterator(),
      metadata = this._metadata[this._getIdentifier(pattern)];
  fs.exists(filename, function (exists) {
    if (!exists) return triples.close();
    triples.source = fs.createReadStream(filename).pipe(N3.StreamParser());
  });
  triples.on('error', console.error);
  triples.setProperty('metadata', metadata);
  return triples;
};

FileFragmentsClient.prototype.getBindingsByPattern = function (pattern) {
  return JSON.parse(fs.readFileSync(this._getPath(pattern) + '_bindings.json'));
};

FileFragmentsClient.prototype._getPath = function (pattern) {
  return fragmentsPath + this._getIdentifier(pattern);
};

FileFragmentsClient.prototype._getIdentifier = function (pattern) {
  return (rdf.toQuickString(pattern.subject) + '-' +
          rdf.toQuickString(pattern.predicate) + '-' +
          rdf.toQuickString(pattern.object))
    .replace(/\?\w+/g, '$')
    .replace(/[^\-_$a-zA-Z]/g, '')
    .toLowerCase();
};

module.exports = FileFragmentsClient;
