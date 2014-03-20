/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Test implementation of FragmentsClient that reads fragments from disk. */

var fs = require('fs'),
    N3 = require('n3'),
    rdf = require('../../lib/rdf/RdfUtil');
var fragmentsPath = __dirname + '/../data/fragments/';

function FileFragmentsClient() {
  this._metadata = {};
}

FileFragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  var filename = this._getPath(pattern) + '.ttl',
      tripleStream = N3.StreamParser(),
      metadata = this._metadata[this._getIdentifier(pattern)];
  fs.exists(filename, function (exists) {
    if (!exists) return tripleStream.end();
    fs.createReadStream(filename).pipe(tripleStream);
  });
  tripleStream.on('error', console.error);
  tripleStream.getMetadata = function (callback) { callback(metadata); };
  return tripleStream;
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
