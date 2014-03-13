/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Test implementation of FragmentsClient that reads fragments from disk. */

var fs = require('fs'),
    N3 = require('n3'),
    rdf = require('../../lib/rdf/RdfUtil');
var fragmentsPath = __dirname + '/../data/fragments/';

function FileFragmentsClient() {}

FileFragmentsClient.prototype.getFragmentByPattern = function (pattern) {
  var filename = this._getPath(pattern) + '.ttl',
      tripleStream = N3.StreamParser(),
      turtleStream = fs.createReadStream(filename);
  turtleStream.on('error', console.error);
  return turtleStream.pipe(tripleStream);
};

FileFragmentsClient.prototype.getBindingsByPattern = function (pattern) {
  return JSON.parse(fs.readFileSync(this._getPath(pattern) + '_bindings.json'));
};

FileFragmentsClient.prototype._getPath = function (pattern) {
  return fragmentsPath +
    ((rdf.isVariable(pattern.subject)   ? '$' : pattern.subject.match(/\w+$/)[0]) + '-' +
     (rdf.isVariable(pattern.predicate) ? '$' : pattern.predicate.match(/\w+$/)) + '-' +
     (rdf.isVariable(pattern.object)    ? '$' : pattern.object.match(/\w+$/)[0])).toLowerCase();
};

module.exports = FileFragmentsClient;
