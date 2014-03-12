/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Test implementation of FragmentsClient that reads fragments from disk. */

var fs = require('fs'),
    N3 = require('n3');
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
    ((/var/.test(pattern.subject) ? '$' : pattern.subject.match(/\w+$/)[0]) + '-' +
     (/var/.test(pattern.predicate) ? '$' : pattern.predicate.match(/\w+$/)) + '-' +
     (/var/.test(pattern.object) ? '$' : pattern.object.match(/\w+$/)[0])).toLowerCase();
};

module.exports = FileFragmentsClient;
