/*! @license ©2014 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */
/* Writer that serializes a SPARQL query result as a CSV of timestamps and resultcounts */
/* This Writer is for debugging purposes */

var SparqlResultWriter = require('./SparqlResultWriter');

function StatsResultWriter(sparqlIterator) {
  SparqlResultWriter.call(this, true, sparqlIterator);
  // Init stats
  this._startTime = process.hrtime();
  this._result = 0;
}
SparqlResultWriter.inherits(StatsResultWriter);


StatsResultWriter.prototype._writeHead = function () {
  this._push(['Result', 'Delay (ms)'].join(',') + '\n');
};

StatsResultWriter.prototype._transform = function (result, done) {
  var time = process.hrtime(this._startTime);
  this._result++;
  this._push([this._result, time[0] * 1000 + (time[1] / 1000000)].join(',') + '\n');
  done();
};

StatsResultWriter.prototype._end = function () {
  var time = process.hrtime(this._startTime);
  this._push(['TOTAL', time[0] * 1000 + (time[1] / 1000000)].join(',') + '\n');
  SparqlResultWriter.prototype._end.call(this);
};

module.exports = StatsResultWriter;
