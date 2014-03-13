/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var Stream = require('stream').Stream;

// Set up the sinon stubbing library
var sinon = global.sinon = require('sinon');

// Set up the Chai assertion library
var chai = global.chai = require('chai'),
    should = global.should = chai.should(),
    expect = global.expect = chai.expect;
chai.use(require('sinon-chai'));

// Add stream testing methods
chai.use(function (chai, utils) {
  // Tests whether the object is a stream with the given items
  utils.addMethod(chai.Assertion.prototype, 'streamOf', function (expectedItems, done) {
    var stream = utils.flag(this, 'object'), items = [];
    should.exist(stream);
    stream.should.be.an.instanceof(Stream);

    stream.on('error', done);
    stream.on('data', function (item) { items.push(item); });
    stream.on('end', function (error) {
      try { items.should.deep.equal(expectedItems); }
      catch (assertionError) { error = assertionError; }
      done(error);
    });
    stream.resume();
  });
});
