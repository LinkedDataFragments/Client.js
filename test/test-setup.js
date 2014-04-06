/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var Logger = require('../lib/util/Logger'),
    Stream = require('stream').Stream,
    Iterator = require('../lib/iterators/Iterator');
Logger.disable();

// Set up the sinon stubbing library
var sinon = global.sinon = require('sinon');

// Set up the Chai assertion library
var chai = global.chai = require('chai'),
    should = global.should = chai.should(),
    expect = global.expect = chai.expect;
chai.use(require('sinon-chai'));

// Add triple testing methods
chai.use(function (chai, utils) {
  // Tests whether the object is equal to the given triple
  utils.addMethod(chai.Assertion.prototype, 'triple', function (s, p, o) {
    var triple = utils.flag(this, 'object');
    expect(triple).to.have.property('subject', s);
    expect(triple).to.have.property('predicate', p);
    expect(triple).to.have.property('object', o);
  });
});

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
  });

  // Tests whether the object is a stream with the given length
  utils.addMethod(chai.Assertion.prototype, 'streamWithLength', function (expectedLength, done) {
    var stream = utils.flag(this, 'object'), items = [];
    should.exist(stream);
    stream.should.be.an.instanceof(Stream);

    stream.on('error', done);
    stream.on('data', function (item) { items.push(item); });
    stream.on('end', function (error) {
      try { items.should.have.length(expectedLength); }
      catch (assertionError) { error = assertionError; }
      done(error);
    });
  });
});

// Add iterator testing methods
chai.use(function (chai, utils) {
  // Tests whether the object is a stream with the given items
  utils.addMethod(chai.Assertion.prototype, 'iteratorOf', function (expectedItems, done) {
    var iterator = utils.flag(this, 'object');
    should.exist(iterator);
    iterator.should.be.an.instanceof(Iterator);
    iterator.toArray(function (error, items) {
      try { error || items.should.deep.equal(expectedItems); }
      catch (assertionError) { error = assertionError; }
      done(error);
    });
  });

  // Tests whether the object is a stream with the given items
  utils.addMethod(chai.Assertion.prototype, 'iteratorWithLength', function (expectedLength, done) {
    var iterator = utils.flag(this, 'object');
    should.exist(iterator);
    iterator.should.be.an.instanceof(Iterator);
    iterator.toArray(function (error, items) {
      try { error || items.should.have.length(expectedLength); }
      catch (assertionError) { error = assertionError; }
      done(error);
    });
  });
});
