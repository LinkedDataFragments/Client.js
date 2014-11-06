/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */

var Logger = require('../lib/util/Logger'),
    Iterator = require('../lib/iterators/Iterator');
Logger.setLevel('warning');

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

// Add iterator testing methods
chai.use(function (chai, utils) {
  // Tests whether the object is a stream with the given items
  utils.addMethod(chai.Assertion.prototype, 'iteratorOf', function (expectedItems, done) {
    getIteratorItems(utils.flag(this, 'object'), function (error, items) {
      try { done(error) || items.should.deep.equal(expectedItems); }
      catch (error) { done(error); }
    });
  });

  // Tests whether the object is a stream with the given items
  utils.addMethod(chai.Assertion.prototype, 'iteratorWithLength', function (expectedLength, done) {
    getIteratorItems(utils.flag(this, 'object'), function (error, items) {
      try { done(error) || items.should.have.length(expectedLength); }
      catch (error) { done(error); }
    });
  });
});

// Gets the items of the given iterator, validating its characteristics
function getIteratorItems(iterator, callback) {
  var wasEmpty = !iterator || iterator.ended, endEmitted = 0;
  should.exist(iterator);
  iterator.should.be.an.instanceof(Iterator);
  iterator.on('end', function () { endEmitted++; });
  iterator.toArray(function (error, items) {
    try {
      expect(error).to.not.exist;
      wasEmpty || endEmitted.should.equal(1);
      callback(null, items);
    }
    catch (assertionError) { callback(error); }
  });
}
