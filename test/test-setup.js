/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */

var Logger = require('../lib/util/Logger'),
    AsyncIterator = require('asynciterator');
Logger.setLevel('warning');

// Set up the sinon stubbing library
global.sinon = require('sinon');

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

// Add AsyncIterator testing methods
chai.use(function (chai, utils) {
  // Tests whether the object is an iterator with the given items
  utils.addMethod(chai.Assertion.prototype, 'iteratorOf', function (expectedItems, done) {
    var actualItems = [];
    validateAsyncIterator(utils.flag(this, 'object'),
                          function (item) { actualItems.push(item); },
                          function () { actualItems.should.deep.equal(expectedItems); }, done);
  });
  // Tests whether the object is an iterator with the given number of items
  utils.addMethod(chai.Assertion.prototype, 'iteratorWithLength', function (expectedItemCount, done) {
    var itemCount = 0;
    validateAsyncIterator(utils.flag(this, 'object'),
                          function (item) { itemCount++; },
                          function () { itemCount.should.equal(expectedItemCount); }, done);
  });
});

// Validates an AsyncIterator and its items
function validateAsyncIterator(iterator, handleItem, validate, done) {
  try {
    should.exist(iterator);
    iterator.should.be.an.instanceof(AsyncIterator);

    if (iterator.ended) {
      validate();
      done();
    }
    else {
      iterator.on('data', handleItem);
      iterator.on('error', done);
      iterator.on('end', function () {
        var error = null;
        try { validate(); }
        catch (e) { error = e; }
        done(error);
      });
    }
  }
  catch (error) { done(error); }
}
