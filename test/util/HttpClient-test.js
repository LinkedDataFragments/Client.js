/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
var HttpClient = require('../../lib/util/HttpClient');

var EventEmitter = require('events').EventEmitter,
    AsyncIterator = require('asynciterator');

describe('HttpClient', function () {
  describe('The HttpClient module', function () {
    it('should be a function', function () {
      HttpClient.should.be.a('function');
    });

    it('should make HttpClient objects', function () {
      HttpClient().should.be.an.instanceof(HttpClient);
    });

    it('should be an HttpClient constructor', function () {
      new HttpClient().should.be.an.instanceof(HttpClient);
    });
  });

  describe('An HttpClient without arguments', function () {
    var request = new EventEmitter();
    var createRequest = sinon.stub().returns(request);
    var client = new HttpClient({ request: createRequest });

    describe('get http://example.org/foo', function () {
      var response = client.get('http://example.org/foo');
      request.emit('response', createResponse([1, 2, 3], 'text/html;encoding=utf8'));

      it('should call request once with the URL and accept "*/*"', function () {
        createRequest.should.have.been.calledOnce;
        createRequest.should.have.been.calledWithMatch({
          url: 'http://example.org/foo',
          method: 'GET',
          headers: { 'accept': '*/*', 'accept-encoding': 'gzip,deflate' },
          followRedirect: true,
        });
      });

      it("should return an iterator with the response's contents", function (done) {
        response.should.be.an.iteratorOf([1, 2, 3], done);
      });

      it('should set the status code', function (done) {
        response.getProperty('statusCode', function (statusCode) {
          statusCode.should.equal(200);
          done();
        });
      });

      it('should set the content type', function (done) {
        response.getProperty('contentType', function (contentType) {
          contentType.should.equal('text/html');
          done();
        });
      });

      it('should set the response time', function (done) {
        response.getProperty('responseTime', function (responseTime) {
          responseTime.should.be.a('number');
          done();
        });
      });
    });
  });

  describe('An HttpClient with content type "text/turtle"', function () {
    var request = new EventEmitter();
    var createRequest = sinon.stub().returns(request);
    var client = new HttpClient({ request: createRequest, contentType: 'text/turtle' });

    describe('get http://example.org/foo', function () {
      var response = client.get('http://example.org/foo', null, { followRedirect: false });
      request.emit('response', createResponse([1, 2, 3], 'text/turtle;encoding=utf8'));

      it('should call request once with the URL and accept "text/turtle"', function () {
        createRequest.should.have.been.calledOnce;
        createRequest.should.have.been.calledWithMatch({
          url: 'http://example.org/foo',
          method: 'GET',
          headers: { 'accept': 'text/turtle', 'accept-encoding': 'gzip,deflate' },
          followRedirect: false,
        });
      });

      it('should return the request value', function (done) {
        response.should.be.an.iteratorOf([1, 2, 3], done);
      });

      it('should set the status code', function (done) {
        response.getProperty('statusCode', function (statusCode) {
          statusCode.should.equal(200);
          done();
        });
      });

      it('should set the content type', function (done) {
        response.getProperty('contentType', function (contentType) {
          contentType.should.equal('text/turtle');
          done();
        });
      });

      it('should set the response time', function (done) {
        response.getProperty('responseTime', function (responseTime) {
          responseTime.should.be.a('number');
          done();
        });
      });
    });
  });

  describe('An HttpClient executing a request that fails synchronously', function () {
    var error = new Error('request failed');
    var createRequest = sinon.stub().throws(error);
    var client = new HttpClient({ request: createRequest });

    it('should emit an error on the response', function (done) {
      client.get('http://example.org/foo').on('error', function (e) {
        expect(e).to.equal(error);
        done();
      });
    });
  });

  describe('An HttpClient executing a request that fails asynchronously', function () {
    var request = new EventEmitter();
    var error = new Error('request failed');
    var createRequest = sinon.stub().returns(request);
    var client = new HttpClient({ request: createRequest });

    it('should emit an error on the response', function (done) {
      client.get('http://example.org/foo').on('error', function (e) {
        expect(e).to.equal(error);
        done();
      });
      request.emit('error', error);
    });
  });
});

// Creates a dummy HTTP response
function createResponse(contents, contentType) {
  var response = AsyncIterator.fromArray(contents);
  response.statusCode = 200;
  response.headers = { 'content-type': contentType };
  return response;
}
