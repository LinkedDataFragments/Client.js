/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
var HttpClient = require('../../lib/http/HttpClient');

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
    var request = sinon.stub().returns('response');
    var client = new HttpClient({ request: request });

    describe('get http://example.org/foo', function () {
      var response = client.get('http://example.org/foo'),
          options = request.getCall(0).args[0];

      it('should call request once with the URL and accept "*/*"', function () {
        request.should.have.been.calledOnce;
        request.should.have.been.calledWithMatch({
          url: 'http://example.org/foo',
          method: 'GET',
          headers: { 'Content-Type': '*/*' },
        });
      });

      it('should return the request value', function () {
        request.should.have.returned('response');
      });
    });
  });

  describe('An HttpClient with content type "text/turtle"', function () {
    var request = sinon.stub().returns('response');
    var client = new HttpClient({ request: request, contentType: 'text/turtle' });

    describe('get http://example.org/foo', function () {
      var response = client.get('http://example.org/foo'),
          options = request.getCall(0).args[0];

      it('should call request once with the URL and accept "text/turtle"', function () {
        request.should.have.been.calledOnce;
        request.should.have.been.calledWithMatch({
          url: 'http://example.org/foo',
          method: 'GET',
          headers: { 'Content-Type': 'text/turtle' },
        });
      });

      it('should return the request value', function () {
        request.should.have.returned('response');
      });
    });
  });
});
