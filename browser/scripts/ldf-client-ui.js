/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Browser interface for the LDF client. */

var LinkedDataFragmentsClientUI = (function ($) {
  var ldf = require('ldf-client'),
      SparqlIterator = ldf.SparqlIterator,
      FragmentsClient = ldf.FragmentsClient,
      Logger = ldf.Logger,
      N3 = require('n3');

  // Creates a new Linked Data Fragments Client UI
  function LinkedDataFragmentsClientUI(element, config) {
    this._$element = $(element);
    this.config = config;
  }

  // Activates the Linked Data Fragments Client UI
  LinkedDataFragmentsClientUI.prototype.activate = function () {
    // Find the UI elements
    var config = this.config,
        $query = this._$element.find('.query'),
        $results = this._$element.find('.results'),
        $execute = this._$element.find('.execute'),
        $log = this._$element.find('.log', this),
        logger = new Logger();

    // Execute the query when the button is clicked
    $execute.click(function () {
      // Clear results and log
      $log.empty();
      $results.empty();
      $execute.prop('disabled', true);

      // Create the iterator to solve the query
      config.logger = logger;
      config.fragmentsClient = new FragmentsClient(config.startFragment, config);
      var sparqlIterator = new SparqlIterator($query.val(), config);
      switch (sparqlIterator.parsedQuery.type) {
        // Write a JSON array representation of the rows
        case 'SELECT':
          var resultsCount = 0;
          appendText($results, '[');
          sparqlIterator.on('data', function (row) {
            appendText($results, resultsCount++ ? ',\n' : '\n', row);
          });
          sparqlIterator.on('end', function () {
            appendText($results, resultsCount ? '\n]' : ']');
          });
        break;
        // Write an RDF representation of all results
        case 'CONSTRUCT':
          var writer = new N3.Writer({ write: function (chunk, encoding, done) {
            appendText($results, chunk), done && done();
          }}, config.prefixes);
          sparqlIterator.on('data', function (triple) { writer.addTriple(triple); })
                        .on('end',  function () { writer.end(); });
        break;
        default:
          throw new Error('Unsupported query type: ' + sparqlIterator.parsedQuery.type);
      }
      sparqlIterator.on('end', function () { $execute.prop('disabled', false); });
      sparqlIterator.on('error', function (error) { $results.text(error.message); throw error; });
      sparqlIterator.read();
    });

    // Appends text to the given element
    function appendText($element) {
      for (var i = 1, l = arguments.length; i < l; i++) {
        var item = arguments[i];
        if (typeof item !== 'string')
          item = JSON.stringify(item, null, '  ');
        $element.append(document.createTextNode(item));
      }
      $element.scrollTop(1E10);
    };

    // Add log lines to the log element
    logger._print = function (items) { appendText($log, items.join(' ').trim() + '\n'); }
  };

  return LinkedDataFragmentsClientUI;
})(jQuery);
