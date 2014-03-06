/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** Browser interface for the LDF client. */

var LinkedDataFragmentsClientUI = (function ($) {
  var ldf = require('ldf-client'),
      SparqlQuerySolver = ldf.SparqlQuerySolver,
      LinkedDataFragmentsClient = ldf.LinkedDataFragmentsClient,
      Logger = ldf.Logger,
      N3 = require('n3');

  // Redirect the logger to the UI
  Logger.prototype._print = function (items) {
    $('.ldf-client .log').each(function () {
      var $log = $(this);
      $log.text($log.text() + items.join(' ') + '\n');
      $log.scrollTop(1E10);
    });
  };

  // Creates a new Linked Data Fragments Client UI
  function LinkedDataFragmentsClientUI(element, config) {
    this._$element = $(element);
    this._config = config;
  }

  // Activates the Linked Data Fragments Client UI
  LinkedDataFragmentsClientUI.prototype.activate = function () {
    // Find the UI elements
    var config = this._config,
        $query = this._$element.find('.query'),
        $results = this._$element.find('.results'),
        $execute = this._$element.find('.execute'),
        $log = this._$element.find('.log', this);

    // Execute the query when the button is clicked
    $execute.click(function () {
      // Clear results and log
      $log.empty();
      $results.empty();
      $execute.prop('disabled', true);

      // Create the client to solve the query
      var client = new LinkedDataFragmentsClient(config),
          querySolver = new SparqlQuerySolver(client, config.prefixes);
      querySolver.getQueryResults($query.val())
      .then(function (result) {
        // Display result rows
        if (result.rows) {
          $results.text(JSON.stringify(result.rows, null, '  '));
        }
        // Display Turtle
        else {
          var writer = new N3.Writer(config.prefixes);
          writer.addTriples(result.triples);
          writer.end(function (error, turtle) {
            $results.text(error ? error.message : turtle);
          });
        }
      })
      .fail(function (error) { $results.text(error.message); })
      .fin(function () { $execute.prop('disabled', false); });
    });
  };

  return LinkedDataFragmentsClientUI;
})(jQuery);
