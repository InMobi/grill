CodeMirror.fromTextArea(document.getElementById("query"), {
    mode: "text/x-sql"
 });

var session = new Session;
if(!session.isLoggedIn()) {
	session.logIn("foo", "bar", function() {
	});
}

var query = new Query;
		query.submitQuery("cube select measure2 from sample_cube where time_range_in('dt', '2014-06-24-23', '2014-06-26-01')");