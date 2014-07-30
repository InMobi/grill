var codeMirror = CodeMirror.fromTextArea(document.getElementById("query"), {
    mode: "text/x-sql"
 });

var util = new Util;
var session = new Session;

//Hidden by default
$("#query-form .loading").hide();
$("#queryui, #loginui").hide();

if(!session.isLoggedIn()) {
	//Show login UI
	$("#loginui").show();
}
else {
	//Show normal UI
	$("#queryui").show();
}

var setEnableForm = function(enable) {
	codeMirror.setOption("readOnly", !enable);
	codeMirror.setOption("nocursor", !enable);
	$("#query-form button").attr("disabled", !enable);
	if(enable)
		$("#query-form .loading").hide();
	else
		$("#query-form .loading").show();
}

var QueryStatusView = function(query) {
	var id = "query-status-view-" + QueryStatusView.instanceNo++;
	var model = query;
	var text = model.getStatusMessage();

	this.updateView = function() {
		text = model.getStatusMessage();
		$("#" + id).text(text);
	}
	
	this.getView = function() {
		return $("<span>", {id: id, text: text});
	}
};
QueryStatusView.instanceNo = 0;

$("#query-form").submit(function(event) {
	event.preventDefault(); 

	//Disable UI components
	setEnableForm(false);

	var query = $("#query").val().trim();

	//Perform basic checks
	if(query) {
		session.submitQuery(query, function(queryObj) {
			if(queryObj) {
				var queryStatusView = new QueryStatusView(queryObj);
				if($("#query-form .loading").next().length > 0)
					$("#query-form .loading").next().remove();
				$("#query-form .loading").after(queryStatusView.getView());

				queryObj.setOnUpdatedListener(queryStatusView.updateView);
				queryObj.setOnCompletedListener(function() {
					setEnableForm(true);
					//Display results
					console.log("Completed");
					queryObj.getResultSet();
				});
			}
			else {
				//Problem submitting query. Reset UI and display message
				setEnableForm(true);
			}
		});
	}
	else {
		//No query. Reset UI
		setEnableForm(true);
	}
});

$("#login-form").submit(function(event){
	event.preventDefault();

	var email = $("#email").val();
	var password = $("#password").val();

	if(!email) {
		$("#email").addClass("error");
		return;
	}
	$("#email").removeClass("error");

	if(!password) {
		$("#password").addClass("error");
		return;
	}
	$("#password").removeClass("error");

	$("#email, #password, #login-btn").attr("disabled", true);

	session.logIn(email, password, function() {
		window.location.reload();
	});
});