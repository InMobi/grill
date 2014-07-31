var QueryListView = function(query) {
	var id = "query-list-view-" + QueryListView.instanceNo++;
	var model = query;

	var getStatusClass = function() {
		if(model.getQueryStatus() === "SUCCESSFUL")
			return "success";

		if(model.getQueryStatus() === "CANCELLED")
			return "warning";

		if(model.getQueryStatus() === "FAILED")
			return "danger";

		return "primary";
	}

	var onCancelClick = function(event) {
		event.preventDefault();

		$(this).attr("disabled", true);
		model.cancelQuery(null);
	}

	var onViewResultClick = function(event) {
		event.preventDefault();

		$(this).attr("disabled", true);
		if($(this).html() === "Close Results") {
			$("#" + id).next().remove();
			$(this).html("View Results");
			$(this).attr("disabled", false);
			return; 
		}

		var resultView = new TableResultView;
		$("#" + id).after(resultView.getView());

		var rs = model.getResultSet();
		rs.getNextRows(function(rows) {
			console.log("Got next rows");
			resultView.updateView(rows);
		});

		$(this).html("Close Results");
		$(this).attr("disabled", false);
	}

	this.updateView = function() {
		$("#" + id + " .panel-title").text(moment(model.getSubmissionTime()).calendar());
		// $("#" + id + " .panel-body").text(model.getUserQuery());
		$("#" + id + " .panel-body").empty();
		CodeMirror($("#" + id + " .panel-body").get(0), {
  			value: model.getUserQuery(),
  			mode:  "text/x-sql",
  			readOnly: true,
  			lineWrapping: true
		});
		$("#" + id + " .panel-footer").empty();
		$("#" + id + " .panel-footer").text(model.getStatusMessage());

		if(!model.isCompleted()) {
			$("#" + id + " .panel-footer").append($("<button>", {
				text: "Cancel Query",
				class: "btn btn-danger"
			}).click(onCancelClick));
		}
		else if(model.getQueryStatus() === "SUCCESSFUL") {
			$("#" + id + " .panel-footer").append($("<button>", {
				text: "View Results",
				class: "btn btn-success"
			}).click(onViewResultClick));
		}

		$("#" + id + " .panel").removeClass().addClass("panel panel-" + getStatusClass());
	}

	this.getView = function() {
		var panelBody = $("<div>",{
			class: "panel-body",
		});
		
		var panelHeading = $("<div>", {
			class: "panel-heading"
		}).append($("<h3>", {
			class: "panel-title",
			text: moment(model.getSubmissionTime()).calendar()
		}));

		var panelFooter = $("<div>", {
			class: "panel-footer",
			text: model.getStatusMessage()
		});

		if(!model.isCompleted()) {
			panelFooter.append($("<button>", {
				text: "Cancel Query",
				class: "btn btn-danger" 
			}).click(onCancelClick));
		}
		else if(model.getQueryStatus === "SUCCESSFUL") {
			panelFooter.append($("<button>", {
				text: "View Results",
				class: "btn btn-success"
			}).click(onViewResultClick));
		}

		var panel = $("<div>", {
			class: "panel panel-" + getStatusClass()
		})
		.append(panelHeading)
		.append(panelBody)
		.append(panelFooter);

		return $("<div>", {
			id: id, 
			class: "col-md-8"
		}).append(panel);
	}

	this.getModel = function() {
		return model;
	}
}

QueryListView.instanceNo = 0;