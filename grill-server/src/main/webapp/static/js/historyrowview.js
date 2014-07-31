var HistoryRowView = function(query) {
	var id = "history-row-view-" + HistoryRowView.instanceNo++;
	var model = query;

	this.getModel = function() {
		return model;
	};

	this.getView = function() {
		var historyRow = $("<tr>", {
			id: id
		});

		var submissionTime = $("<td>", {
			text: moment(model.getSubmissionTime()).format('MMMM Do YYYY, h:mm:ss a')
		}).attr("data-sort-value", model.getSubmissionTime());

		var userQuery = $("<td>", {
			text: model.getUserQuery()
		});

		var status = $("<td>").append(
			$("<span>", {
				class: "label label-" + getStatusClass(),
				text: model.getQueryStatus().toLowerCase()
			}).attr("data-sort-value", model.getQueryStatus().toLowerCase())
		);

		var actions = $("<td>");

		return historyRow
			.append(submissionTime)
			.append(userQuery)
			.append(status)
			.append(actions);
	};

	this.attachedToView = function() {
		var el = $("#" + id + " td:nth-child(2)");
		el.empty();
		console.log(el.get(0));
		var codeMirror = CodeMirror(el.get(0), {
    		mode: "text/x-sql",
    		lineWrapping: true,
    		readOnly: true,
    		value: model.getUserQuery()
 		});
	}

	var getStatusClass = function() {
		if(model.getQueryStatus() === "SUCCESSFUL")
			return "success";

		if(model.getQueryStatus() === "CANCELLED")
			return "warning";

		if(model.getQueryStatus() === "FAILED")
			return "danger";

		return "primary";
	};
};
HistoryRowView.instanceNo = 0;