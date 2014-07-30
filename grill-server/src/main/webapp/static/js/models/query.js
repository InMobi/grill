var Query = function(handle) {
	var queryHandle = handle;
	var queryURL = util.QUERY_URL + "/" + queryHandle;
	var queryStatus = "";
	var userQuery = "";
	var submissionTime = "";
	var statusMessage = "";
	var progress = 0;
	var resultSetAvailable = false;
	var priority = "";
	var onCompletedListener = null;
	var onUpdatedListener = null;

	this.getQueryStatus = function() {
		return queryStatus;
	};

	this.getStatusMessage = function() {
		return statusMessage;
	}

	this.isQuerySubmitted = function() {
		return queryHandle !== null;
	};

	this.isResultSetAvailable = function() {
		return resultSetAvailable;
	}

	this.getProgress = function() {
		return progress;
	}

	this.getResultSet = function() {
		$.ajax({
			url: queryURL + "/resultset",
			type: 'GET', 
			data: {
				publicId: session.getSessionHandle()["publicId"],
				queryHandle: queryHandle,
				pageNumber: 1,
				fetchSize: 10
			},
			dataType: 'json',
			success: function(data) {
				console.log(data);
			}
		});
	};

	this.setOnCompletedListener = function(listener) {
		onCompletedListener = listener;
	}

	this.setOnUpdatedListener = function(listener) {
		onUpdatedListener = listener;
	}

	var update = function(callback) {
		$.ajax({
			url: queryURL,
			type: 'GET',
			data: {publicId: session.getSessionHandle()["publicId"]},
			dataType: 'json',
			success: function(data) {
				console.log(data);
				if(data.hasOwnProperty("queryHandle") && data["queryHandle"].hasOwnProperty("handleId") && data["queryHandle"]["handleId"] === queryHandle) {
					queryStatus = data["status"]["status"];
					userQuery = data["userQuery"];
					submissionTime = data["submissionTime"];
					statusMessage = data["status"]["statusMessage"];
					progress = data["status"]["progress"];
					resultSetAvailable = data["status"]["isResultSetAvailable"];
					priority = data["priority"];
				}
				else
					console.log("Error updating query data: " + data);
				if(util.isFunction(callback))
					callback();
			},
			error: function(jqXHR, textStatus, errorThrown) {
				console.log("Error querying query data: " + textStatus);
				if(util.isFunction(callback))
					callback();
			}
		});
	}

	var interval = window.setInterval(function() {
		if(queryStatus !== "SUCCESSFUL" && queryStatus !== "FAILED" && queryStatus !== "CANCELLED") {
			update(function() {
				if(util.isFunction(onUpdatedListener))
					onUpdatedListener();
			});
		}
		else {
			clearInterval(interval);
			if(util.isFunction(onCompletedListener))
				onCompletedListener();
		}
	}, 3000);

};