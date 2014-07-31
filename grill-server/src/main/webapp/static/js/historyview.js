var HistoryView = function() {
	var queryListViews = [];

	this.addQueryListView = function(queryListView) {
		for(var i = 0; i < queryListViews.length; i++) {
			if(queryListViews[i].getModel().getHandle() === queryListView.getModel().getHandle())
				return;
		}
		
		var localRefreshView = this.refreshView;
		queryListView.getModel().setOnUpdatedListener(function() {
			queryListViews.push(queryListView);
			localRefreshView();
		});
	}

	this.refreshView = function() {
		queryListViews.sort(function(a,b) {
			return b.getModel().getSubmissionTime() - a.getModel().getSubmissionTime();
		});

		$("#historyui").empty();
		for(var i = 0; i < queryListViews.length; i++) {
			$("#historyui").append(queryListViews[i].getView());
			queryListViews[i].updateView();
		}
	}
}