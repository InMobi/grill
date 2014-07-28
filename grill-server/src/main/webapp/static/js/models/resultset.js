var ResultSet = function(rows) {
	var totalRows = rows;
	var startIndex = 1;
	var fetchSize = 10;

	//Fetch n number of results starting from start index
	this.getRows = function(start, n, callback) {

	};

	//Check if there are more rows available and then return the next rows
	this.getNextRows = function(callback) {
		if(startIndex < totalRows ) {
			this.getResultsFrom(startIndex, fetchSize, callback);
			startIndex += fetchSize;
		}
		else {
			callback(null);
		}
	};

	this.setFetchSize = function(size) {
		fetchSize = size;
	};

	this.getFetchSize = function() {
		return fetchSize;
	};

	this.reset = function() {
		startIndex = 1;
	};

	this.setTotalRows = function(total) {
		totalRows = total;
	};

	this.getTotalRows = function() {
		return totalRows;
	};
};