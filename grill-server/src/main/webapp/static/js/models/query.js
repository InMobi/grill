var Query = function() {
	var queryHandle;
	var queryStatus;

	this.submitQuery = function(query) {
		//Submit query using ajax
		$.ajax({
			url: util.QUERY_URL,
			type: 'POST',
			dataType: 'json',
			contentType: false,
			processData: false,
			cache: false,
			data: util.createMultipart({ 
				publicId: session.getSessionHandle()["publicId"], 
				query: query
			}),
			success: function(data) {
				console.log(data);
			},
			error: function(jqXHR, textStatus, errorThrown) {

			}
		});
	};

	this.isQuerySubmitted = function() {
		return queryHandle !== null;
	};

	this.getResultSet = function() {

	};
};