var Session = function() {
	//Check if the cookie exists
	var sessionHandle = null;
	
	if(docCookies.hasItem("publicId") && docCookies.hasItem("secretId")) {
		sessionHandle = {
			publicId: docCookies.getItem("publicId"),
			secretId: docCookies.getItem("secretId")
		}
	}

	//Checks if the user has logged in
	this.isLoggedIn = function() {
		return sessionHandle !== null && sessionHandle.hasOwnProperty("publicId") && sessionHandle.hasOwnProperty("secretId");
	};

	//Authenticates the user and generates a session handle
	this.logIn = function(username, password, callback) {
		$.ajax({
			url: util.SESSION_URL,
			type: 'POST',
			dataType: 'json',
			contentType: false,
			processData: false,
			cache: false,
			data: util.createMultipart({username: username, password: password}),
			success: function(data) {
				console.log(data);
				if(data.hasOwnProperty("publicId") && data.hasOwnProperty("secretId")) {
					sessionHandle = data;
					docCookies.setItem("publicId", sessionHandle["publicId"]);
					docCookies.setItem("secretId", sessionHandle["secretId"]);
				}
				else
					console.log("Error authenticating user: " + data);
				if(util.isFunction(callback))
					callback();
			},
			error: function(jqXHR, textStatus, errorThrown) {
				console.log("Error authenticating user: " + textStatus);
				if(util.isFunction(callback))
					callback();
			}
		});
	};

	this.getAllQueries = function() {

	};

	this.getAvailableMeta = function() {

	};

	this.searchMeta = function() {

	};

	this.getSessionHandle = function() {
		return sessionHandle;
	}

	this.submitQuery = function(query, callback) {
		if(this.isLoggedIn()) {
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
					console.log("Request success");
					var myQuery = null;

					if(data.hasOwnProperty("type") && data.hasOwnProperty("handleId") && data["type"] === "queryHandle") {
						console.log("Query submitted successfuly");
						queryHandle = data["handleId"];
						myQuery = new Query(queryHandle);
					}
					else
						console.log("Error sending query request: " + data);
					
					if(util.isFunction(callback))
						callback(myQuery);
				},
				error: function(jqXHR, textStatus, errorThrown) {
					console.log("Error sending query request: " + data);
					if(util.isFunction(callback))
						callback(null);
				}
			});
		}
		else
			callback("User not logged in");
	};
};