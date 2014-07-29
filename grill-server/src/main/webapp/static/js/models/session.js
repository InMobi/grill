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
				callback();
			},
			error: function(jqXHR, textStatus, errorThrown) {
				console.log("Error authenticating user: " + textStatus);
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
};