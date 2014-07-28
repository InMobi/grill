var Session = function() {
	//Check if the cookie exists
	var sessionHandle = null;
	
	if(docCookies.hasItem("sessionHandle")) {
		sessionHandle = docCookies.getItem("sessionHandle");
	}

	//Checks if the user has logged in
	this.isLoggedIn = function() {
		return sessionHandle !== null;
	};

	//Authenticates the user and generates a session handle
	this.logIn = function(username, password, callback) {
		//TODO implementation
		$.ajax({
			url: Util.SESSION_URL,
			type: 'POST',
			data: {username: username, password: password},
			success: function(data) {
				console.log(data);
			}
		});

		if(Util.isFunction(callback))
			callback();
	};

	this.getAllQueries = function() {

	};

	this.getAvailableMeta = function() {

	};

	this.searchMeta = function() {

	};
};