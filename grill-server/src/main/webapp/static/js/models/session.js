var Session = function() {
	//Check if the cookie exists
	var sessionHandle = null;
	var userName = "";
	
	if(docCookies.hasItem("publicId") && docCookies.hasItem("secretId") && docCookies.hasItem("userName")) {
		sessionHandle = {
			publicId: docCookies.getItem("publicId"),
			secretId: docCookies.getItem("secretId")
		}
		userName = docCookies.getItem("userName");
	}

	//Checks if the user has logged in
	this.isLoggedIn = function() {
		return sessionHandle !== null && sessionHandle.hasOwnProperty("publicId") && sessionHandle.hasOwnProperty("secretId");
	};

	//Authenticates the user and generates a session handle
	this.logIn = function(username, password, callback) {
		userName = username;
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
					docCookies.setItem("userName", userName);
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

	this.getAllQueries = function(callback) {
		$.ajax({
			url: util.QUERY_URL,
			type: 'GET',
			dataType: 'json',
			data: {
				publicId: session.getSessionHandle()["publicId"],
				user: userName
			},
			success: function(data) {
				if(data !== null && data.length > 0) {
					if(util.isFunction(callback))
						callback(data);
				}
			}
		});
	};

		this.getAvailableMeta = function(callback) {
            $.ajax({
                url: util.META_URL,
                type: 'GET',
                dataType: 'json',
                data: {publicId: session.getSessionHandle()["publicId"]},
                success: function(tableList){
                    var metaArr = [];
                    for(var item in tableList)
                    {
                        console.log(tableList[item]);
                        var name = tableList[item].name;
                        var type = tableList[item].type;
                        var metaObj = new Meta(name,type);
                        metaArr.push(metaObj);
                    }
                    if(util.isFunction(callback))
                        callback(metaArr);
                }
            });
    	};

    	this.getCubeMeta = function(cubeName, callback)
    	{
    	    $.ajax({
    	        url: util.META_URL+"/"+cubeName+"/cube",
                type: 'GET',
                dataType: 'json',
                data: {publicId: session.getSessionHandle()["publicId"]},
                success: function(tableList){
                 var cubeArr = [];
                 for(var item in tableList)
                 {
                     console.log(tableList[item]);
                     var name = tableList[item].name;
                     var type = tableList[item].type;
                     var metaObj = new Meta(name,type);
                     cubeArr.push(metaObj);
                 }
                 if(util.isFunction(callback))
                     callback(cubeArr);
                }
    	    });
    	};


    	this.getDimtableMeta = function(dimtableName, callback)
        	{
        	    $.ajax({
        	        url: util.META_URL+"/"+dimtableName+"/dimtable",
                    type: 'GET',
                    dataType: 'json',
                    data: {publicId: session.getSessionHandle()["publicId"]},
                    success: function(tableList){
                     var dimArr = [];
                     for(var item in tableList)
                     {
                         console.log(tableList[item]);
                         var name = tableList[item].name;
                         var type = tableList[item].type;
                         var metaObj = new Meta(name,type);
                         dimArr.push(metaObj);
                     }
                     if(util.isFunction(callback))
                         callback(dimArr);
                    }
        	    });
        	};



    	this.searchMeta = function(keyword, callback) {
            $.ajax({
                url: util.META_URL+"/"+keyword,
                type: 'GET',
                dataType: 'json',
                data: {publicId: session.getSessionHandle()["publicId"]},
                success: function(tableList){
                    var metaArr = [];
                    for(var item in tableList)
                    {
                        console.log(tableList[item]);
                        var name = tableList[item].name;
                        var type = tableList[item].type;
                        var childArr = [];
                        childArr = tableList[item].columns;
                        var metaObj = new Meta(name,type);
                        for (var col in childArr)
                        {
                            var childName = childArr[col].name;
                            var childType = childArr[col].type;
                            var metaChildObj = new Meta(childName,childType);
                            metaObj.addChild(metaChildObj);
                        }
                        metaArr.push(metaObj);
                    }
                    if(util.isFunction(callback))
                        callback(metaArr);
                }
            });
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
					console.log("Error sending query request: " + textStatus);
					if(util.isFunction(callback))
						callback(null);
				}
			});
		}
		else
			callback("User not logged in");
	};
};
