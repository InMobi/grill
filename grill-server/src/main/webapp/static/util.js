var Util = {};

Util.prototype.isFunction = function(functionToCheck) {
	var getType = {};
	return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
};

Util.prototype.SESSION_URL: "http://localhost:19999/session/", 