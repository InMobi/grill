var Meta = function(metaName, metaType) {
	var name = metaName;
	var type = metaType;

	this.getName = function() {
		return name;
	}

	this.getType = function() {
		return type;
	}
}