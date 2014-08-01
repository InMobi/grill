var Meta = function(metaName, metaType) {
	var name = metaName;
	var type = metaType;
	var clicked = false;
    var columns = [];

	this.getName = function() {
		return name;
	}

	this.getType = function() {
		return type;
	}

	this.getColumns = function(){
	    return columns;
	}

	this.setClicked = function() {
    	    clicked = !clicked;
    	}

    	this.getClicked = function() {
    	    return clicked;
    	}

	this.addChild =function(child){
        columns.push(child);
	}
}