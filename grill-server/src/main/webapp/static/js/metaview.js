var MetaView = function(meta) {
	var model = meta;

	this.getView = function() {
		return $("<li>", {
			class: "list-group-item list-group-item-" + getClass(),
			text: model.getName()
		});
	}
	
	var getClass = function() {
		if(model.getType() === "Cube") {
			return "info";
		}
		else if(model.getType() === "DimensionTable") {
			return "success";
		}
		else if(model.getType() === "StorageTable") {
			return "warning";
		}
		return "default";
	}
}