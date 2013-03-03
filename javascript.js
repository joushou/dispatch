
Array.prototype.each = function(f) {
	for(var i = 0, len = this.length; i < len; ++i)
		if(f(this[i], i, len)) return true;
	return false;
}

var basicAJAX = function(url, data, method, cb) {
	if(!method) method = 'GET';
	var xhr = new XMLHttpRequest();
	xhr.onreadystatechange = function(){
		switch(xhr.readyState) {
			case 4: // This means taht the response is ready - error or not
				if (xhr.status === 200) cb(xhr.responseText);
			case 3: // Bah, can't remember these. Look em' up.
			case 2: // ... It's not like they're interesting anyway.
			case 1:
			default:
				break;
		}
	}
	xhr.open(method, url, true);
	xhr.send(data);
}

var sidebar = function(s) {
	s.className = 'sidebar';
	var items = [];
	var selected = '';
	var callback = function(){};

	var updateSelection = function(id, noCallback) {
		items.each(function(k){
			if(k.id === id) k.node.className = "sidebar_item sidebar_selected";
			else k.node.className = "sidebar_item";
		});
		selected = id;
		if(!noCallback) callback(id);
	};

	var makeClickHandler = function(id, node) {
		node.onclick = function() {
			updateSelection(id);
		};
	};

	var cleanupItems = function() {
		var x = items;
		items = [];
		x.each(function(k){ if(k.clean) items.push(k); });
	}

	var removeItem = function(k) {
		if(selected = k.id && items.length > 1) {
			var i = items.indexOf(k);
			if(i === 0) ++i;
			var x = items[i-1];
			updateSelection(x.id, true);
		}
		s.removeChild(k.node);
	}

	var addItem = function(id, name, descr) {
		var found = false;
		if (items.each(function(k){if(k.id === id) { k.clean = true; return true;} })) return;
		var div1 = document.createElement('DIV');
		var div2 = document.createElement('DIV');
		var t1 = document.createTextNode(name);
		var t2 = document.createTextNode(descr);
		div1.className = 'sidebar_item';
		div2.className ='sidebar_descr';
		div2.appendChild(t2);
		div1.appendChild(t1);
		div1.appendChild(div2);
		makeClickHandler(id, div1);
		s.appendChild(div1);
		items.push({id: id, name: t1, descr: t2, node: div1, clean: true});
	};

	var update = function(arr) {
		items.each(function(k){ k.clean = false; });
		arr.each(function(k){
			addItem(k, 'Client', k);
		});
		items.each(function(k){if(!k.clean)removeItem(k);});
		cleanupItems();
	}

	var setCallback = function(cb) {
		callback = cb;
	}

	return {
		update: update,
		setCallback: setCallback
	};
}

var tableview = function(s){
	s.className = 'tableview';
	var table = undefined;
	var items = [];
	var initialize = function() {
		var t = document.createElement("TABLE");
		var thead = document.createElement("THEAD");
		var tbody = document.createElement("TBODY");
		var tfoot = document.createElement("TFOOT");
		t.appendChild(thead);
		t.appendChild(tbody);
		t.appendChild(tfoot);
		table = {head: thead, body: tbody, foot: tfoot};
		s.appendChild(t);
		makeHeader(['name', 'id', 'description']);
	}

	var makeHeader = function(a){
		a.each(function(i){
			var th = document.createElement("TH");
			th.innerHTML = i;
			table.head.appendChild(th);
		})
	}
	var addItem = function(id, name, description) {

	}
	return {
		initialize: initialize,
		addItem: addItem
	};
}
