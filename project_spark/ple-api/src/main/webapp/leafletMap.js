var mymap = L.map('mapid', {
    center :[0,0],
    zoom: 0,
    maxZoom: 10
});

L.tileLayer('http://vanhalen:8087/webapi/{z}/{x}/{y}')
    .addTo(mymap);

function onMapClick(e) {
		popup
			.setLatLng(e.latlng)
			.setContent("You clicked the map at " + e.latlng.toString())
			.openOn(mymap);
	}

mymap.on('click', onMapClick);
