document.getElementById("getDatabaseButton").onclick = function () {
    let url = "http://localhost:8080/webapi/users";
    let xhr = new XMLHttpRequest();
    xhr.open("GET", url, false);
    xhr.setRequestHeader('Content-type','application/json; charset=utf-8');
    xhr.send(null);
    let userList = JSON.parse(xhr.responseText);
    let x = document.getElementById("DatabaseTable");
    let nbRows = x.rows.length-1;
    for(let i = 0; i < nbRows; i++){
        x.deleteRow(1);
    }
    for(let j = 0; j < userList.length; j++){
        let row = x.insertRow(j+1);
        let cell1 = row.insertCell(0);
        let cell2 = row.insertCell(1);
        cell1.innerHTML = userList[j].firstname;
        cell2.innerHTML = userList[j].lastname;
    }
};

document.getElementById("deleteButton").onclick = function () {
    let firstname = document.getElementById("firstname").value;
    let lastname = document.getElementById("lastname").value;
    let url = "http://localhost:8080/webapi/user/"+firstname+"/"+lastname;
    let xhrDelete = new XMLHttpRequest();
    xhrDelete.open("DELETE", url, true);
    xhrDelete.setRequestHeader('Content-type','application/json; charset=utf-8');
    xhrDelete.send(null);
};

document.getElementById("deleteAllButton").onclick = function () {
    let url = "http://localhost:8080/webapi/users/";
    let xhr = new XMLHttpRequest();
    xhr.open("DELETE", url, false);
    xhr.setRequestHeader('Content-type','application/json; charset=utf-8');
    xhr.send(null);
};

document.getElementById("submitButton").onclick = function () {
    let url = "http://localhost:8080/webapi/user/";
    let xhr = new XMLHttpRequest();
    let fn = document.getElementById("firstname").value;
    let ln = document.getElementById("lastname").value;
    let data = JSON.stringify({firstname: fn, lastname: ln});
    xhr.open("POST", url, true);
    xhr.setRequestHeader('Content-type','application/json; charset=utf-8');
    xhr.send(data);
};

document.getElementById("updateButton").onclick = function () {
    let xhr = new XMLHttpRequest();
    let prevFn = document.getElementById("prevFirstname").value;
    let prevLn = document.getElementById("prevLastname").value;
    let newFn = document.getElementById("newFirstname").value;
    let newLn = document.getElementById("newLastname").value;
    let url = "http://localhost:8080/webapi/user/"+prevFn+"/"+prevLn;
    let data = JSON.stringify({firstname: newFn, lastname: newLn});
    xhr.open("PUT", url, true);
    xhr.setRequestHeader('Content-type','application/json; charset=utf-8');
    xhr.send(data);
};