const express = require('express');
const md = require('markdown').markdown;
const ejs = require('ejs');
const morgan = require('morgan'); //to use the log of all the request
const cors = require('cors'); //for cross origin

const rootdir = '/home/lulu/Cours/semestre_9/S9_Web/td/td_06'

const DBASE_NAME = 'M2WEB'
const collection = 'contacts'
const MONGO_URL = 'mongodb://localhost:27017';

const MongoClient = require('mongodb').MongoClient;

const app = express();

app.use(cors());
app.use(morgan('combined'));

app.get('/', function(req, res) {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/html');
    res.sendFile('/ressources/index.html', {
        root: rootdir
    });
});

app.get('/users', function(req, res) {
    async_call(let contacts = allContacts());
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    res.json(contacts);
    console.log(contacts);
});

app.get('/user/:firstname/:lastname', function(req, res) {
    res.send('Bonjour utilisateur ' + req.params.firstname + ' ' + req.params.lastname);
});

app.use(function(req, res, next) {
    res.status(404).send('Page introuvable !');
});

async function allContacts() {
    try {
        const client = await MongoClient.connect(MONGO_URL, {
            useNewUrlParser: true
        });
        const db = client.db(DBASE_NAME);
        const contacts = db.collection(collection);
        let results = contacts.find();
        client.close();
        return results;
    } catch (e) {
        console.error(e)
    }
}

async function printAllContacts(collection) {
    const results = await collection.find().toArray();
    for (const result of results) {
        console.log(`Contact : ${result.name} is ${result.age}`);
    }
}

(async function() {
    try {
        const client = await MongoClient.connect(MONGO_URL, {
            useNewUrlParser: true
        });
        const db = client.db(DBASE_NAME);
        const contacts = db.collection(collection);
        printAllContacts(contacts);
        client.close();
    } catch (e) {
        console.error(e)
    }
})()


app.listen(8080, function() {
    console.log('Example app listening on port 8080!');
});