'use strict'
const admin = require('firebase-admin');
const firebase = require('firebase')
const getData = async (db)=> {
    const snapshot = await db.collection('generators').get();
    snapshot.forEach((doc) => {
        console.log(doc.id, '=>', doc.data());
    });
}

const updateSystem = (db, system)=> {
    db.collection("systemStatus").doc('system').update(system);
}
const updateLoad = (db, load)=> {

                const id = load?.id;
                let loadObj = {
                    status: load?.state == "true",
                    lastHB: new Date().getTime(),
                }
                if (load?.value) {
                    loadObj['latestPowerReading'] = load.value;
                }
                if (load?.ValueLoad) {
                    loadObj['latestPowerReading'] = load.ValueLoad;
                }
                console.log(id, loadObj);
                db.collection("loads").doc(id).update(loadObj, {merge: true});

}
const updateLoads = (db, loads)=> {

        Object.keys(loads).map((loadBus)=>{
        const busLoads = loads[loadBus][loadBus][0];
            busLoads.map((load, index)=> {
                const id = load?.relaynames;
                const loadObj = {
                    status: load?.relaystate == "true",
                    latestPowerReading: load?.ValueLoad,
                    lastHB: new Date().getTime(),
                }
                db.collection("loads").doc(id).update(loadObj, {merge: true});
            })
    })
}
const updateLoadBuses = (db, loadBuses)=> {
    loadBuses.map((bus)=>{
        const id = bus['VBLnames'];
        let updateObject = {
            name : bus['VBLnames'],
            VL: parseFloat(bus['VL'])
        }
        db.collection("loadBuses").doc(id).set(updateObject, { merge: true });
    })
}
const updateGenerators = (db, generators)=> {
    generators.map((generator)=>{
        const id = generator['VBGnames'].substr(1);
        let updateObject = {
            bus : generator['VBGnames'],
            name : id,
            producedPower: parseFloat(generator['PowerG']),
            status: (generator['Gstate'] == 'true'),
            vbs: parseFloat(generator['VG'])
        }
        if (generator['nominalPower']) {
            updateObject['nominalPower'] = parseFloat(generator['nominalPower'])
        }
        db.collection("generators").doc(id).set(updateObject, { merge: true });
    })
}
try {
    const serviceAccount = require('./iot-dashboard-b1512-a8a81dc3a4f8.json');
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount)
    });

} catch (e) {
    console.log(e);
}

const db = admin.firestore();
const users = {
    espUser: {
        role: 'microcontroller',
        password: '3R_fv!PvDyDnD64wB@-$e9k'
    },
    feUser: {
        role:'frontend',
        password: 'Y=^j*kj7X3mnurXy&UJx7qJ'
    },
    beUser: {
        role: 'backend',
        password: '7G@9hcHT4keCtX3brt#c6G9'
    },
    matlab: {
        role: 'matlab',
        password: 'UKR5Kr+bd@$gJYpY89jjHgA'
    }
}
const permissions = {
    microcontroller: {
        subscribe: ['loads-control'],
        publish: ['loads-updates'],
    },
    frontend: {
        subscribe: ['loads-updates', 'system-update', 'alerts'],
        publish: ['loads-control'],
    },
    backend: {
        subscribe: ['loads-updates'],
        publish: ['loads-control'],
    },
    matlab: {
        subscribe: ['loads-updates' ],
        publish: ['loads-control', 'system-update', 'loads-updates', 'alerts'],
    }
}
let clients = {}
const fs = require('fs');
const aedes = require('aedes')({
        authenticate: (client, username, password, callback) => {
            const auth = (users.hasOwnProperty(username) && users[username]['password'] == password);
            if (auth) {
                clients[client.id] = {role: users[username]['role'], timestamp: new Date().getTime()}
            }
            return callback((!auth) ? new Error('Unauthorized user') : null, auth)
    },
    authorizePublish: (client, packet, callback) => {
            if (clients.hasOwnProperty(client.id)) {
                const allowedTopics = permissions[clients[client.id]['role']]['publish'];
                if (allowedTopics.length > 0 && allowedTopics.indexOf(packet.topic) >= 0) {
                    return callback(null)
                }
                return callback(new Error('Client not allowed to publish to this topic'));
            }
            return callback(new Error('Client not allowed to publish'));
    },

    authorizeSubscribe: (client, sub, callback) => {
        if (clients.hasOwnProperty(client.id)) {
            const allowedTopics = permissions[clients[client.id]['role']]['subscribe'];
            if (allowedTopics.length > 0 && allowedTopics.indexOf(sub.topic) >= 0) {
                return callback(null, sub)
            }
            return callback(new Error('Client not allowed to subscribe to this topic'));
        }
        return callback(new Error('Client not allowed to subscribe'));
    },
})
const server = require('net').createServer(aedes.handle)
const httpServer = require('http').createServer()
const ws = require('websocket-stream')
const port = process.env.PORT || 1883
const wsPort = process.env.PORT ||8888


server.listen(port, function () {
    console.log('server listening on port', port)
})


ws.createServer({
    server: httpServer
}, aedes.handle)

httpServer.listen(wsPort, function () {
    console.log('websocket server listening on port', wsPort)
})

aedes.on('clientError', function (client, err) {
    console.log('client error', client.id, err.message, err.stack)
})

aedes.on('connectionError', function (client, err) {
    console.log('client error', client, err.message, err.stack)
})

aedes.on('publish', function (packet, client) {
    if (client && packet?.topic) {
        const topic = packet?.topic;
        console.log('message from client', client.id)
        let msg = {};
        try {
             msg = JSON.parse(packet.payload.toString());
        } catch (e) {
            console.log(e)
        }

        if (topic === 'loads-updates') {
            if (msg?.id) {
                updateLoad(db, msg)
            } else {
                updateLoads(db, msg)
            }
        }
        if (topic === 'system-update') {
            const freq = parseFloat(msg?.systemState?.freq || 59);
            // eslint-disable-next-line max-len
            const generatorsUpdate = msg?.generators?.Generators?.length ? msg?.generators?.Generators[0] : [];
            const tmpLoadBuses = msg?.LoadBusses?.length ? msg?.LoadBusses[0] : [];
            updateLoadBuses(db, tmpLoadBuses);
            updateGenerators(db, generatorsUpdate);
            updateSystem (db, {freq});
        }
    }
})

aedes.on('subscribe', function (subscriptions, client) {
    if (client) {
        console.log('subscribe from client', subscriptions, client.id, clients[client.id])
    }
})

aedes.on('client', function (client) {
    console.log('new client', client.id, clients[client.id])
})