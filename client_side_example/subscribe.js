const async_mqtt = require("async-mqtt")

// const host = "localhost";
// const port = 1883;

const host = "gps.transo.in";
const port = 5022;

const client = async_mqtt.connect(`ws://${host}:${port}`, {
    clientId: "WebStorm Subscriber",
    protocol: "ws"
});

let server_type, service_type, companyID, vehicleID;
const topic = 'app/#';

client.on('connect', async function () {
    console.log("Connected");
    client.subscribe(topic).then(r => {
        try {
            r.forEach((entry) => {
                console.log("Subscribed to ", entry.topic);
            })
        } catch (e) {
            console.log(e.stack);
            process.exit();
        }
    })
})

client.on('close', async function() {
    console.log("Closed");
})

client.on('disconnect', async function () {
    console.log("Disconnected");
})

client.on('message', async function (topic, message) {
    console.log(topic + " sends " + message.toString());
    const topic_list = topic.toString().split('/');
    // await process(topic_list[0], topic_list[2], topic_list[3], message);
})

const process = async function (server_type, company_id, vehicle_id, message) {
    console.log(server_type, company_id, vehicle_id, message.toString())
};
