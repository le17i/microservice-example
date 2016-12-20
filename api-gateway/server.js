var open = require('amqplib').connect('amqp://localhost');
var express = require('express');
var bodyParser = require('body-parser');

var queues =  {
    getAll: 'getContacts',
    getById: 'getContact',
    create: 'postContact'
};

var app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

var generateUuid = () => '${Math.random()}${Math.random()}${Math.random()}';

function consumeQueue(queueID, channel, res) {
    return channel.consume(
        queueID,
        (msg) => {
            if(msg !== null) {
                let data = JSON.parse(msg.content.toString())
                return res.status(data.status).json(data);
            }
        },
        {
            noAck: true
        }
    );
}

function sendToQueue(queueID, replyID, params, channel) {
    return channel.sendToQueue(
        queueID,
        new Buffer(JSON.stringify(params)),
        {
            correlationId: generateUuid(),
            replyTo: replyID
        }
    );
}

function assertQueue(queueID, params, channel, res) {
    return channel.assertQueue(queueID).then((ok) => {
        consumeQueue(ok.queue, channel, res);
        sendToQueue(queueID, ok.queue, params, channel);
    });
}

function callService(queueID, params, res) {
    open
        .then((conn) => conn.createChannel())
        .then((ch) => assertQueue(queueID, params, ch, res))
        .catch(console.warn);
}

app.get('/', (req, res) => callService(queues.getAll, null, res));

app.get('/:id', (req, res) => callService(queues.getById, { id: req.params.id.toString() }, res))

app.post('/', (req, res) => callService(queues.create, { name: req.body.name }, res))

app.listen(5000, () => console.info('Server starting in port 5000'));