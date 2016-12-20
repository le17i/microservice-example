var open = require('amqplib').connect('amqp://localhost');
var mongoose = require('mongoose');
var queues =  {
    getAll: 'getContacts',
    getById: 'getContact',
    create: 'postContact'
};

mongoose.connect('mongodb://' + (process.env.MONGO_PORT_27017_TCP_ADDR || 'localhost') + ':27017/db');
var Contact = mongoose.model('Contact', { name: String });

open
    .then((conn) => conn.createChannel())
    .then((ch) => {
        ch.assertQueue(queues.getAll).then(
            (ok) => ch.consume(queues.getAll, (msg) => {
                if(msg !== null) {                    
                    Contact.find((err, data) => {
                        let reply = null;
                        if(err) {
                            reply = new Buffer(JSON.stringify({ error: err, status: 500 }));
                        }
                        else {
                            reply = new Buffer(JSON.stringify({ data: data, status: 200 }));
                        }

                        ch.sendToQueue(msg.properties.replyTo, reply, { correlationId: msg.properties.correlationId });
                        ch.ack(msg);
                    });
                }
            })
        );

        ch.assertQueue(queues.getById).then(
            (ok) => ch.consume(queues.getById, (msg) => {
                if(msg !== null) {
                    let params = JSON.parse(msg.content.toString());
                    Contact.findById({ _id: params.id }, (err, data) => {
                        let reply = null;
                        if(err) {
                            reply = new Buffer(JSON.stringify({ error: err, status: 500 }));
                        }
                        else {
                            reply = new Buffer(JSON.stringify({ data: data, status: 200 }));
                        }

                        ch.sendToQueue(msg.properties.replyTo, reply, { correlationId: msg.properties.correlationId });
                        ch.ack(msg);
                    });
                }
            })
        );

        ch.assertQueue(queues.create).then(
            (ok) => ch.consume(queues.create, (msg) => {
                if(msg !== null) {
                    let reply = null;
                    let params = JSON.parse(msg.content.toString());
                    let name = params.name;
                    let newContact = new Contact({ name: name });

                    newContact.save((err, data) => {
                        if(err) {
                            reply = new Buffer(JSON.stringify({ error: err, status: 500 }));
                        }
                        else {
                            reply = new Buffer(JSON.stringify({ data: data, status: 201 }));
                        }

                        ch.sendToQueue(msg.properties.replyTo, reply, { correlationId: msg.properties.correlationId });
                        ch.ack(msg);
                    });
                }
            })
        );
    })
    .catch(console.warn);