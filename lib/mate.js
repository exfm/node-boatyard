"use strict";

var util = require('util'),
    EventEmitter = require('events').EvenetEmitter,
    cluster = require('cluster'),
    winston = require('winston'),
    http = require('http'),
    aws = require('plata'),
    log = winston.loggers.add('mate', {
        'console': {
            'level': 'silly',
            'timestamp': true,
            'colorize': true
        },
        'file': {
            'filename': 'mate.log',
            'level': 'silly',
            'timestamp': true,
            'colorize': true
        }
    });


function Mate(id, nconf){
    this.id = id;
    this.queueName = nconf.get('queue-name');
    this.numHands = Number(nconf.get('hands'));
    this.secret = nconf.get('secret');
    this.key = nconf.get('key');

    aws.connect({'key': this.key, 'secret': this.secret});

    var self = this;

    this.freeWorkers = {};  // handId => true

    this.supervisors = {};  // handId => timeout

    this.pendingMessages = {}; // partitionId => SQS message.

    this.queue = aws.sqs.Queue(this.queueName);
    this.queue.on('message', function(message){
        // Figure out who to send it to.
        var worker = self.accquireHand(),
            maxTime = 30000;

        // If no more free hands, stop listening for new messages.
        if(!self.hasFreeHands()){
            log.info('No free hands.  Closing queue.');
            self.queue.close();
        }
        // log.info('Got a message!!!!', message);

        // Send the message off to the worker.
        self.pendingMessages[message.body.id] = message;
        worker.send(message.body);

        // Start supervisor kill timeout.
        self.supervisors[worker.id] = setTimeout(function(){
            self.killHand(worker.id);
            self.startWorker();
        }, maxTime);


    });

    this.queue.listen(1000);
    this.queueListening = true;

    this.server = http.createServer(function(req, res){
        if(req.url.indexOf('shutdown') > -1){
            log.info('Got shutdown call');
            self.close();
        }
        res.writeHead(200, {'Content-Type': 'text/plain'});
        res.end('okay');
    });
    this.server.listen(10000);
}

Mate.prototype.hasFreeHands = function(){
    return Object.keys(this.freeWorkers).length > 0;
};

Mate.prototype.startWorker = function(){
    var worker = cluster.fork();
    worker.on('message', function(msg){
        this.handleMessageFromHand(worker.id, msg);
    }.bind(this));
    this.freeWorkers[worker.id] = true;
    log.info('Started new hand ' + worker.id);
    return worker;
};

Mate.prototype.allHandsOnDeck = function(){
    for (var i = 0; i < this.numHands; i++){
        this.startWorker();
    }

    log.info('Started ' + Object.keys(cluster.workers).length + ' hands');
    return this;
};

Mate.prototype.handleMessageFromHand = function(handId, msg){
    if(msg.action === "FEEDME"){
        this.freeHand(handId);
        if(!this.queueListening){
            this.queue.listen(1000);
            this.queueListening = true;
        }
    }
    else if(msg.action === "RELEASE"){
        log.info('Hand ' + handId + ' send ack for parition ' + msg.partitionId);
        clearTimeout(this.supervisors[handId]);
        this.freeHand(handId);
        this.pendingMessages[msg.partitionId].ack();
    }
};

Mate.prototype.accquireHand = function(){
    var handId = Object.keys(this.freeWorkers)[0];
    delete this.freeWorkers[handId];
    return cluster.workers[handId];
};

Mate.prototype.freeHand = function(handId){
    this.freeWorkers[handId] = true;
};

Mate.prototype.killHand = function(handId){
    cluster.workers[handId].destroy();
    log.info('Killed hand ' + handId);
};

Mate.prototype.close = function(){
    var self = this;
    this.queue.close();
    this.queueListening = false;
    this.pendingMessages = {};

    Object.keys(this.supervisors).forEach(function(t){
        clearTimeout(self.supervisors[t]);
    });

    Object.keys(cluster.workers).forEach(function(handId){
        self.killHand(handId);
    });

    setTimeout(function(){
        self.server.close();
        log.info('Goodbye');
        process.exit(0);
    }, 500);
};



module.exports = Mate;