"use strict";

var util = require('util'),
    EventEmitter = require('events').EvenetEmitter,
    cluster = require('cluster'),
    winston = require('winston'),
    http = require('http'),
    aws = require('plata'),
    log = winston.loggers.add('mate', {
        'console': {
            'level': 'error',
            'timestamp': true
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
    this.maxTime = 300000;

    this.queue = aws.sqs.Queue(this.queueName);
    this.queue.on('ready', function(details){
        self.maxTimeout = details.visibilityTimeout;
        log.info('Setting timeout to ' + self.maxTimeout);
    });
    this.queue.on('message', function(message){
        // Figure out who to send it to.
        var worker = self.accquireHand();

        // If no more free hands, stop listening for new messages.

        // Send the message off to the worker.
        self.pendingMessages[message.body.id] = message;
        worker.send(message.body);
        log.info('Message sent to worker.  Setting supervisor.');

        // Start supervisor kill timeout.
        self.supervisors[worker.id] = setTimeout(function(){
            log.info('Supervisor timeout for hand ' + worker.id);
            self.killHand(worker.id);
            self.startWorker();
            self.ensureListening();
        }, self.maxTime);

        if(!self.hasFreeHands()){
            self.queue.close();
            self.queueListening = false;
        }
    });

    this.queueListening = false;

    this.server = http.createServer(function(req, res){
        if(req.url.indexOf('shutdown') > -1){
            log.info('Got shutdown call');
            self.close();
        }
        res.writeHead(200, {'Content-Type': 'text/plain'});
        res.end('okay');
    });
    this.server.listen(10000);

    this.ensureListening();
}

Mate.prototype.hasFreeHands = function(){
    return Object.keys(this.freeWorkers).length > 0;
};

Mate.prototype.startWorker = function(initial){
    if(!this.hasFreeHands()  && initial !== true){
        log.warn('No free hands.  not starting worker sucker.');
        return;
    }
    var worker = cluster.fork();
    worker.on('message', function(msg){
        this.handleMessageFromHand(worker.id, msg);
    }.bind(this));
    this.freeWorkers[worker.id] = true;
    log.info('Started new hand ' + worker.id);
    log.info(Object.keys(cluster.workers).length + ' hands now in cluster.');
    return worker;
};

Mate.prototype.allHandsOnDeck = function(){
    for (var i = 0; i < this.numHands; i++){
        this.startWorker(true);
    }

    log.info('Started ' + Object.keys(cluster.workers).length + ' hands');
    return this;
};

Mate.prototype.handleMessageFromHand = function(handId, msg){
    if(msg.action === "FEEDME"){
        log.info('Hand ' + handId + ' requesting work.');
        this.freeHand(handId);
        this.ensureListening();
    }
    else if(msg.action === "RELEASE"){
        log.info('Hand ' + handId + ' sent ack for parition ' + msg.partitionId);
        clearTimeout(this.supervisors[handId]);
        this.freeHand(handId);
        this.pendingMessages[msg.partitionId].ack();
    }
};

Mate.prototype.ensureListening = function(){
    if(!this.queueListening){
        this.queue.listen(1000);
        this.queueListening = true;
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
    try{
        cluster.workers[handId].destroy();
        log.info('Killed hand ' + handId);
    }
    catch(e){
        log.info('Hand already dead.');
    }
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