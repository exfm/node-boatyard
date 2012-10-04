"use strict";
var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    mongo = require('mongodb'),
    sequence = require('sequence'),
    when = require('when'),
    dgram  = require('dgram'),
    winston = require('winston'),
    cluster = require('cluster');

winston.loggers.add('shipyard', {
    console: {
        'level': 'silly', 'timestamp': true, 'colorize': true
    }
});

var log = winston.loggers.get('shipyard');

var PARTITION_STATE = {
    'AVAILABLE': 0,
    'PROCESSING': 1,
    'COMPLETE': 2,
    'ERROR': -1
};

function Partition(id, start, stop){
    this.id = id;
    this.start = start;
    this.stop = stop;
    this.state = PARTITION_STATE.AVAILABLE;
}
util.inherits(Partition, EventEmitter);

Partition.prototype.accquire = function(workerId, mateId){
    this.mateId = mateId;
    this.workerId = workerId;
    this.state = PARTITION_STATE.PROCESSING;
    this.emit('accquired');
};

Partition.prototype.error = function(msg){
    this.state = PARTITION_STATE.ERROR;
    this.emit('error', msg);
};

Partition.prototype.release = function(){
    this.state = PARTITION_STATE.COMPLETE;
    this.emit('complete');
};

Partition.prototype.progress = function(total, complete, error, msg){
    this.emit('progress');
};

Partition.prototype.toString = function(){
    return util.format("Partition(id=%d, start=%d, stop=%d)",
        this.id, this.start, this.stop);
};

// Captain dishes out partitions to mates
function Captain(mongoHosts, dbName, collectionName, partitionSize){
    this.mongoHosts = mongoHosts;
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.partitionSize = partitionSize;
    this.partitionCount = 0;
    this.partitions = {};

    this.db = {};
    this.collection = {};
    this.total = 0;

    this.availablePartitions = [];
    this.completedPartitions = [];
}

Captain.prototype.getRandomMongoHost = function(){
    return this.mongoHosts[Math.floor(Math.random()*this.mongoHosts.length)];
};

Captain.prototype.startShip = function(){
    // How many things do we need to chew through?
    log.silly('Starting ship....');
    this.db = new mongo.Db(this.dbName,
        new mongo.Server(this.mongoHosts[0], 27017, {}),
        {'native_parser':true, 'slave_ok': true});

    sequence(this).then(function(next){
        this.db.open(next);
    }).then(function(next, err, db){
        db.collection(this.collectionName, next);
    }).then(function(next, err, collection){
        this.collection = collection;
        this.collection.count(next);
    }).then(function(next, err, count){
        this.total = count;
        this.partitionCount = Math.ceil(this.total/this.partitionSize);
        for(var i = 0; i< this.partitionCount; i++){
            this.partitions[i] = new Partition(i,
                i*this.partitionSize, (i*this.partitionSize) + this.partitionSize);
            this.availablePartitions.push(i);
        }
        log.silly('Determined Partitions: ', this.partitions);
        this.startServer();
    });
};

Captain.prototype.startServer = function(){
    this.server = dgram.createSocket('udp4', function (msg, rinfo) {


        log.info('Got message: ' + msg.toString(), rinfo);
        msg = JSON.parse(msg.toString());
        var workerAddress = rinfo.address,
            data = [],
            partitionId = -1,
            statusMessage = "",
            completed = -1,
            total = -1,
            errored = -1,
            mateId = msg.mate,
            handId = msg.hand;


        if(msg.action === "FEEDME"){
            // Pick a partition and pass it back to the worker
            var client = dgram.createSocket("udp4"),
                partition,
                message;

            if(this.availablePartitions.length){
                partitionId = this.availablePartitions.shift();
                partition = this.partitions[partitionId];

                log.info(util.format('Giving partition %s to hand %s on mate %s',
                    partition, handId, mateId));

                message = new Buffer(JSON.stringify({
                    'action': "EAT",
                    'mate': mateId,
                    'hand': handId,
                    'partitionId': partitionId,
                    'start': partition.start,
                    'stop': partition.stop,
                    'mongoHost': this.getRandomMongoHost(),
                    'dbName': this.dbName,
                    'collectionName': this.collectionName
                }));
            }
            else{
                message = new Buffer(JSON.stringify({
                    'action': "EMPTY",
                    'mate': mateId,
                    'hand': handId
                }));

                log.info('Available partitions empty.');
                log.info(util.format('Sending empty to kill hand %s on mate %s',
                    handId, mateId));
            }
            client.send(message, 0, message.length, 9001, rinfo.address, function(err, bytes) {
                client.close();
            });
        }
        else if(msg.action === 'PROGRESS'){
            this.partitions[msg.partitionId].progress(msg.total, msg.completed,
                msg.errored, msg.statusMessage);
            // Calculate total job progress

        }
        else if(msg.action === 'RELEASE'){
            this.partitions[msg.partitionId].release();
        }
        else if(msg.action === 'ERROR'){
            this.partitions[msg.partitionId].error(msg.message);
        }
        else{
            log.error('Dont know how to handle message: ', msg);
        }
    }.bind(this));
    this.server.bind(9000);
    log.info('Captain server started on port 9000.');
};

module.exports.Captain = Captain;

function Mate(id, captainHost, numHands){
    this.id = id;
    this.captainHost = captainHost;
    this.numHands = numHands;
}
util.inherits(Hand, EventEmitter);

Mate.prototype.allHandsOnDeck = function(){
    for (var i = 0; i < this.numHands; i++){
        cluster.fork();
    }

    Object.keys(cluster.workers).forEach(function(id) {
        cluster.workers[id].on('message', function(msg){
            this.handleMessageFromHand(id, msg);
        }.bind(this));
    }.bind(this));

    log.info('Started ' + Object.keys(cluster.workers).length + ' hands');

    this.server = dgram.createSocket('udp4', function (msg, rinfo) {
        log.silly('Got message from captain ' + msg.toString(), rinfo);

        msg = JSON.parse(msg.toString());
        if(msg.action === "EMPTY"){
            if(cluster.workers.hasOwnProperty(msg.hand)){
                this.killHand(msg.hand);
            }
            if(Object.keys(cluster.workers).length === 0){
                log.info('All hands relieved.  Jumping ship.');
                this.server.close();
            }
            return;
        }
        this.tellHand(msg.hand, msg);

    }.bind(this));
    this.server.bind(9001);
    return this;
};

Mate.prototype.tellCaptain = function(handId, msg){
    var client = dgram.createSocket("udp4"),
        message = new Buffer(JSON.stringify(msg));
    log.silly("Telling captain for hand " + handId + ": " + message.toString());
    client.send(message, 0, message.length, 9000, this.captainHost, function() {
        client.close();
    });
};

Mate.prototype.tellHand = function(handId, msg){
    cluster.workers[handId].send(msg);
};

Mate.prototype.handleMessageFromHand = function(handId, msg){
    if(msg.action === "FEEDME"){
        this.accquire(handId);
    }
    else if(msg.action === "PROGRESS"){
        this.progress(handId, msg.partitionId, msg.total,
            msg.completed, msg.errored, msg.message);
    }
    else if(msg.action === "RELEASE"){
        this.release(handId, msg.partitionId);
    }
    else if(msg.action === "ERROR"){
        this.error(handId, msg.partitionId, msg.message);
    }
};

Mate.prototype.accquire = function(handId){
    this.tellCaptain(handId, {
        'action': "FEEDME",
        'mate': this.id,
        'hand': handId
    });
};

Mate.prototype.progress = function(handId, partitionId, total, completed, errored, message){
    this.tellCaptain(handId, {
        'action': "PROGRESS",
        'partitionId': partitionId,
        'total': total,
        'completed': completed,
        'errored': errored,
        'message': message
    });
};

Mate.prototype.release = function(handId, partitionId){
    this.tellCaptain(handId, {
        'action': "RELEASE",
        'partitionId': partitionId
    });
};

Mate.prototype.killHand = function(handId){
    cluster.workers[handId].destroy();
    log.info('Killed hand ' + handId);
};

module.exports.Mate = Mate;


function Hand(mateId){
    this.mateId = mateId;
    this.partitionId = -1;
    this.id = cluster.worker.id;
}
util.inherits(Hand, EventEmitter);

Hand.prototype.tellMate = function(message){
    message.hand = this.id;
    process.send(message);
};

Hand.prototype.getToWork = function(task){
    process.on('message', function(msg){
        log.info('Worker got message', msg);
        this.partitionId = msg.partitionId;
        task.apply(this, [msg]);
    }.bind(this));
    this.getWorkToDo();
};

Hand.prototype.getWorkToDo = function(){
    process.send({'action': 'FEEDME'});
};

Hand.prototype.progress = function(total, completed, errored, message){
    this.tellMate({
        'action': "PROGRESS",
        'total': total,
        'completed': completed,
        'errored': errored,
        'message': message,
        'partitionId': this.partitionId
    });
};

Hand.prototype.error = function(message){
    this.tellMate({
        'action': "ERROR",
        'message': message
    });
};

Hand.prototype.release = function(){
    this.tellMate({
        'action': "RELEASE",
        'partitionId': this.partitionId
    });
};


module.exports.Hand = Hand;