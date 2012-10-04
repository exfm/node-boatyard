"use strict";
var util = require('util'),
    EventEmitter = require('events').EventEmitter,
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

function Partition(captain, data){
    Object.keys(data).forEach(function(k){
        this[k] = data[k];
    }.bind(this));

    Object.keys(captain.extras).forEach(function(k){
        this[k] = captain.get(k);
    }.bind(this));

    this.state = PARTITION_STATE.AVAILABLE;
}
util.inherits(Partition, EventEmitter);

Partition.prototype.accquire = function(handId, mateId){
    this.mateId = mateId;
    this.handId = handId;
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
    return util.format("Partition(id=%d)", this.id);
};

// Captain dishes out partitions to mates
function Captain(partitionSize){
    this.partitionSize = partitionSize;
    this.partitionCount = 0;
    this.partitions = {};
    this.total = 0;

    this.availablePartitions = [];
    this.completedPartitions = [];
    this.extras = {};
}

Captain.prototype.set = function(k, v){
    this.extras[k] = v;
};

Captain.prototype.get = function(k){
    if(this.hasOwnProperty(k)){
        return this[k];
    }
    // @todo (lucas) Allow callable extras, ie random mongo host.
    return this.extras[k];
};

Captain.prototype.startShip = function(partitioner){
    partitioner.apply(this, [function(total, data){
        this.total = total;
        this.partitionCount = Math.ceil(this.total/ this.partitionSize);
        for(var i = 0; i < data.length; i++){
            this.partitions[data[i].id] = new Partition(this, data[i]);
        }
        log.silly('Determined Partitions: ', this.partitions);
        this.startServer();
    }.bind(this)]);
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
                    'partition': partition.getData()
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
        else if(msg.action === 'HEARTBEAT'){
            var client = dgram.createSocket("udp4"),
                message = new Buffer(JSON.stringify({'action': "HEARTBOOP"}));
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
    this.captainAlive = false;
    this.messageQueue = [];
    this.monitorInterval = null;
}
util.inherits(Hand, EventEmitter);

Mate.prototype.monitorCaptain = function(){
    var client = dgram.createSocket("udp4"),
        message = new Buffer(JSON.stringify({
            'action': 'HEARTBEAT',
            'mate': this.id
        }));
    client.send(message, 0, message.length, 9000, this.captainHost, function() {
        client.close();
    });
};

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
                clearInterval(this.monitorInterval);
                this.server.close();
            }
            return;
        }
        if(msg.action === "HEARTBOOP"){
            this.captainAlive = true;
            this.messageQueue.forEach(function(m){
                this.tellCaptain(m[0], m[1]);
            }.bind(this));

            this.messageQueue = [];

            return;
        }
        this.tellHand(msg.hand, msg);
    }.bind(this));

    this.monitorInterval = setInterval(this.monitorCaptain.bind(this), 100);

    this.server.bind(9001);
    return this;
};

Mate.prototype.tellCaptain = function(handId, msg){
    if(this.captainAlive === false){
        this.messageQueue[this.messageQueue.length] = [handId, msg];
    }
    else{
        var client = dgram.createSocket("udp4"),
            message = new Buffer(JSON.stringify(msg));
        log.silly("Telling captain for hand " + handId + ": " + message.toString());
        client.send(message, 0, message.length, 9000, this.captainHost, function() {
            client.close();
        });
    }
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