"use strict";

var util = require('util'),
    EventEmitter = require('events').EvenetEmitter,
    dgram = require('dgram'),
    cluster = require('cluster'),
    winston = require('winston'),
    log = winston.loggers.get('shipyard');

function Mate(id, captainHost, numHands){
    this.id = id;
    this.captainHost = captainHost;
    this.numHands = numHands;
    this.captainAlive = false;
    this.messageQueue = [];
    this.monitorInterval = null;
}

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

module.exports = Mate;