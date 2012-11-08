"use strict";

var util = require('util'),
    EventEmitter = require('events').EvenetEmitter,
    cluster = require('cluster'),
    winston = require('winston'),
    request = require('superagent'),
    log = winston.loggers.get('boatyard');

function Mate(id, captainHost, numHands){
    this.id = id;
    this.captainHost = captainHost;
    this.numHands = numHands;
    this.captainAlive = false;
    this.captainAlive = true;
    this.messageQueue = [];
    this.monitorInterval = null;
}

Mate.prototype.checkCaptain = function(){
    var self = this;
    request.get('http://' + this.captainHost + ':9000/heartbeat').end(function(res){
        self.captainAlive = (res.statusCode !== 200) ? false : true;
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

    this.monitorInterval = setInterval(this.checkCaptain.bind(this), 100);
    return this;
};

Mate.prototype.tellCaptain = function(path, msg){
    msg = msg || {};
    var self = this;
    if(this.captainAlive === false){
        this.messageQueue.push([path, msg]);
    }
    else{
        request.post('http://' + this.captainHost + ':9000' + path)
            .send(msg)
            .end(function(res){
                if(path === '/acquire'){
                    if(res.body.partition === null){
                        if(cluster.workers.hasOwnProperty(res.body.hand)){
                            self.killHand(res.body.hand);
                        }
                        if(Object.keys(cluster.workers).length === 0){
                            log.info('All hands relieved.  Jumping ship.');
                            clearInterval(self.monitorInterval);
                        }
                    }
                    else{
                        self.tellHand(res.body.hand, res.body);
                    }
                }

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
    this.tellCaptain('/acquire', {'hand': handId, 'mate': this.id});
};

Mate.prototype.progress = function(handId, partitionId, total, completed, errored, message){
    this.tellCaptain('/progress/' + partitionId, {
        'total': total,
        'completed': completed,
        'errored': errored,
        'message': message
    });
};

Mate.prototype.release = function(handId, partitionId){
    this.tellCaptain("/release/" + partitionId);
};

Mate.prototype.killHand = function(handId){
    cluster.workers[handId].destroy();
    log.info('Killed hand ' + handId);
};

module.exports = Mate;