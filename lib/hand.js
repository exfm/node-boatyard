"use strict";

var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    cluster = require('cluster'),
    winston = require('winston'),
    log = winston.loggers.get('boatyard');

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
        this.partitionId = msg.id;
        task.apply(this, [msg]);
    }.bind(this));
    this.getWorkToDo();
};

Hand.prototype.getWorkToDo = function(){
    process.send({'action': 'FEEDME'});
};

Hand.prototype.release = function(){
    this.tellMate({
        'action': "RELEASE",
        'partitionId': this.partitionId
    });
};

module.exports = Hand;