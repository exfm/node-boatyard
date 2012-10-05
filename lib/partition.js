"use strict";

var util = require('util'),
    EventEmitter = require('events').EventEmitter;

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

module.exports = Partition;