"use strict";

var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    dgram  = require('dgram'),
    winston = require('winston'),
    Partition = require('./partition'),
    log = winston.loggers.get('shipyard');

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
    if(typeof this.extras[k] === 'function'){
        return this.extras[k].apply(this, []);
    }
    return this.extras[k];
};

Captain.prototype.startShip = function(partitioner){
    partitioner.apply(this, [function(total, data){
        this.total = total;
        this.partitionCount = Math.ceil(this.total/ this.partitionSize);
        for(var i = 0; i < data.length; i++){
            this.partitions[data[i].id] = new Partition(this, data[i]);
            this.availablePartitions.push(data[i].id);
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
            handId = msg.hand,
            message,
            client = dgram.createSocket("udp4");


        if(msg.action === "FEEDME"){
            // Pick a partition and pass it back to the worker
            var partition;

            if(this.availablePartitions.length){
                partitionId = this.availablePartitions.shift();
                partition = this.partitions[partitionId];

                log.info(util.format('Giving partition %s to hand %s on mate %s',
                    partition, handId, mateId));

                message = new Buffer(JSON.stringify({
                    'action': "EAT",
                    'mate': mateId,
                    'hand': handId,
                    'partition': partition
                }));
                console.log(message.toString());
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

module.exports = Captain;
