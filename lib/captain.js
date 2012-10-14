"use strict";

var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    dgram  = require('dgram'),
    winston = require('winston'),
    Partition = require('./partition'),
    log = winston.loggers.add('boatyard', {
        console: {
            'level': 'silly', 'timestamp': true, 'colorize': true
        }
    });

function Stat(){
    this.start = -1;
    this.end = -1;
}

Stat.prototype.update = function(d){
    var self = this;
    Object.keys(d).forEach(function(name){
        self[name] = d[name];
    });
    return self;
};


// Captain dishes out partitions to mates
function Captain(partitionSize, boat){
    this.partitionSize = partitionSize;
    this.partitionCount = 0;
    this.partitions = {};
    this.total = 0;
    this.complete = false;

    this.inflightPartitions = {};
    this.availablePartitions = [];
    this.completedPartitions = [];
    this.extras = {};

    this.boat = boat;
    boat.captain = this;

    this.stats = {
        'partition': {},
        'overall': new Stat().update({
            'start': Date.now()
        })
    };

    this.on('update', function(){
        this.calculateStats();
        log.info('Update: ' + JSON.stringify(this.stats, null, 4));
    }.bind(this));

    this.on('complete', function(){
        this.calculateStats();
        log.info('Complete!  Returning to yard.');
        log.info(JSON.stringify(this.stats, null, 4));
        this.server.close();
    }.bind(this));

    this.on('ready', function(){
        log.info('Captain is ready!');
    });

    this.emit('ready');
}

util.inherits(Captain, EventEmitter);

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

Captain.prototype.startBoat = function(partitioner){
    partitioner.apply(this, [function(total, data){
        this.total = total;
        this.partitionCount = Math.ceil(this.total/ this.partitionSize);
        for(var i = 0; i < data.length; i++){
            this.partitions[data[i].id] = new Partition(this, data[i]);
            this.availablePartitions.push(data[i].id);
            this.stats.partition[data[i].id] = new Stat();
        }
        log.silly('Determined Partitions: ', this.partitions);
        this.startServer();
    }.bind(this)]);
};

Captain.prototype.calculateStats = function(){
    var avgTime = -1,
        totalTime = -1,
        completedCount = -1;
    Object.keys(this.stats.partition).forEach(function(id){
        if(this.stats.partition[id].state === "complete"){
            totalTime += this.stats.partition[id].totalTime;
            completedCount++;
        }
    }.bind(this));
    avgTime = totalTime / completedCount;

    this.stats.overall.update({
        'avgTime': avgTime,
        'totalTime': totalTime,
        'remaining': -1,
        'completed': this.completedPartitions,
        'inflight': this.inflightPartitions,
        'comepleted': this.completedPartitions
    });
    return this;
};

Captain.prototype.startServer = function(){
    this.server = dgram.createSocket('udp4', function (msg, rinfo) {
        log.info('Got message: ' + msg.toString(), rinfo);
        msg = JSON.parse(msg.toString());
        var self = this,
            workerAddress = rinfo.address,
            data = [],
            partitionId = -1,
            statusMessage = "",
            completed = -1,
            total = -1,
            errored = -1,
            mateId = msg.mate,
            handId = msg.hand,
            message,
            client = dgram.createSocket("udp4"),
            stat = new Stat();


        if(msg.action === "FEEDME"){
            // Pick a partition and pass it back to the worker
            var partition;

            if(this.availablePartitions.length){
                partitionId = this.availablePartitions.shift();
                partition = this.partitions[partitionId];

                stat = this.stats.partition[partitionId] = new Stat();
                stat.update({
                    'state': "inflight",
                    'start': Date.now()
                });

                log.info(util.format('Giving partition %s to hand %s on mate %s',
                    partition, handId, mateId));

                message = new Buffer(JSON.stringify({
                    'action': "EAT",
                    'mate': mateId,
                    'hand': handId,
                    'partition': partition
                }));
                this.inflightPartitions[partition.id] = partition;

                // this.boat.accquire(mateId, handId, partitionId);

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

                if(Object.keys(this.inflightPartitions).length === 0){
                    this.complete = true;
                    this.emit('complete');
                }
                // this.boat.handKilled(mateId, handId);
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

            // this.boat.progress(mateId, handId, msg.partitionId, msg.total,
            //     msg.completed, msg.errored, msg.statusMessage);

        }
        else if(msg.action === 'RELEASE'){
            stat = this.stats.partition[msg.partitionId];
            this.partitions[msg.partitionId].release();
            stat.update({
                'state': "complete",
                'end': Date.now()
            }).update({
                'totalTime': stat.end - stat.start
            });

            delete this.inflightPartitions[msg.partitionId];

            self.emit('update');

            // this.boat.release(mateId, handId, msg.partitionId);
        }
        else if(msg.action === 'ERROR'){
            this.partitions[msg.partitionId].error(msg.message);

            stat = this.stats.partition[msg.partitionId];
            stat.update({
                'state': "error",
                'end': Date.now()
            }).update({
                'totalTime': stat.end - stat.start
            });

            delete this.inflightPartitions[msg.partitionId];
            // this.boat.error(mateId, handId, msg.partitionId, msg.message);
        }
        else{
            log.error('Dont know how to handle message: ', msg);
        }
    }.bind(this));
    this.server.bind(9000);
    log.info('Captain server started on port 9000.');
};

module.exports = Captain;
