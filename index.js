"use strict";
var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    mongo = require('mongodb'),
    sequence = require('sequence'),
    when = require('when'),
    dgram  = require('dgram');

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

    this.mateId = null;
    this.workerId = null;
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
}

Captain.prototype.getRandomMongoHost = function(){
    return this.mongoHosts[Math.floor(Math.random()*this.mongoHosts.length)];
};

Captain.prototype.startShip = function(){
    // How many things do we need to chew through?
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
        console.log('Partitions: ', this.partitions);
        this.startServer();
    });
};

Captain.prototype.startServer = function(){
    this.server = dgram.createSocket('udp4', function (msg, rinfo) {
        msg = msg.toString();

        console.log('Got message', msg, rinfo);
        var workerAddress = rinfo.address,
            data = [],
            partitionId = -1,
            statusMessage = "",
            completed = -1,
            total = -1,
            errored = -1;


        if(msg.indexOf("FEEDME") === 0){
            // Pick a partition and pass it back to the worker
            var client = dgram.createSocket("udp4"),
                partition,
                message;

            data = /FEEDME\|([\w\.]+)\|(\d+)/.exec(msg);
            var mateId = data[1],
                workerId = data[2];

            if(this.availablePartitions.length){
                partitionId = this.availablePartitions.shift();
                partition = this.partitions[partitionId];
                message = new Buffer("EAT|"+mateId+"-"+workerId+"|" + partitionId+"|" + partition.start + "-" + partition.stop);
            }
            else{
                message = new Buffer("EMPTY|"+mateId + "-" + workerId);
                console.log("EMPTTYTYYYYYYYYYYYYYYYYYYY");
            }
            client.send(message, 0, message.length, 9001, rinfo.address, function(err, bytes) {
                client.close();
            });
        }
        else if(msg.indexOf('PROGRESS|') === 0){
            data = /PROGRESS\|(\d+)\|(\d+)-(\d+)-(\d+)-(.*)/.exec(msg);
            partitionId = Number(data[1]);
            total = Number(data[2]);
            completed = Number(data[3]);
            errored = Number(data[4]);
            statusMessage = data[5];
            this.partitions[partitionId].progress(total, completed,
                errored, statusMessage);
            // Calculate total job progress

        }
        else if(msg.indexOf('RELEASE|') === 0){
            data = /RELEASE\|(\d+)/.exec(msg);
            partitionId = Number(data[1]);
            this.partitions[partitionId].release();
        }
        else if(msg.indexOf('ERROR|') === 0){
            data = /ERROR\|(\d+)\|(.*)/.exec(msg);
            partitionId = Number(data[1]);
            statusMessage = data[2];
            this.partitions[partitionId].error(statusMessage);
        }
        else{
            console.error('Dont know how to handle message: ' + msg);
        }
    }.bind(this));
    this.server.bind(9000);
    console.log('Captain server started on port 9000.');
};

module.exports.Captain = Captain;



function Hand(mateId){
    this.mateId = mateId;
}
util.inherits(Hand, EventEmitter);


