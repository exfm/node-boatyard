"use strict";

var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    winston = require('winston'),
    express = require('express'),
    Stat = require('./stat'),
    app = express(),
    log = winston.loggers.add('boatyard', {
        console: {
            'level': 'silly', 'timestamp': true, 'colorize': true
        }
    });

var Partition = require('./partition');

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
    }.bind(this));

    this.on('complete', function(){
        this.calculateStats();
        this.stats.overall.update({
            'end': Date.now()
        });
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
    app.set('captain', this);
    this.server = app.listen(9000, function(){
        log.info('Captain server started on port 9000.');
    });
    return this;
};

// Get current state of the boat.
app.get('/', function(req, res){
    res.send(app.get('captain').stats);
});

app.get('/heartbeat', function(req, res){
    res.send({'ok': true});
});

// Acquire a partition
app.post('/acquire', function(req, res){
    var partition = null,
        captain = app.get('captain'),
        stat,
        partitionId,
        mate = req.param('mate'),
        hand = req.param('hand');

    if(captain.availablePartitions.length){
        partitionId = captain.availablePartitions.shift();
        partition = captain.partitions[partitionId];

        stat = captain.stats.partition[partitionId] = new Stat();
        stat.update({'state': "inflight", 'start': Date.now()});

        log.info(util.format('Giving partition %s to hand %s on mate %s',
            partition, hand, mate));

        captain.inflightPartitions[partition.id] = partition;
        return res.send({'partition': partition});
    }

    log.info('Available partitions empty.');
    log.info(util.format('Sending empty to kill hand %s on mate %s',
        hand, mate));

    if(Object.keys(captain.inflightPartitions).length === 0){
        captain.complete = true;
        captain.emit('complete');
    }
    res.send({'partition': partition});
});

app.post('/progress/:partitionId', function(req, res){
    var partitionId = req.param('partitionId'),
        captain = app.get('captain');

    captain.partitions[partitionId].progress(req.param('total'),
        req.param('completed'), req.param('errored'),
        req.param('message'));
});

app.post('/release/:partitionId', function(req, res){
    var partitionId = req.param('partitionId'),
        captain = app.get('captain'),
        stat = captain.stats.partition[partitionId];

    captain.partitions[partitionId].release();
    stat.update({
        'state': "complete",
        'end': Date.now()
    }).update({
        'totalTime': stat.end - stat.start
    });

    delete captain.inflightPartitions[partitionId];

    captain.emit('update');
    res.send(stat);
});

app.post('/error/:partitionId', function(req, res){
    var partitionId = req.param('partitionId'),
        captain = app.get('captain'),
        stat = captain.stats.partition[partitionId];

    captain.partitions[partitionId].error(req.param('message'));
    stat.update({
        'state': "error",
        'end': Date.now()
    }).update({
        'totalTime': stat.end - stat.start
    });

    delete captain.inflightPartitions[partitionId];
    res.send(stat);
});

module.exports = Captain;
module.exports.app = app;
