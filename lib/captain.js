"use strict";

var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    winston = require('winston'),
    aws = require('plata'),
    log = winston.loggers.add('captain', {
        'console': {
            'level': 'error',
            'timestamp': true
        },
        'file': {
            'filename': 'captain.log',
            'level': 'silly',
            'timestamp': true,
            'colorize': true
        }
    });

var Partition = require('./partition');

// Captain creates partitions and puts them in SQS.
function Captain(nconf){
    this.partitionSize = nconf.get('size');
    this.key = nconf.get('key');
    this.secret = nconf.get('secret');
    this.queueName = nconf.get('queue-name');

    aws.connect({'key': this.key, 'secret': this.secret});
    this.extras = {};
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
    var self = this;
    partitioner.apply(this, [function(total, data){
        var partitions = [];
        for(var i = 0; i < data.length; i++){
            partitions.push(new Partition(self, data[i]));
        }
        aws.onConnected(function(){
            var queue = aws.sqs.Queue(self.queueName);
            queue.batch(partitions)
                .put()
                .then(function(){
                    log.info('Added ' + partitions.length + ' partitions to the queue.');
                    process.exit(0);
                });
        });
    }]);
};

module.exports = Captain;
