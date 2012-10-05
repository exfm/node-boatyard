"use strict";

var nconf = require('nconf'),
    mongo = require('mongodb'),
    DB = mongo.Db,
    Server = mongo.Server;

module.exports = function(cb){
    var db,
        self = this,
        hosts = nconf.get('mongoHosts'),
        opts = {
            'slave_ok': true,
            'native_parser': true
        }, partitions = [];

    this.set('dbName', nconf.get('dbName'));
    this.set('collectionName', nconf.get('collectionName'));
    this.set('mongoHost', function(){
        return hosts[Math.floor(Math.random() * hosts.length)];
    });

    db = new DB(nconf.get('dbName'), new Server(hosts[0], 27017), opts);
    db.open(function(err, db){
        db.collection(nconf.get('collectionName'), function(err, collection){
            collection.count(function(err, count){
                var partitionCount = Math.ceil(count / self.partitionSize);
                for(var i = 0; i< partitionCount; i++){
                    partitions.push({
                        'id': i,
                        'start': i*self.partitionSize,
                        'stop': (i*self.partitionSize) + self.partitionSize,
                        'mongoHost': self.get('mongoHost'),
                        'dbName': self.get('dbName'),
                        'collectionName': self.get('collectionName')
                    });
                }
                cb(count, partitions);
            });
        });
    });
};