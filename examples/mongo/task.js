"use strict";
var mongo = require('mongodb'),
    DB = mongo.Db,
    Server = mongo.Server;

module.exports = function(partition){
    var self = this,
        db,
        completed = 0,
        total = partition.stop - partition.start,
        opts = {
            'slave_ok': true,
            'native_parser': true
        },
        q = {
            '_id': {
                '$gte': partition.start,
                '$lt': partition.stop
            }
        };
    console.log(q);

    db = new DB(partition.dbName, new Server(partition.mongoHost, 27017), opts);
    db.open(function(err, db){
        db.collection(partition.collectionName, function(err, collection){
            collection.find(q, function(err, cursor){

                if(err !== null){
                    return self.error(err);
                }
                self.progress(total, completed, 0, "calling cursor.toArray");
                cursor.toArray(function(err, items){
                    self.progress(total, completed, 0, "in cursor.toArray");
                    items.forEach(function(item, index){
                        completed++;
                        if(index % 100 === 0){
                            self.progress(total, completed, 0, "Song " + item._id);
                        }
                    });
                    self.release();
                    db.close();

                    self.getWorkToDo();
                });
            });
        });
    });
};