"use strict";

module.exports = function(cb){
    var self = this,
        count = 1000000,
        partitions = [],
        partitionCount = Math.ceil(count / self.partitionSize);

    for(var i = 0; i< partitionCount; i++){
        partitions.push({
            'id': i,
            'start': i*self.partitionSize,
            'stop': (i*self.partitionSize) + self.partitionSize
        });
    }
    cb(count, partitions);
};