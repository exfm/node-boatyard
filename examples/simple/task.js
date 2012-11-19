"use strict";

var interval = null;
module.exports = function(partition){
    console.log('Simple task got arguments', arguments);
    var self = this,
        completed = 0,
        total = partition.stop - partition.start;

    function work(){
        completed += 10000;
        if(completed === total){
            self.release();
            self.getWorkToDo();
            clearInterval(interval);
        }
    }
    interval = setInterval(work, 100);

    work();
};