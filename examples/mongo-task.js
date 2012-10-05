"use strict";

var interval = null;
module.exports = function(partition){
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
        else{
            self.progress(total, completed, 0, "Making sausages....");
        }
    }
    interval = setInterval(work, 100);

    work();
};