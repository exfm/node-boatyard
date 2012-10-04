"use strict";

var eatInterval = null;
function eat(partitionId, start, stop){
    var completed = 0,
        total = stop - start;

    eatInterval = setInterval(function(){
        completed += 10000;
        if(completed === total){
            this.release();
            this.getWorkToDo();
            clearInterval(eatInterval);
        }
        else{
            this.progress(total, completed, 0, "Making sausages....");
        }
    }.bind(this), 100);
}

module.exports = eat;