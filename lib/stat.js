"use strict";
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

module.exports = Stat;