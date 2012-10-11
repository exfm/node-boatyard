"use strict";

var request = require('request');

function Boat(name, yard){
    this.name = name;
    this.yard = yard;
    this.callYard = false;
}

// Phone home to the yard and let them know our partitioner function source.
Boat.prototype.setPartitioner = function(partitionerSource){
    if(this.callYard){
        request.post({
            'url': this.yard+"/api/boat/" + this.id,
            'form': {
                'put': {
                    'partitioner': partitionerSource
                }
            }
        });
    }
};

// Phone home to the yard and let them know our task function source.
Boat.prototype.setTask = function(taskSource){
    if(this.callYard){
        request.post({
            'url': this.yard+"/api/boat/" + this.id,
            'form': {
                'put': {
                    'task': taskSource
                }
            }
        });
    }
};

Boat.prototype.progress = function(mateId, handId, partitionId, total, completed, errored, msg){
    if(this.callYard){
        request.post({
            'url': this.yard+"/api/boat/" + this.id + "/error",
            'form': {
                'mateId': mateId,
                'handId': handId,
                'partitionId': partitionId,
                'message': msg,
                'total': total,
                'errored': errored,
                'completed': completed
            }
        });
    }
};

Boat.prototype.release = function(mateId, handId, partitionId){
    if(this.callYard){
        request.post({
            'url': this.yard+"/api/boat/" + this.id + "/release",
            'form': {
                'mateId': mateId,
                'handId': handId,
                'partitionId': partitionId
            }
        });
    }
};

Boat.prototype.error = function(mateId, handId, partitionId, msg){
    if(this.callYard){
        request.post({
            'url': this.yard+"/api/boat/" + this.id + "/error",
            'form': {
                'mateId': mateId,
                'handId': handId,
                'partitionId': partitionId,
                'message': msg
            }
        });
    }
};

Boat.prototype.accquire = function(mateId, handId, partitionId){
    if(this.callYard){
        request.post({
            'url': this.yard+"/api/boat/" + this.id + "/accquire",
            'form': {
                'mateId': mateId,
                'handId': handId,
                'partitionId': partitionId
            }
        });
    }
};

Boat.prototype.handKilled = function(mateId, handId){
    if(this.callYard){
        request.post({
            'url': this.yard+"/api/boat/" + this.id + "/hand-killed",
            'form': {
                'mateId': mateId,
                'handId': handId
            }
        });
    }
};

Boat.prototype.handAdded = function(mateId, handId){
    if(this.callYard){
        request.post({
            'url': this.yard+"/api/boat/" + this.id + "/hand-added",
            'form': {
                'mateId': mateId,
                'handId': handId
            }
        });
    }
};

module.exports = Boat;