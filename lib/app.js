"use strict";
var express = require('express'),
    app = express(),
    server = require('http').createServer(app),
    io = require('socket.io').listen(server),
    browserify = require('browserify'),
    routes = require('./routes'),
    leveldb = require('leveldb');

function Boat(id, data){
    var self = this;

    this.id = id;
    Object.keys(data).forEach(function(key){
        self[key] = data[key];
    });
}

Boat.get = function(id, cb){
    Boat.DB.get(id, function(err, data){
        cb(err, new Boat(id, data));
    });
};

Boat.getAll = function(cb){
    var boats = [];
    Boat.DB.forRange("boat-", function(id, data){
        boats.push(new Boat(id, data));
    }, function(){
        cb(boats);
    });
};

Boat.update = function(id, ops, cb){
    // ops:
    // {
    //     'add': {'mates': 'localhost'},
    //     'put': {'out': true},
    //     'remove': ['offline'],
    //     'trim': {'mates': 5}
    // }
    Boat.DB.get(id, function(err, d){
        d = d || {};

        if(ops.hasOwnProperty('add')){
            Object.keys(ops.add).forEach(function(k){
                if(d.hasOwnProperty(k)){
                    if(Array.isArray(ops.add[k])){
                        ops.add[k].forEach(function(v){
                            d[k].push(v);
                        });
                    }
                    else{
                        d[k] += ops.add[k];
                    }
                }
                else{
                    d[k] = ops.add[k];
                }
            });
        }
        if(ops.hasOwnProperty('put')){
            Object.keys(ops.put).forEach(function(k){
                d[k] = ops.put[k];
            });
        }

        if(ops.hasOwnProperty('remove')){
            Object.keys(ops.remove).forEach(function(k){
                delete d[k];
            });
        }

        if(ops.hasOwnProperty('trim')){
            Object.keys(ops.trim).forEach(function(k){
                d[k] = d[k].slice(0, ops.trim[k] - 1);
            });
        }

        Boat.DB.put(id, d, {}, function(err){
            cb(err, d);
        });
    });
};

// Configuration

app.configure(function(){
    app.set('views', __dirname + '/views');
    app.set('view engine', 'jade');
    app.use(express.bodyParser());
    app.use(express.methodOverride());
    app.use(app.router);
    app.use(express.static(__dirname + '/public'));
});

app.configure('development', function(){
    app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function(){
    app.use(express.errorHandler());
});

app.use(browserify({

}));

app.use(function(req, res, next) {
    var data='';
    req.setEncoding('utf8');
    req.on('data', function(chunk) {
       data += chunk;
    });

    req.on('end', function() {
        req.rawBody = data;
        next();
    });
});

app.get('/api/boat/:id', function(req, res, next){
    Boat.get(req.param('id'), function(boat){
        res.send(boat);
    });
});

app.get('/api/boat', function(req, res, next){
    Boat.getAll(function(boats){
        res.send(boats);
    });
});

app.post('/api/boat/:id', function(req, res, next){
    Boat.update(req.param('id'), JSON.parse(req.rawBody), function(err, data){
        res.send(data);
    });
});

app.post('/api/boat/:id/hand-added', function(req, res, next){
    var op = {
        'add': {
            'hands': [req.param('handId') + ' - ' + req.param('mateId')]
        }
    };
    Boat.update(req.param('id'), op, function(err, data){
        res.send(data);
    });
});

app.post('/api/boat/:id/hand-killed', function(req, res, next){
    var op = {
        'remove': {
            'hands': [req.param('handId') + ' - ' + req.param('mateId')]
        }
    };
    Boat.update(req.param('id'), op, function(err, data){
        res.send(data);
    });
});

app.post('/api/boat/:id/progress', function(req, res, next){
    var op = {
        'add': {
            'progress': {
                'mateId': req.param('mateId'),
                'handId': req.param('handId'),
                'partitionId': req.param('partitionId'),
                'total': req.param('total'),
                'completed': req.param('completed'),
                'errored': req.param('errored'),
                'message': req.param('message')
            }
        }
    };

    // @todo (lucas) Calculate over total / time remaining for this job
    // @todo (lucas) emit progress event on socket.

    Boat.update(req.param('id'), op, function(err, data){
        res.send(data);
    });
});

app.post('/api/boat/:id/error', function(req, res, next){
    var op = {
        'add': {
            'errors': {
                'mateId': req.param('mateId'),
                'handId': req.param('handId'),
                'partitionId': req.param('partitionId'),
                'message': req.param('message')
            }
        }
    };

    // @todo (lucas) emit error event on socket.

    Boat.update(req.param('id'), op, function(err, data){
        res.send(data);
    });
});

app.post('/api/boat/:id/accquire', function(req, res, next){
    res.send("OK");
});

app.post('/api/boat/:id/deployed', function(req, res, next){

    // Move current data to new deploy key
    new Boat(req.param('id')).newRun().then(function(data){
        res.send(data);
    });
});



module.exports = server;