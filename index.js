"use strict";
var winston = require('winston');

winston.loggers.add('shipyard', {
    console: {
        'level': 'silly', 'timestamp': true, 'colorize': true
    }
});

module.exports.Captain = require('./lib/captain');
module.exports.Mate = require('./lib/mate');
module.exports.Hand = require('./lib/hand');
module.exports.Ship = require('./lib/ship');
module.exports.log = winston.loggers.get('shipyard');