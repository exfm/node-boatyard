"use strict";
var winston = require('winston');

winston.loggers.add('boatyard', {
    console: {
        'level': 'silly', 'timestamp': true, 'colorize': true
    }
});

module.exports.Captain = require('./lib/captain');
module.exports.Mate = require('./lib/mate');
module.exports.Hand = require('./lib/hand');
module.exports.Boat = require('./lib/boat');
module.exports.logger = winston.loggers.get('shipyard');