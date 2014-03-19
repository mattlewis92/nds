var net = require('net');
var util = require('./util');

module.exports = exports = function(host, port) {

    var instantiated = false;

    var commands = {};

    const BLOCK_START = '---BLOCKSTART';

    const BLOCK_END = 'BLOCKEND---';

    var handleData = function(data, callback) {
        data = JSON.parse(data);
        var command = data.command;

        if (!commands[command]) {
            throw command + ' is not registered as a command!';
        } else {
            console.log('Executing command: ' + command);
        }

        commands[command](data.data, function(result) {
            if (data.callback_id != null) callback(result, data.callback_id);
        });
    }

    var commands = {
        registerCommand: function(cmd, fn) {
            commands[cmd] = fn;
        },

        writeToStream: function(data) {

        }
    };

    net.createServer(function(sock) {

        if (instantiated) {
            console.log('SERVER CAN ONLY HANDLE ONE CONNECTION');
            sock.destroy();
            return;
        }

        instantiated = true;

        var remoteAddress = sock.remoteAddress +':'+ sock.remotePort;

        console.log('CONNECTED: ' + remoteAddress);

        util.handleBlock(sock, handleData);

        sock.on('close', function(data) {
            instantiated = false;
            console.log('CLOSED: ' + remoteAddress);
            //process.exit();
        });

        commands.writeToStream = function(data, callback) {

            util.writeToSocket(sock, data);
            if (callback) callback();
            return;

            if (callback) {

                var bufferCheck = function() {
                    if (sock.bufferSize == 0) {
                        setImmediate(callback);
                    } else {
                        setImmediate(bufferCheck);
                    }
                }

                bufferCheck();

            }
        }

    }).listen(port, host);

    console.log('Server listening on ' + host +':'+ port);

    return commands;

}