var net = require('net');
var util = require('./util');

module.exports = exports = function(host, port) {

    var instantiated = false;

    var commands = {};

    //look at the command sent and execute it if it exists
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

    //instantiate the socket
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
            if (commands['cleanup']) commands['cleanup']();
            console.log('CLOSED: ' + remoteAddress);
            //process.exit();
        });

    }).listen(port, host);

    console.log('Server listening on ' + host +':'+ port);

    //function to register a command
    return {
        registerCommand: function(cmd, fn) {
            commands[cmd] = fn;
        }
    };

}