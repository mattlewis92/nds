var net = require('net');
var util = require('./util');

module.exports = exports = function(host, port, connectedCallback) {

    var callbacks = {};

    var callbackCount = 0;

    var client = new net.Socket();
    client.connect(port, host, function() {

        console.log('CONNECTED TO: ' + host + ':' + port);

        util.handleBlock(client, function(data) {
            var parsed = JSON.parse(data);
            var callbackId = parsed.callback_id;
            var callback = callbacks[callbackId];
            callback(parsed.data);
            delete callbacks[callbackId];
        });

        connectedCallback();

    });

    client.on('close', function() {
        console.log('Connection closed');
    });

    return {
        closeConnection: function() {
            client.destroy();
        },

        executeCommand: function(cmd, data, callback) {
            if (!callback) callback = function() {};
            var dataToSend = {command: cmd, data: data, callback_id: callbackCount};
            callbacks[callbackCount] = callback;
            util.writeToSocket(client, dataToSend);
            callbackCount++;
        }
    }

}