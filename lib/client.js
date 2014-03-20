var net = require('net');
var util = require('./util');

module.exports = exports = function(host, port, connectedCallback) {

    var callbacks = {};

    var callbackCount = 0;

    var client = new net.Socket();

    //simply function to connect to a server node
    client.connect(port, host, function() {

        //console.log('CONNECTED TO: ' + host + ':' + port);

        util.handleBlock(client, function(data) {
            var parsed = JSON.parse(data);

            //track and handle callbacks
            var callbackId = parsed.callback_id;
            var callback = callbacks[callbackId];
            callback(parsed.data);
            delete callbacks[callbackId];

        });

        connectedCallback();

    });

    //handle the close event
    client.on('close', function() {
        console.log('Connection closed');
    });

    return {
        //calling this function will close down the connection to the node
        closeConnection: function() {
            client.destroy();
        },

        //execute a remote command on the server node
        executeCommand: function(cmd, data, callback) {
            var dataToSend = {command: cmd, data: data};
            if (callback) {
                dataToSend.callback_id = callbackCount;
                callbacks[callbackCount] = callback;
                callbackCount++;
            }

            util.writeToSocket(client, dataToSend);

        }
    }

}