var io = require('socket.io-client');
var async = require('async');
var readline = require('readline');


var env = process.env.NDS_ENV || 'development';
var config = require('./config/' + env);

var sockets = [];

async.forEach(config.hosts, function(host, callback) {

    var socket = io.connect(host);
    var called = false;

    socket.on('connect', function() {

        socket.on('disconnect', function() {
            console.log('HOST ' + host + ' DISCONNECTED');
        });

        if (!called) callback();
        called = true;
    });

    sockets.push(socket);

}, function(err) {

    console.log('All hosts connected');

    var rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    var count = 0;

    var getSocketToSaveWordTo = function(input) {
        var firstChar = input.charCodeAt(0) - 97;
        var socketIndex = firstChar % sockets.length;
        return sockets[socketIndex];
    }

    rl.on('line', function (word) {
        if (word.length == 0) { //end of input
            rl.close();
            emitRemoveDupes();
        } else {
            var socket = getSocketToSaveWordTo(word);
            socket.emit('storeWord', {word: word, index: count}, function (data) {
                console.log(data);
            });
            count++;
        }
    });

});

var emitRemoveDupes = function() {
    sockets.forEach(function(socket) {
        socket.emit('removeDuplicates', null, function(data) {
            console.log('Lowest id', data);
            process.exit();
        });
    });
}