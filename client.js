var io = require('socket.io-client');
var async = require('async');
var readline = require('readline');


var env = process.env.NDS_ENV || 'development';
var config = require('./config/' + env);

var sockets = [];

var count = 0;

function hook_stdout() {
    var old_write = process.stdout.write;

    process.stdout.write = (function(write) {
        return function(string, encoding, fd) {
            //write.apply(process.stdout, arguments);
        }
    })(process.stdout.write);

    return function() {
        process.stdout.write = old_write;
    }
}

var unhook = hook_stdout();

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

    var getSocketToSaveWordTo = function(input) {
        var firstChar = input.toLowerCase().charCodeAt(0) - 97;
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
                //console.log(data);
            });
            count++;
        }
    });

});

var emitRemoveDupes = function() {

    async.forEach(sockets, function(socket, callback) {
        socket.emit('removeDuplicates', null, function() {
            callback();
        });
    }, function(err) {
        console.log('\n--RESULT--\n');
        unhook();
        retrieveWord(0);
    });
}

var retrieveWord = function(index) {
    async.forEach(sockets, function(socket, callback) {

        socket.emit('retrieveWord', index, function(word) {
            if (word) console.log(word);
            callback();
        });

    }, function(err) {

        if (index < count) {
            retrieveWord(index + 1);
        } else {
            //console.log('FINISHED');
            process.exit();
        }

    });
}