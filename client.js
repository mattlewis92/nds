var io = require('socket.io-client');
var async = require('async');
var readline = require('readline');
var fs = require('fs');

var hosts = fs.readFileSync('hosts.cfg').toString().split('\n');

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

//var unhook = hook_stdout();
var unhook = function(){};

async.forEach(hosts, function(host, callback) {

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

    var stillToSave = 0;
	var isComplete = false;

    rl.on('line', function (word) {

        if (word.length == 0) { //end of input
            rl.close();
			isComplete = true;
			if (stillToSave == 0) emitRemoveDupes();
        } else {
            var socket = getSocketToSaveWordTo(word);
			stillToSave++;
            socket.emit('storeWord', {word: word, index: count}, function (data) {
				stillToSave--;
                if (isComplete && stillToSave == 0) emitRemoveDupes();
            });
            count++;
        }
    });

});


var hasStartedRemovingDupes = false;
var emitRemoveDupes = function() {
	
	if (hasStartedRemovingDupes) return;
	hasStartedRemovingDupes = true;

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