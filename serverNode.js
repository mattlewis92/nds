var port = parseInt(process.argv[2]);
var io = require('socket.io').listen(port, { log: false });
var os = require('os');
var fs = require('fs.extra');
var extend = require('util')._extend;

var dbPath = './data/' + port;

if (fs.existsSync(dbPath)) {
    fs.rmrfSync(dbPath);
}

fs.mkdirSync(dbPath);

console.log('Server listening on', port);

var allowedMemorySize = os.totalmem() / 8;
//allowedMemorySize = 1000;

var memoryUsageGuess = 0;

io.sockets.on('connection', function (socket) {

    console.log('Connection received');

    var words = {};

    var filesWritten = 0;

    var checkMemoryUsage = function() {
        if (memoryUsageGuess > allowedMemorySize) {
            memoryUsageGuess = 0;
            var wordsBackup = extend({}, words);
            words = {};
            filesWritten++;

            fs.writeFile(dbPath + '/words' + filesWritten, 'module.exports=' + JSON.stringify(wordsBackup) + ';', function(err) {
                if(err) {
                    console.log(err);
                } else {
                    console.log("The file was saved!");
                }
            });

        }
    }

    socket.on('storeWords', function (chunkOfWords, fn) {

        //console.log('STORING', Object.keys(chunkOfWords).length);

        words = extend(words, chunkOfWords);

        for (var i in chunkOfWords) {
            memoryUsageGuess += (i + chunkOfWords[i]).length;
        }

        checkMemoryUsage();

        if (fn) fn('saved');

    });

    var result = {};

    socket.on('removeDuplicates', function(data, fn) {

        var mapReduceFn = function(obj) {
            var keys = Object.keys(obj);
            var build = {};
            keys.forEach(function(key) {
                var word = obj[key];
                if (!build[word]) {
                    build[word] = key;
                } else if(build[word] > key) {
                    build[word] = key;
                }
            });

            var keys = Object.keys(build);
            var result = {};
            keys.forEach(function(key) {
                result[build[key]] = key;
            });

            return result;
        }

        if (filesWritten == 0) {

            result = mapReduceFn(words);
            words = null;
            fn();

        } else {

        }

    });

    socket.on('retrieveWords', function(data, fn) {

        var response = {};

        for(var i = data.indexGreaterThanOrEqualTo; i <= data.indexLessThanOrEqualTo; i++) {
            if (result[i]) {
                response[i] = result[i];
                delete result[i];
            }
        }

        fn(response);

    });

});