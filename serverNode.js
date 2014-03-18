var port = parseInt(process.argv[2]);
var os = require('os');
var fs = require('fs.extra');
var extend = require('util')._extend;
var server = require('./lib/server')('127.0.0.1', port);

var dbPath = './data/' + port;

if (fs.existsSync(dbPath)) {
    fs.rmrfSync(dbPath);
}

fs.mkdirSync(dbPath);

var allowedMemorySize = os.totalmem() / 8;
//allowedMemorySize = 1000;

var memoryUsageGuess = 0;

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

server.registerCommand('storeWords', function(chunkOfWords, callback) {

    words = extend(words, chunkOfWords);

    for (var i in chunkOfWords) {
        memoryUsageGuess += (i + chunkOfWords[i]).length;
    }

    checkMemoryUsage();

    callback('saved');

});

var result = {};

server.registerCommand('removeDuplicates', function(data, callback) {

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
        callback();

    } else {
        //TODO
    }

});

server.registerCommand('retrieveWords', function(data, callback) {
    var response = {};

    for(var i = data.indexGreaterThanOrEqualTo; i <= data.indexLessThanOrEqualTo; i++) {
        if (result[i]) {
            response[i] = result[i];
            delete result[i];
        }
    }

    callback(response);
});