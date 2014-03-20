var port = parseInt(process.argv[2]);
var os = require('os');
var fs = require('fs.extra');
var extend = require('util')._extend;
var server = require('./lib/server')(os.hostname(), port);

var dbPath = './data/' + port;

//cleanup working directory
if (fs.existsSync(dbPath)) {
    fs.rmrfSync(dbPath);
}
fs.mkdirSync(dbPath);

var allowedMemorySize = os.totalmem() / 8;

var memoryUsageGuess = 0;

var words = {};

var result = {};

var filesWritten = 0;

//If memory usage exceeds a limit, write the chunk of words to disk
var checkMemoryUsage = function() {

    if (memoryUsageGuess > allowedMemorySize) {
        memoryUsageGuess = 0;
        var wordsBackup = extend({}, words);
        words = {};
        filesWritten++;

        fs.writeFile(dbPath + '/words' + filesWritten + '.json', JSON.stringify(wordsBackup), function(err) {
            if(err) {
                console.log('ERROR SAVING FILE', err);
            }
        });

    }
}

//command to store words
server.registerCommand('storeWords', function(chunkOfWords, callback) {

    words = extend(words, chunkOfWords);

    for (var i in chunkOfWords) {
        memoryUsageGuess += (i + chunkOfWords[i]).length;
    }

    checkMemoryUsage();

    callback('saved');

});

//command to remove duplicates
server.registerCommand('removeDuplicates', function(data, callback) {

    //function to remove all duplicates from a given list
    var mapReduceFn = function(obj) {
        var keys = Object.keys(obj);
        var build = {};
        keys.forEach(function(key) {
            var word = obj[key];
            var lineNumber = build[word];
            if (lineNumber == null) {
                build[word] = key;
            } else if(parseInt(lineNumber) > parseInt(key)) {
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

    //flips an objects keys and values
    var flipObject = function(obj) {
        var build = {};
        for (var i in obj) {
            build[obj[i]] = i;
        }
        return build;
    }

    //If we can process all data in memory then let's do so.
    if (filesWritten == 0) {

        result = mapReduceFn(words);
        words = null;
        callback();

    } else {

        filesWritten++;

        //flush the last bits to disk
        fs.writeFile(dbPath + '/words' + filesWritten + '.json', JSON.stringify(words), function(err) {
            words = null;
            if(err) {
                console.log('ERROR SAVING FILE', err);
            }

            var totalWords = 0;

            for (var i = 1; i <= filesWritten; i++) {
                var file1Name = dbPath + '/words' + i + '.json';
                var file1 = JSON.parse(fs.readFileSync(file1Name));

                //on the first loop remove all duplicates from each list
                if (i == 1) {
                    var file1DupesRemoved = mapReduceFn(file1);
                } else {
                    var file1DupesRemoved = extend({}, file1);
                }

                file1 = null;
                fs.writeFileSync(file1Name, JSON.stringify(file1DupesRemoved));
                var file1Words = flipObject(file1DupesRemoved);
                file1DupesRemoved = null;

                totalWords += Object.keys(file1Words).length;

                //loop through every block of works after the current word block removing duplicates
                for (var j = i + 1; j <= filesWritten; j++) {
                    var file2Name = dbPath + '/words' + j + '.json';
                    var file2 = JSON.parse(fs.readFileSync(file2Name));

                    //on the first loop remove all duplicates from each list
                    if (i == 1) {
                        file2 = mapReduceFn(file2);
                    }

                    var file2Words = flipObject(file2);
                    file2 = null;
                    for (var k in file1Words) {
                        delete file2Words[k];
                    }
                    var file2DupesRemoved = flipObject(file2Words);
                    file2Words = null;

                    fs.writeFileSync(file2Name, JSON.stringify(file2DupesRemoved));
                    file2DupesRemoved = null;
                }

                file1Words = null;

            }

            callback();

        });

    }

});

//Pre chunks an object up, otherwise node hits performance problems when looking up by key in a loop
//in order for this to work we assume the requested chunk size will always be the same
var chunksAdded = 0;
var chunkObject = function(obj, chunkSize) {

    var objKeys = Object.keys(obj).map(function(index) {
        return parseInt(index);
    });
    objKeys.sort(function(a,b){return a-b});

    var chunks = new Array();
    var buildChunk = {};
    objKeys.forEach(function(i) {
        var bottomThreshold = chunksAdded * chunkSize;
        var topThreshold = bottomThreshold + chunkSize;

        while (i >= topThreshold) {
            chunks[chunksAdded] = extend({}, buildChunk);
            buildChunk = {};
            chunksAdded++;

            bottomThreshold = chunksAdded * chunkSize;
            topThreshold = bottomThreshold + chunkSize;
        }

        buildChunk[i] = obj[i];
    });
    chunks[chunksAdded] = buildChunk;

    return chunks;
}

var chunkedResult = null;
var filesRead = 0;
var chunksRead = 0;

//command to retrieve words in a given index range
server.registerCommand('retrieveWords', function(data, callback) {

    var chunkSize = data.indexLessThanOrEqualTo - data.indexGreaterThanOrEqualTo;
    var chunkIndexToSend = data.indexGreaterThanOrEqualTo / chunkSize;

    //if we've processed it all in memory
    if (filesWritten == 0) {
        if (chunkedResult == null) {
            chunkedResult = chunkObject(result, chunkSize);
            result = {};
        }
    } else {

        if (filesRead == 0) {
            //console.log('READING', filesRead+1);
            chunkedResult = chunkObject(JSON.parse(fs.readFileSync(dbPath + '/words' + (filesRead+1) + '.json')), chunkSize);
            filesRead++;
        } else if (chunksRead == (chunksAdded) && filesRead < filesWritten) {
            //console.log('READING', filesRead+1);
            var lastChunk = chunkedResult[chunkIndexToSend];
            chunkedResult = chunkObject(JSON.parse(fs.readFileSync(dbPath + '/words' + (filesRead+1) + '.json')), chunkSize);
            if (!chunkedResult[chunkIndexToSend]) chunkedResult[chunkIndexToSend] = {};
            for (var i in lastChunk) {
                chunkedResult[chunkIndexToSend][i] = lastChunk[i];
            }
            filesRead++;
        }

    }

    callback(chunkedResult[chunkIndexToSend]);

    chunkedResult[chunkIndexToSend] = null;
    chunksRead++;

});

//cleanup global variables, quite hacky but I ran out of time
server.registerCommand('cleanup', function(data, callback) {
    memoryUsageGuess = 0;
    filesWritten = 0;
    words = {};
    chunkedResult = null;
    result = {};
    chunksAdded = 0;
    chunksRead = 0;
    filesRead = 0;
    if (fs.existsSync(dbPath)) {
        fs.rmrfSync(dbPath);
    }

    fs.mkdirSync(dbPath);
    if (callback) callback();
});