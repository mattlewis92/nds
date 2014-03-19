const BLOCK_START = '---BLOCKSTART';

const BLOCK_END = 'BLOCKEND---';

var writeToSocket = function(sock, data) {
    var toWrite = '';
    if (data) {
        toWrite = JSON.stringify(data);
    }
    sock.write(BLOCK_START + toWrite + BLOCK_END);
}

module.exports = exports = {

    handleBlock: function(sock, handleData) {

        var buildFullPacket = '';

        sock.on('data', function(data) {

            data = data.toString();

            //console.log(data);

            buildFullPacket += data;

            if (buildFullPacket.lastIndexOf(BLOCK_END) == buildFullPacket.length - BLOCK_END.length) {

                var parts = buildFullPacket.split(BLOCK_START);
                if (parts.length > 2) {

                    for (var i = 1; i < parts.length; i++) {
                        handleData(parts[i].replace(BLOCK_END, ''), function(result, callbackId) {
                            writeToSocket(sock, {data: result, callback_id: callbackId});
                        });
                    }

                } else {
                    var cleanData = buildFullPacket.replace(BLOCK_END, '').replace(BLOCK_START, '');

                    handleData(cleanData, function(result, callbackId) {
                        writeToSocket(sock, {data: result, callback_id: callbackId});
                    });

                    cleanData = null;
                }

                buildFullPacket = '';
                parts = null;
            }

        });
    },

    writeToSocket: writeToSocket

}