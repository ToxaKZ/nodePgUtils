const Elasticsearch = require('elasticsearch');
let config = require('./config');

// override config if specified
if (process.argv[2]) {
    try {
        config = JSON.parse(process.argv[2]);
    } catch (e) {
        console.log(`Unable to parse configuration file: ${e.message}`);
        process.exit();
    }
}

// Delay metrics push start moment
let initialDelay = 0;
if (config.node.count > 1) {
    initialDelay = Math.round(Math.random() * 1000);
}

const hostId = 'host' + process.pid;
const hostname = 'node-generator-test-' + process.pid;

const meta = {
    objectId: 'testpool_test' + process.pid,
    objectName: 'TestPool/Test' + process.pid
};

let elasticsearch = new Elasticsearch.Client({
    host: config.server,
    log: 'error',
    requestTimeout: 120000
});

let batch = generateBatch();

// Convert size to MB
let size = JSON.stringify(batch).length * 100 / 1024 / 1024;
size = Math.round(size) / 100;

// Start!
console.log(`Pid: ${process.pid}. Delaying data push for ${initialDelay} ms. ` +
    `Batch size: ${size} MB`);
setTimeout(pushData, initialDelay);

function getMetric() {
    let dReadCount = Math.random() * 10000;
    let dWriteCount = Math.random() * 10000;
    let lBytesRead = Math.random() * 10000000;
    let lBytesWritten = Math.random() * 10000000;
    let dReadLatency = Math.random();
    let dWriteLatency = Math.random();

    return {
        metric: 'analyticProbes:kstat:smbShare',
        hostId: hostId,
        hostname: hostname,
        objectId: meta.objectId,
        objectName: meta.objectName,
        timestamp: new Date(),
        values: {
            d_readCount: dReadCount,
            d_writeCount: dWriteCount,
            l_bytesRead: lBytesRead,
            l_bytesWritten: lBytesWritten,
            d_readLatency: dReadLatency,
            d_writeLatency: dWriteLatency,
            d_totalOps: dReadCount + dWriteCount,
            l_totalBytes: lBytesRead + lBytesWritten,
            d_averageLatency: dReadLatency + dWriteLatency
        }
    };
}

function generateBatch() {
    let batch = [];
    for (let i = 0; i < config.node.batchSize; i++) {
        batch.push({
            index: {
                _index: config.index,
                _type: 'analytics'
            }
        });
        batch.push(getMetric());
    }

    return batch;
}

function pushData() {
    elasticsearch.bulk({
        body: batch
    }, function(err) {
        if (err) {
            console.log(`Pid: ${process.pid}. Error: ${err.message}`);
        }

        // Reiterating
        setTimeout(pushData, config.node.interval);
    });
}
