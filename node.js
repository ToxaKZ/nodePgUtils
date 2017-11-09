const _ = require('lodash');
const async = require('async');
const Elasticsearch = require('elasticsearch');
const config = require('./config');

// Delay metrics push start moment
const initialDelay = Math.round(Math.random() * config.node.interval / 2);

const hostId = 'host' + process.pid;
const hostname = 'node-generator-test-' + process.pid;

const meta = {
    objectId: 'testpool_test' + process.pid,
    objectName: 'TestPool/Test' + process.pid
};

let elasticsearch = new Elasticsearch.Client({
    host: config.server,
    log: 'error'
});

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

let batch = generateBatch();
let size = JSON.stringify(batch).length;


// Run
console.log('pid:', process.pid, 'Delaying node data push for',
    initialDelay, 'ms. Batch size:', Math.trunc(size / 1024 / 1024), 'MB');
setTimeout(pushData, initialDelay);

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
            console.log('pid:', process.pid, 'error:', err.message);
        }

        // Reiterating
        setTimeout(pushData, config.node.interval);
    });
}
