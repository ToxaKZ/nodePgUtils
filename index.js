const fs = require('fs');
const childProcess = require('child_process');
const _ = require('lodash');
const async = require('async');
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

let clients = [];
function prepareClients() {
    for (let i = 0; i < config.node.count; i++) {
        clients.push(childProcess.fork('./node.js', [JSON.stringify(config)]));
    }
}

let results = [];

let previousCnt = 0;
function measure() {
    async.parallel([
        function(next) {
            elasticsearch.indices.stats({
                index: config.index
            }, next);
        },
        function(next) {
            elasticsearch.nodes.stats({}, next);
        }
    ], function(err, res) {
        if (!err) {
            let cnt = res[0][0]._all.primaries.docs.count;

            // Pickup first property from object (it's a node id)
            let health = _.find(res[1][0].nodes);

            if (cnt !== previousCnt) {
                let item = {
                    timestamp: new Date().toString(),
                    throughput: cnt - previousCnt,
                    health: {
                        cpu: {
                            os: health.os.cpu.percent,
                            process: health.process.cpu.percent,
                            proxy: health.proxy.cpu.percent
                            // '1m': health.os.cpu.load_average['1m'],
                            // '5m': health.os.cpu.load_average['5m'],
                            // '15m': health.os.cpu.load_average['15m']
                        },
                        memory: health.os.mem.used_percent,
                        memorydb: health.process.mem.usage,
                        memoryproxy: health.proxy.mem.usage,
                        heapUsed: 0
                    }
                };
                console.log(`${item.timestamp}: ${item.throughput} inserts ` +
                    `(Total: ${cnt})`);
                results.push(item);

                previousCnt = cnt;
            } else {
                // console.log(`${new Date().toString()}: - (Total: ${cnt})`);
            }
        } else {
            // Skip 'index not found' error
            if (err.status !== 404) {
                console.log(`Monitor error: ${err.message}`);
            }
        }
    });
}

function avg(val, precision) {
    precision = precision || 2;
    let p = Math.pow(10, precision);
    return Math.round(val * p) / p;
}

function med(data, field, percent) {
    if (percent === undefined) {
        percent = 0.5;
    }
    if (percent > 1) {
        percent = percent / 100;
    }
    let sorted = _.sortBy(data, field);
    let index = Math.ceil(sorted.length * percent);
    return _.get(sorted[index], field) || 0;
}

function startTest() {
    console.log('Starting test.');
    console.log(`Batch: ${config.node.batchSize}, Nodes: ${config.node.count}`);

    prepareClients();

    // Measure state, and push to results array
    setInterval(measure, config.monitor.interval);

    // Trigger the end of tests, exit application
    setTimeout(function() {
        let sorted = _.sortBy(results, 'throughput');

        let median = med(results, 'throughput', 50);
        let average = avg(_.meanBy(sorted, 'throughput')) || 0;
        let expected = config.node.count * config.node.batchSize;
        let expectedMonitor = config.monitor.interval / config.node.interval;
        let expectedRate = Math.round(expectedMonitor * expected);

        // Health averages
        let cpuOs = avg(_.meanBy(results, 'health.cpu.os'));
        let cpuProcess = avg(_.meanBy(results, 'health.cpu.process'));
        let cpuProxy = avg(_.meanBy(results, 'health.cpu.proxy'));
        let memory = avg(_.meanBy(results, 'health.memory'));
        let heapUsed = avg(_.meanBy(results, 'health.heapUsed'));
        let memorydb = avg(_.meanBy(results, 'health.memorydb'));
        let memoryproxy = avg(_.meanBy(results, 'health.memoryproxy'));

        console.log('Finished.');
        console.log(`Expected total: ${expected}`);
        console.log(`Expected per ${config.monitor.interval} ms: ` +
            `${expectedRate}`);
        console.log(`Median: ${median}`);
        console.log(`Average: ${average}`);
        console.log('');

        // Saving the results
        let res = {
            id: config.id,
            date: new Date().toString(),
            expected: expected,
            clients: config.node.count,
            batch: config.node.batchSize,
            monitorInterval: config.monitor.interval,
            nodeInterval: config.node.interval,
            expectedRate: expectedRate,
            median: median,
            average: average,
            health: {
                cpuOs: cpuOs,
                cpuProcess: cpuProcess,
                cpuProxy: cpuProxy,
                memory: memory,
                memorydb: memorydb,
                memoryproxy: memoryproxy,
                heapUsed: heapUsed
            }
        };

        let exists = fs.existsSync(config.monitor.saveTo);

        let output = [];
        if (exists) {
            output = JSON.parse(fs.readFileSync(config.monitor.saveTo, 'utf8'));
        }

        output.push(res);

        fs.writeFileSync(config.monitor.saveTo,
            JSON.stringify(output, 4, 4), 'utf8');

        clients.forEach(function(client) {
            client.kill();
        });

        process.exit();
    }, config.monitor.stop + config.node.interval);
}

let elasticsearch = new Elasticsearch.Client({
    host: config.server,
    log: 'error',
    requestTimeout: 120000
});

console.log(`Removing index: ${config.index}`);
elasticsearch.indices.delete({
    index: config.index
}, function(err) {
    if (err && err.status !== 404) {
        console.log(`Unable to remove index ${config.index}: ${err.message}`);
    } else {
        startTest();
    }
});

function stopRunning() {
    console.log('Exiting...');
    clients.forEach(function(client) {
        client.kill();
    });
    process.exit();
}

process.on('SIGINT', stopRunning);
process.on('SIGTERM', stopRunning);
process.on('exit', stopRunning);
