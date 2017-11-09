const childProcess = require('child_process');
const _ = require('lodash');
const Elasticsearch = require('elasticsearch');
const config = require('./config');

var pg = require('pg');
const { Client } = require('pg')
const client = new Client();
client.connect()

let clients = [];
function prepareClients() {
    for (let i = 0; i < config.node.count; i++) {
        clients.push(childProcess.fork('./node.js'));
    }
}

let results = [];

let previousCnt = 0;
function measure() {
    client.query('select count(*) as count from analytics_analytics_test_xxxyyy ', function(err, res) {
        if (!err) {
            let cnt = res.rows[0].count;

            if (cnt !== previousCnt) {
                let item = {
                    timestamp: new Date().toString(),
                    throughput: cnt - previousCnt
                };
                console.log(item.timestamp, ':', item.throughput);
                results.push(item);

                previousCnt = cnt;
            } else {
                console.log(new Date().toString(), ':', '-');
            }
        }
    });
}

function startTest() {
    prepareClients();

    // Measure state, and push to results array
    setInterval(measure, config.monitor.interval);

    // Trigger the end of tests, exit application
    setTimeout(function() {
        let sorted = _.sortBy(results, 'throughput');

        
        
        // console.log(JSON.stringify(results, 4, 4));
        console.log('Finished.');
        let index = Math.trunc(sorted.length / 2);
        console.log('Median:', sorted[index].throughput);
        console.log('Average:', _.meanBy(sorted, 'throughput'));

        const fs = require('fs');
        const content = JSON.stringify(results);
        
        fs.writeFile("result.json", content, 'utf8', function (err) {
            if (err) {
                return console.log(err);
            }
        
            console.log("The file was saved!");


                clients.forEach(function(client) {
                    client.kill();
                });

                process.exit();
        }); 


    }, config.monitor.stop);
}

let elasticsearch = new Elasticsearch.Client({
    host: config.server,
    log: 'error'
});

elasticsearch.indices.delete({
    index: config.index
}, function() {
    startTest();
});

process.on('SIGINT', function() {
    clients.forEach(function(client) {
        client.kill();
    });
    process.exit();
});
