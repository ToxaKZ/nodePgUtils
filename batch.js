const childProcess = require('child_process');
const baseConfig = require('./config');
const _ = require('lodash');
const batches = _.reverse(require('./batches'));

function run() {
    if (!batches.length) {
        return;
    }
    let node = batches.pop();

    _.forEach(node, function(item, key) {
        baseConfig.node[key] = item;
    });

    let proc = childProcess.fork('./index.js', [JSON.stringify(baseConfig)]);

    proc.on('exit', function() {
        console.log('Cool down for 15s');
        setTimeout(run, 15000);
    });
}

run();
