var express = require('express');
var app = express();
var bodyParser = require('body-parser')
var queryNumber = 0;
//app.use( bodyParser.json() );       // to support JSON-encoded bodies
//app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
//  extended: true
//}));
app.use(bodyParser.text({type:"*/*", limit: '150mb'}));
var pg = require('pg');
 const { Client } = require('pg')
var copyFrom = require('pg-copy-streams').from;


// client.connect()

//client.query('SELECT $1::text as message', ['Hello world!'], (err, res) => {
//  console.log(err ? err.stack : res.rows[0].message) // Hello World!
//  client.end()
//})

app.use(function (req, res, next) {
       console.log('Time:', Date.now())
   console.log(req.method);
   console.log(req.url);
   console.log(req.query);
   if(req.url == '/_bulk'){
    queryNumber++;
    req.queryNumber = queryNumber;
    req.startTime = new Date();
   }
   next();
})


app.post("/_bulk", function(req,res){
    console.log('BULK number '+req.queryNumber+' started');
    var arr = [];
     var bd = req.body.toString();
       var bds = bd.split('\n');
      for(var i=0; i<bds.length; i=i+2){
            if(bds[i].length>0){
            var firstPart=JSON.parse(bds[i]);
            var secondPart=JSON.parse(bds[i+1]);
            }

            var objectToInsert =  { index: firstPart.index._index,
            metric: secondPart.metric,
            hostId: secondPart.hostId,
            objectId: secondPart.objectId,
            objectName: secondPart.objectName,
            timestamp: secondPart.timestamp,
            values:
             []
            };

            Object.keys(secondPart.values).forEach(m=>{
                objectToInsert.values.push({name: m, value: secondPart.values[m]});
            });
            arr.push(objectToInsert);
        }

        var stream = require("stream")
        var s = new stream.PassThrough()
        // a.write("your string")
        // a.end()
    
        // var s = new Readable
        var query = '';
        arr.forEach(arrelem=>{
            query = ''+new Date(arrelem.timestamp).toUTCString()+'\t'+arrelem.hostId+'\t'+arrelem.objectName;
            arrelem.values.forEach((m)=>{
                query = query+'\t'+m.value;
            });
            s.push(query+'\n');    
        });
       
        s.end();
        // s.push(null);
        
        const client = new Client();
        client.connect();

        // pg.connect(function(err, client, done) {
            var stream = client.query(copyFrom('COPY analytics_analytics_test_xxxyyy FROM STDIN'));
           // var fileStream = fs.createReadStream('some_file.tsv')
           // fileStream.on('error', done);
            stream.on('error', function(err){
                console.log(err);
            });
            stream.on('end', function(){
              ///  console.log('inserted');
                console.log('BULK number '+req.queryNumber+' finished in '+(new Date()-req.startTime));
                client.end();
                res.send({errors:false});
            });
            s.pipe(stream);
            
        // client.query('select bulkMetricInsert(\'' +JSON.stringify(arr)+ '\')', function(err, data   ){
        //     console.log(err);
        //     console.log('BULK number '+req.queryNumber+' finished in '+(new Date()-req.startTime));
        //     
        // });
        
});



app.get("/fus-version/deployed-scripts/1", function(req,res){
var dep={
    "_index": "fus-version",
    "_type": "deployed-scripts",
    "_id": "1",
    "_version": 56,
    "found": true,
    "_source": {
        "deployed_scripts": [
            "deployment/elastic/plainApi/indices-putTemplate/20141015151301_events.json",
            "deployment/elastic/plainApi/indices-putTemplate/20141015154901_logs.json",
            "deployment/elastic/plainApi/indices-putTemplate/20141015163901_audit-logs.json",
            "deployment/elastic/plainApi/indices-putTemplate/20150527172103_alert-rule_queries.json",
            "deployment/elastic/plainApi/index/20151014144601_alert_processing_status_logs.js",
            "deployment/elastic/plainApi/index/20151014144602_alert_processing_status_events.js",
            "deployment/elastic/plainApi/indices-putTemplate/20151023115702_analytics-avg-number-of-shards.json",
            "deployment/elastic/plainApi/indices-putTemplate/20151023115703_analytics-min-number-of-shards.json",
            "deployment/elastic/plainApi/indices-putTemplate/20151023115704_analytics-max-number-of-shards.json",
            "deployment/elastic/plainApi/indices-putTemplate/20151023115705_analytics-sum-number-of-shards.json",
            "deployment/elastic/plainApi/indices-putTemplate/20151106101101_fusionui.json",
            "deployment/elastic/plainApi/indices-putTemplate/20151127171001_fusionui.json",
            "deployment/elastic/reIndex/20151127171002_fusionui_reindex.js",
            "deployment/elastic/reIndex/20151229171003_fusionui_reindex.js",
            "deployment/elastic/plainApi/indices-putTemplate/20160531171009_fusionui.json",
            "deployment/elastic/reIndex/20161102215301_fusionui_reindex.js",
            "deployment/elastic/reIndex/20170221163202_fusionui_reindex.js",
            "deployment/elastic/plainApi/indices-deleteMapping/20170221173101_atomicNode.json",
            "deployment/elastic/plainApi/indices-putSettings/20170330142413_default_mapping_analytics.json",
            "deployment/elastic/plainApi/indices-putTemplate/20170330142413_default_mapping_analytics.json",
            "deployment/elastic/plainApi/indices-putTemplate/20170330142413_default_mapping_realtime_analytics.json",
            "deployment/elastic/plainApi/indices-putTemplate/20170330142433_default_mapping_errors.json",
            "deployment/elastic/plainApi/indices-putTemplate/20173003054300_fus-version.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0000_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0000_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0001_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0001_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0002_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0002_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0003_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0003_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0004_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0004_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0005_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0005_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0006_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0006_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0007_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0007_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0008_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0008_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0009_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0009_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0010_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0010_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0011_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0011_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0012_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0012_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0013_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0013_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0014_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0014_default_alert_rule_rule.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0015_default_alert_rule_percolator.json",
            "deployment/elastic/plainApi/defaultAlertRules/index/rule_0015_default_alert_rule_rule.json"
        ]
    }
};
res.send(dep);
});

app.listen(9200);