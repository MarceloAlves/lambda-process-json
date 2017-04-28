var async = require('async');
var AWS = require('aws-sdk');
var dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});
var s3 = new AWS.S3();

exports.handler = (event, context, callback) => {
  var tableName = "WaitTimes",
  s3BucketName = event.Records[0].s3.bucket.name,
  s3ObjectKey = event.Records[0].s3.object.key,
  datetime = event.Records[0].s3.object.key.replace(/disneyland\/|californiaadventure\//,"").replace(".json", ""),
  dstBucketName = "wait-times-processed";

  async.waterfall([
    function download(next){
      console.log("Getting File");
      s3.getObject({
        Bucket: s3BucketName,
        Key: s3ObjectKey
      },
      next);
    },
    function convertJson(response, next){
      console.log("Reading File");
      var jsonResult = JSON.parse(response.Body);
      next(null, jsonResult);
    },
    function storeDynamo(result, next){
      console.log("Saving to Dynamo");
      result.forEach(function(item){
        dynamodb.putItem({
          "TableName": tableName,
          "Item": {
            "id": {"N": `${item.id}`},
            "datetime": {"N": `${datetime}`},
            "name": {"S": item.name},
            "waitTime": {"N": item.waitTime.toString()},
            "active": {"N": `${item.active ? 1 : 0}`},
            "status": {"S": item.status},
            "fastPass": {"N": `${item.fastPass ? 1 : 0}`}
          }
        }, function(err, data){
          if (err) {
            console.log('error: putting item into dynamodb failed: '+err);
          } else {
            console.log('success');
          }
        });
      })
      next(null, result, next);
    },
    function moveFile(data, next){
      console.log("Moving Buckets");
      s3.putObject({
        Bucket: dstBucketName,
        Key: s3ObjectKey,
        Body: JSON.stringify(data),
        ContentType: "text/plain"
      },
      function(err, done){
        if (err) {
          console.log(err);
        } else {
          console.log("done");
        }
      })
    }
  ]);
};
