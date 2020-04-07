const S3 = require("aws-sdk/clients/s3");
const fs = require("fs");
const qs = require("qs");
const axios = require("axios");
const Queue = require("smart-request-balancer");
const dotenv = require("dotenv");
dotenv.config();

const client = new S3();
const bucket = process.env.JOBS_BACKUP_BUCKET || "";
const webhookURL = process.env.WEBHOOK_URL || "";
const slackChannel = process.env.SLACK_CHANNEL || "";

// queue config, slack incoming webhook url has rate 1/sec
const queue = new Queue({
  rules: {
    slack: {
      rate: 1, // one message
      limit: 1, // per second // https://api.slack.com/docs/rate-limits
      priority: 1
    }
  }
});

function findMaxOccuring(array) {
  let maxMap = {};
  let maxCount = 0;
  let maxCountElement;
  array.forEach(el => {
    if (!maxMap[el]) {
      maxMap[el] = 1;
    } else {
      maxMap[el]++;
    }
    if (maxMap[el] > maxCount) {
      maxCountElement = el;
      maxCount = maxMap[el];
    }
  });
  return maxCountElement;
}

// renameFile copy the file to new file with processed flag
// delete the old file
async function renameFile(key) {
  try {
    let newKeyParts = key.split(".");
    newKeyParts.splice(newKeyParts.length - 1, 0, "processed"); //add the processed flag just before file extension
    var copyParams = {
      Bucket: bucket,
      CopySource: bucket + "/" + key,
      Key: newKeyParts.join(".")
    };
    var deleteParams = {
      Bucket: bucket,
      Key: key
    };
    await client.copyObject(copyParams).promise();
    await client.deleteObject(deleteParams).promise();
  } catch (error) {
    // not throwing error, as even if it fails, will process again
    // which may not be an issue as getting notified again is better :)
    console.log("renameFile: ", error);
  }
}

// postResultToSlack make slack webhook request after formatting data
// data-format ex:
/* processedFile: test.json.aborted.gz , destination: S3, stats: {
    "failureCount": 458,
    "abortedCount": 229,
    "topFailureReasons": " MissingRegion: could not find region configuration"
  } , 
  processedFile: test.json.aborted.gz , destination: AF, stats: {
    "failureCount": 3,
    "abortedCount": 1,
    "topFailureReasons": " Missing Authentication"
  }, 
*/

async function postResultToSlack(data, fileKey) {
  try {
    let postData = [],
      slackRequestData,
      stats,
      axiosConfig;
    let destination = new Map();
    let errorResponse;

    data.forEach(failureInputData => {
      // console.log(JSON.stringify(failureInputData));
      if (!destination.has(failureInputData.custom_val)) {
        destination.set(failureInputData.custom_val, {
          failureCount: 0,
          abortedCount: 0,
          topFailureReasons: []
        });
      }
      var { failureCount, abortedCount, topFailureReasons } = destination.get(
        failureInputData.custom_val
      );
      if (failureInputData.job_state == "failed") {
        failureCount = failureCount + 1;
      }
      if (failureInputData.job_state == "aborted") {
        abortedCount = abortedCount + 1;
      }
      errorResponse =
        failureInputData.error_code +
        " " +
        failureInputData.error_response.Error;

      topFailureReasons.push(errorResponse);

      destination.set(failureInputData.custom_val, {
        failureCount: failureCount,
        abortedCount: abortedCount,
        topFailureReasons: topFailureReasons
      });
    });

    for (let [dest, destStats] of destination) {
      stats = { ...destStats };
      stats.topFailureReasons = findMaxOccuring(destStats.topFailureReasons);
      postData.push(
        "processedFile: " +
          fileKey +
          " , destination: " +
          dest +
          " , " +
          "stats: " +
          JSON.stringify(stats, null, "\t")
      );
    }

    slackRequestData = { channel: slackChannel, text: postData.join(",\n") };

    axiosConfig = {
      method: "post",
      url: webhookURL,
      data: qs.stringify({
        payload: JSON.stringify(slackRequestData)
      }),
      headers: {
        "content-type": "application/x-www-form-urlencoded;charset=utf-8"
      }
    };

    await queue.request(
      retry =>
        axios(axiosConfig)
          .then(response => response.data)
          .catch(error => {
            // Too many requests
            if (error.response.status === 429) {
              return retry(error.response.data.parameters.retry_after); // https://api.slack.com/docs/rate-limits
            }
            throw error;
          }),
      "test_id",
      "slack"
    );
  } catch (error) {
    console.log("postResultToSlack: ", error);
  }
}

// processFile get the required keys from the file, make a json payload
// then call postToSlack
// then rename file in aws
async function processFile(key) {
  let params = {
    Bucket: bucket,
    Key: key,
    ExpressionType: "SQL",
    Expression:
      "select s.custom_val,s.attempt, s.job_state, s.error_code, s.error_response  from s3object[*] s",
    InputSerialization: {
      CompressionType: "GZIP",
      JSON: {
        Type: "LINES"
      }
    },
    OutputSerialization: {
      JSON: {
        RecordDelimiter: ","
      }
    }
  };
  try {
    const result = await client.selectObjectContent(params).promise();

    const events = result.Payload;

    let results = [];

    // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#selectObjectContent-property

    for await (const event of events) {
      if (event.Records) {
        let payload = event.Records.Payload.toString();
        results.push(payload);
      } else if (event.Stats) {
        // handle Stats event
      } else if (event.Progress) {
        // handle Progress event
      } else if (event.Cont) {
        // handle Cont event
      } else if (event.End) {
        // handle End event
      }
    }

    // trim the trailing "," from the last element
    let lastResult = results[results.length - 1];
    if (lastResult[lastResult.length - 1] == ",") {
      results[results.length - 1] = lastResult.substr(0, lastResult.length - 1);
    }

    // make the string a json array
    let entirePayload = results.join("");
    entirePayload = "[" + entirePayload + "]";

    // logging
    /* let parts = key.split("/");
    fs.writeFileSync("test/" + parts[parts.length - 1], entirePayload); */

    let jsonPayload = JSON.parse(entirePayload);
    await postResultToSlack(jsonPayload, key);
  } catch (err) {
    // handle error
    console.log("processFile: ", key, " ", err);
  }
}

let fileKeys = [];
let params = {
  Bucket: bucket
};

// listKeys list jobs_backup bucket keys and process aborted
// and not proceesed jobs except gateway
async function listKeys() {
  try {
    const response = await client.listObjectsV2(params).promise();
    response.Contents.forEach(obj => fileKeys.push(obj.Key));

    if (response.NextContinuationToken) {
      params.ContinuationToken = response.NextContinuationToken;
      await listKeys();
    }

    //console.log("read complete....", fileKeys.length);

    await fileKeys.forEach(async fileKey => {
      if (
        !fileKey.includes("gw") &&
        !fileKey.includes("processed") &&
        fileKey.includes("gz") &&
        fileKey.includes("aborted")
      ) {
        await processFile(fileKey);
        await renameFile(fileKey);
      }
    });
  } catch (error) {
    console.log("listKeys: ", error);
  }
}

// start
listKeys();
