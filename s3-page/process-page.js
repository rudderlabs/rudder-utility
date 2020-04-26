const S3 = require("aws-sdk/clients/s3");
const fs = require("fs");

const getenv = require("getenv");

const client = new S3();
const bucket = getenv("S3_BUCKET", "");
const keyPrefix = getenv("KEY_PREFIX", "");
const pageToSearch = getenv("PAGE_URL", "");
const identifier = getenv("IDENTIFIER", "");

var deviceIdToFieldsMap = new Map();

function map_to_object(map) {
  const out = Object.create(null);
  map.forEach((value, key) => {
    if (value instanceof Map) {
      // console.log("value of type map...");
      out[key] = map_to_object(value);
    } else if (value instanceof Set) {
      // console.log("value of type set....");
      out[key] = [...value];
    } else if (value instanceof Object) {
      // console.log("value of type object...");
      Object.keys(value).forEach(valueKey => {
        if (value.hasOwnProperty(valueKey)) {
          out[valueKey] = map_to_object(value[valueKey]);
        }
      });
    } else {
      out[key] = value;
    }
  });
  return out;
}

async function processData(data, fileKey) {
  var topLevelKey;
  var pageUrl;

  try {
    data.forEach(event => {
      if (identifier == "ip") {
        topLevelKey = event.ip;
      } else {
        topLevelKey = event.device_id;
      }

      if(event.url) {
          pageUrl = event.url
      } else {
          pageUrl = event.context_page
      }

      if (topLevelKey) {
        // console.log("\n processing device_ids.....");
        if (!deviceIdToFieldsMap.has(topLevelKey)) {
          deviceIdToFieldsMap.set(topLevelKey, new Map());
          var deviceIdMap = deviceIdToFieldsMap.get(topLevelKey);
          deviceIdMap.set("ip", new Set());
          deviceIdMap.set("ua", new Set());
          deviceIdMap.set("anonIdToUserId", new Map());
          deviceIdMap.set("userId", new Map());
          deviceIdMap.set("anonymousId", new Map());
        }

        var ipSet = deviceIdToFieldsMap.get(topLevelKey).get("ip");
        var uaSet = deviceIdToFieldsMap.get(topLevelKey).get("ua");
        var anonIdMap = deviceIdToFieldsMap.get(topLevelKey).get("anonymousId");
        var userIdMap = deviceIdToFieldsMap.get(topLevelKey).get("userId");
        var anonIdToUserIdLink = deviceIdToFieldsMap
          .get(topLevelKey)
          .get("anonIdToUserId");

        ipSet.add(event.ip);
        uaSet.add(event.ua);

        // console.log("ipSet: ", ipSet, " uaSet: " + JSON.stringify(uaSet));

        if (!anonIdMap.has(event.anonymousId)) {
          anonIdMap.set(event.anonymousId, new Map());
        }
        var anonIdToPageUrlCountsMap = anonIdMap.get(event.anonymousId);
        if (!anonIdToPageUrlCountsMap.has(pageUrl)) {
          anonIdToPageUrlCountsMap.set(pageUrl, 0);
        }
        anonIdToPageUrlCountsMap.set(
          pageUrl,
          anonIdToPageUrlCountsMap.get(pageUrl) + 1
        );
        anonIdMap.set(event.anonymousId, anonIdToPageUrlCountsMap);

        // console.log("anonIdMap: ", anonIdMap);

        if (event.userId) {
          if (!userIdMap.has(event.userId)) {
            userIdMap.set(event.userId, new Map());
          }
          var userIdToPageUrlCountsMap = userIdMap.get(event.userId);
          if (!userIdToPageUrlCountsMap.has(pageUrl)) {
            userIdToPageUrlCountsMap.set(pageUrl, 0);
          }
          userIdToPageUrlCountsMap.set(
            pageUrl,
            userIdToPageUrlCountsMap.get(pageUrl) + 1
          );
          userIdMap.set(event.userId, userIdToPageUrlCountsMap);
        }

        // console.log("userIdMap: ", userIdMap);

        if (!anonIdToUserIdLink.has(event.anonymousId)) {
          anonIdToUserIdLink.set(event.anonymousId, new Set());
        }
        if (event.userId) {
          anonIdToUserIdLink.set(
            event.anonymousId,
            anonIdToUserIdLink.get(event.anonymousId).add(event.userId)
          );
        }

        // console.log("anonIdToUserIdLink: ", anonIdToUserIdLink);
      }
    });

    // console.log(
    //   "\n deviceIdToFieldsMap: final ",
    //   JSON.stringify(map_to_object(deviceIdToFieldsMap))
    // );
  } catch (error) {
    console.log("processData: ", error);
  }
}

// processFile get the required keys from the file, make a json payload
// then call postToSlack
// then rename file in aws
async function processFile(key) {
  var query;
  if (pageToSearch) {
    query =
      "select _1.request_ip as ip, _1.context.device.rudder_device_id as device_id, _1.context.userAgent as ua, _1.anonymousId, _1.userId, _1.context.page.url as context_page, _1.properties.url, _1.sentAt from s3object[*] where  _1.type='page' AND _1.properties.url=" +
      "'" +
      pageToSearch +
      "'";
  } else {
    query =
      "select _1.request_ip as ip, _1.context.device.rudder_device_id as device_id, _1.context.userAgent as ua, _1.anonymousId, _1.userId, _1.context.page.url as context_page, _1.properties.url, _1.sentAt from s3object[*] where  _1.type='page'";
  }
  let params = {
    Bucket: bucket,
    Key: key,
    ExpressionType: "SQL",
    Expression: query,
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

    // console.log("length...", results.length);
    if (results.length > 0) {
      // trim the trailing "," from the last element
      let lastResult = results[results.length - 1];
      if (lastResult && lastResult[lastResult.length - 1] == ",") {
        results[results.length - 1] = lastResult.substr(
          0,
          lastResult.length - 1
        );
      }

      // make the string a json array

      let entirePayload = results.join("");
      entirePayload = "[" + entirePayload + "]";

      // logging
      /* let parts = key.split("/");
    fs.writeFileSync("test/" + parts[parts.length - 1], entirePayload); */

      let jsonPayload = JSON.parse(entirePayload);
      //console.log(JSON.stringify(jsonPayload));
      await processData(jsonPayload, key);
    }
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

    //console.log("read complete....", fileKeys);

    // await fileKeys.forEach(async fileKey => {
    //   await processFile(fileKey);
    // });

    for (var i = 0; i < fileKeys.length; i++) {
      if (keyPrefix) {
        if (fileKeys[i].includes(keyPrefix)) {
          await processFile(fileKeys[i]);
        } else {
          continue;
        }
      } else {
        await processFile(fileKeys[i]);
      }
    }

    if (deviceIdToFieldsMap.size > 0) {
      fs.appendFileSync(
        "output.json",
        JSON.stringify(map_to_object(deviceIdToFieldsMap)) + "\n"
      );
    }
  } catch (error) {
    console.log("listKeys: ", error);
  }
}

// start
listKeys();
