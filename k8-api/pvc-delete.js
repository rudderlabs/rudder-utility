const k8s = require("@kubernetes/client-node");
const qs = require("qs");
const axios = require("axios");
const Queue = require("smart-request-balancer");
const dotenv = require("dotenv");
const getenv = require("getenv");
//dotenv.config();

const webhookURL = getenv(
  "WEBHOOK_URL",
  ""
);
const slackChannel = getenv("SLACK_CHANNEL", "");
const timeUnit = getenv("TIME_UNIT", "DAYS");

const isPvcToBeDeleted = getenv.bool("IS_PVC_TO_BE_DELETED", false);
let allowedUninstalledTimeUnits = getenv.int(
  "PVC_RETAIN_ALLOWED_TIME_UNITS",
  7
);

console.log(
  webhookURL,
  slackChannel,
  timeUnit,
  allowedUninstalledTimeUnits,
  isPvcToBeDeleted
);

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

const kc = new k8s.KubeConfig();
// uncomment this for local testing
//kc.loadFromDefault();
kc.loadFromCluster();

const k8sApi = kc.makeApiClient(k8s.CoreV1Api);

const pvcsAllNamespace = [];

// sendSlackNotification: send notification to slack
async function sendSlackNotification(pvc, deleted) {
  try {
    let slackRequestData;
    if (!deleted) {
      slackRequestData = {
        channel: slackChannel,
        text:
          "Name: " +
          pvc.name +
          " Namespace: " +
          pvc.namespace +
          " will be deleted on " +
          pvc.deletionScheduledDate.toISOString().split("T")[0] +
          "\n"
      };
    } else {
      slackRequestData = {
        channel: slackChannel,
        text:
          "Name: " +
          pvc.name +
          " Namespace: " +
          pvc.namespace +
          " to be/is deleted on " +
          pvc.deletionScheduledDate.toISOString().split("T")[0] +
          "\n"
      };
    }

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
            console.log(error)
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
    console.log("slack sending error: ", error);
  }
}

// deletePvcs: delete pvc based on deletion criteria and call to push notification
async function deletePvcs(pvcList) {
  let pvc, uninstalledDay, today;
  for (let i = 0; i < pvcList.length; i++) {
    pvc = pvcList[i];
    if (!pvc.labels["uninstalled-on"]) {
      continue;
    }
    pvc.labels["uninstalled-on"] = parseInt(pvc.labels["uninstalled-on"]);

    if (timeUnit == "DAYS") {
      uninstalledDay = (pvc.labels["uninstalled-on"]) / (60 * 60 * 24); //days since epoch when it was uninstalled, it comes in secs
      today = parseInt((Date.now()) / (60 * 60 * 24 * 1000)); // from ms to days
      pvc.deletionScheduledDate = new Date(
        (pvc.labels["uninstalled-on"] +
          allowedUninstalledTimeUnits * 60 * 60 * 24) *
          1000
      );
    }

    if (timeUnit == "SECS") {
      uninstalledDay = pvc.labels["uninstalled-on"]; //seconds since epoch when it was uninstalled
      today = parseInt(Date.now()/1000); // ms to secs
      // debug logs
       console.log("uinstall Date: ", pvc.labels["uninstalled-on"])
       console.log("allowedUninstalledTimeUnits: ", allowedUninstalledTimeUnits)
       console.log("expected date: ", pvc.labels["uninstalled-on"] + allowedUninstalledTimeUnits)
      pvc.deletionScheduledDate = new Date(
        (pvc.labels["uninstalled-on"] + allowedUninstalledTimeUnits) * 1000
      );
    }

    today =  Math.round(today)
    uninstalledDay = Math.round(uninstalledDay)

    let diffUnits = today - uninstalledDay

    // debug logs
     console.log("todayUnit: ", today + " uninstalledUnit: ", uninstalledDay)
     console.log ("diff units: " + diffUnits + " allowed diff units: " + allowedUninstalledTimeUnits)

    if (diffUnits < allowedUninstalledTimeUnits) {
      try {
        await sendSlackNotification(pvc, false);
      } catch (error) {
        console.log("sending to slack error", error, pvc);
      }
    }

    if (today - uninstalledDay >= allowedUninstalledTimeUnits) {
      try {
        if (isPvcToBeDeleted) {
          console.log("===deleting pvc ====")
          await k8sApi.deleteNamespacedPersistentVolumeClaim(
            pvc.name,
            pvc.namespace
          );
        }
        await sendSlackNotification(pvc, true);
      } catch (error) {
        console.log("pvc deletion request error", error, pvc);
      }
    }
  }
}

// getAllPvcs: gets all pvcs and push them for deletion
async function getAllPvcs(_continue) {
  try {
    let result = await k8sApi.listPersistentVolumeClaimForAllNamespaces(
      false,
      _continue
    );

    result.body.items.forEach(pvc => {
      pvcsAllNamespace.push({
        name: pvc.metadata.name,
        namespace: pvc.metadata.namespace,
        labels: { ...pvc.metadata.labels }
      });
    });

    if (result.body.metadata._continue) {
      getAllPvcs(result.body.metadata._continue);
    }

    // debug log to check
    console.log(JSON.stringify(pvcsAllNamespace));

    await deletePvcs(pvcsAllNamespace);
  } catch (err) {
    console.log(err);
  }
}

// start: get all pvs by all namespace, delete based on uinstalled-on, send notification to slack
getAllPvcs();
