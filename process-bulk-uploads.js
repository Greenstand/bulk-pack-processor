require("dotenv").config();
const {
  connectionString,
  dataInputMicroserviceURI,
  dataInputMicroserviceURIV2,
} = require("./config/config");
const axios = require("axios").default;
const { Pool } = require("pg");

const pool = new Pool({
  connectionString,
});
//const Sentry = require('@sentry/node');
//Sentry.init({ dsn: Config.sentryDSN });

const v2MicroserviceURIV1Endpoints = dataInputMicroserviceURIV2 + "v1/";
const v1MicroserviceURIV2Endpoints = dataInputMicroserviceURI + "v2/";

const errorHandler = async (err, data) => {
  if (axios.isAxiosError(err)) {
    console.log("\n\n\n\n\n===================================");
    console.log({
      transformerUrl: err.config.url,
      data,
      response: err.response?.data || err.message,
    });
    console.log("===================================\n\n\n\n\n");
  }
};

const v2Requests = async (data, endpoint, v2Only) => {
  const optionsV2 = {
    method: "POST",
    url: dataInputMicroserviceURIV2 + endpoint,
    data,
  };

  const optionsV1 = {
    ...optionsV2,
    url: v1MicroserviceURIV2Endpoints + endpoint,
  };

  // send data to both v1 and v2
  await axios(optionsV2);

  if (!v2Only) {
    await axios(optionsV1);
  }
};

const v1Requests = async (data, endpoint, httpVerb) => {
  const optionsV1 = {
    method: httpVerb || "POST",
    url: dataInputMicroserviceURI + endpoint,
    data,
  };

  const optionsV2 = {
    ...optionsV1,
    method: httpVerb || "POST",
    url: v2MicroserviceURIV1Endpoints + endpoint,
  };

  // send data to both v1 and v2
  await axios(optionsV2);
  await axios(optionsV1);
};

(async () => {
  const query = {
    text: `SELECT *
    FROM bulk_tree_upload
    WHERE processed = FALSE
    ORDER BY KEY ASC`,
  };
  const rval = await pool.query(query);
  let failingBulkPacks = false;

  for (let row of rval.rows) {
    let shouldBeProcessed = true;
    console.log("processing key: " + row.key);
    const key = row.key;
    const bulkData = row.bulk_data;

    if (bulkData.pack_format_version === "2") {
      console.log("bulk pack format version 2 detected");
      // Version 2
      const wallet_registrations = bulkData.wallet_registrations;
      const device_configurations = bulkData.device_configurations;
      const sessions = bulkData.sessions;
      const captures = bulkData.captures;
      const messages = bulkData.messages;
      const tracks = bulkData.tracks;
      if (wallet_registrations?.length) {
        console.log("processing v2 wallet_registrations");
        for (const wallet_registration of wallet_registrations) {
          try {
            await v2Requests(
              { ...wallet_registration, key },
              "wallet_registrations"
            );
          } catch (e) {
            shouldBeProcessed = false;
            errorHandler(e, wallet_registration);
            continue;
          }
        }
        console.log("v2 wallet_registrations done");
      }

      if (device_configurations?.length) {
        console.log("processing v2 device_configurations");
        for (const device_configuration of device_configurations) {
          try {
            await v2Requests(
              { ...device_configuration, key },
              "device_configurations"
            );
          } catch (e) {
            shouldBeProcessed = false;
            errorHandler(e, device_configuration);
            continue;
          }
        }
        console.log("v2 device_configurations done");
      }

      if (sessions?.length) {
        console.log("processing v2 sessions");
        for (const session of sessions) {
          try {
            await v2Requests({ ...session, key }, "sessions", true);
          } catch (e) {
            shouldBeProcessed = false;
            errorHandler(e, session);
            continue;
          }
        }
        console.log("v2 sessions done");
      }

      if (tracks?.length) {
        console.log("processing v2 tracks");
        for (const track of tracks) {
          try {
            await v2Requests({ ...track, key }, "track", true);
          } catch (e) {
            shouldBeProcessed = false;
            errorHandler(e, track);
            continue;
          }
        }
        console.log("v2 tracks done");
      }

      if (captures?.length) {
        console.log("processing v2 captures");
        for (const capture of captures) {
          try {
            await v2Requests({ ...capture, key }, "captures");
          } catch (e) {
            shouldBeProcessed = false;
            errorHandler(e, capture);
            continue;
          }
        }
        console.log("v2 captures done");
      }

      if (messages?.length) {
        console.log("v2 messages");
        for (const message of messages) {
          try {
            await v2Requests({ ...message, key }, "messages", true);
          } catch (e) {
            shouldBeProcessed = false;
            errorHandler(e, message);
            continue;
          }
        }
        console.log("v2 messages done");
      }
    } else {
      // Version 1
      console.log("bulk pack format version 1 detected");
      if (bulkData.registrations?.length) {
        console.log("processing v1 registrations");
        for (let planter of bulkData.registrations) {
          try {
            await v1Requests({ ...planter, key }, "planter");
          } catch (e) {
            shouldBeProcessed = false;
            errorHandler(e, planter);
            continue;
          }
        }
        console.log("v1 registrations done");
      }

      if (bulkData.devices?.length) {
        console.log("processing v1 devices");
        for (let device of bulkData.devices) {
          try {
            await v1Requests({ ...device, key }, "device", "PUT");
          } catch (e) {
            shouldBeProcessed = false;
            errorHandler(e, device);
            continue;
          }
        }
        console.log("v1 devices done");
      }

      if (bulkData.trees?.length) {
        console.log("processing v1 trees");
        for (let tree of bulkData.trees) {
          try {
            await v1Requests({ ...tree, key }, "tree");
          } catch (e) {
            shouldBeProcessed = false;
            errorHandler(e, tree);
            continue;
          }
        }
        console.log("v1 trees done");
      }
    }

    const update = {
      text: `UPDATE bulk_tree_upload
      SET processed = TRUE,
      processed_at = now()
      WHERE id = $1`,
      values: [row.id],
    };
    console.log("update");
    if (shouldBeProcessed) {
      await pool.query(update);
    } else {
      failingBulkPacks = true;
    }
    console.log(`Processed bulk tree upload ${row.id}`);
  }
  console.log("done");
  pool.end();

  failingBulkPacks ? process.exit(1) : process.exit(0);
  process.exit(0);
})().catch((e) => {
  console.log(e);
  //Sentry.captureException(e);
  pool.end();

  console.log("notify-slack-reports done with catch");
  process.exit(1);
});
