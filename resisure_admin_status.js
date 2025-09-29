import * as fs from 'fs';
const verJSON = JSON.parse(fs.readFileSync('version.json'));
if (process.argv.length > 2){
  process.argv.forEach(function (val, index, array) {
    if (val == '-v') {
      verJSON.forEach(function(ver, index, array) {
        console.log( '\r\n' + 'version number: ' + ver.version+ ', date:' +ver.date+ '')
        console.log(ver.information)
      }
      )
      process.exit(0)
    }
  }
  )
} else {
     console.log('version number: ' +verJSON[0].version+ ', date:' +verJSON[0].date+ '')
     console.log(verJSON[0].information)
 }

//Config File Reader
import * as dotenv from 'dotenv';
dotenv.config({ path: 'config.env' });
//END CONFIG READER
//AMQP
import * as amqp from 'amqplib';
const waitMs = milliseconds => new Promise(resolve => setTimeout(() => resolve(), milliseconds));
const amqp_options = {
	protocol: process.env.RABBITMQ_PROTOCOL,
  username: process.env.RABBITMQ_USERNAME,
  password: process.env.RABBITMQ_PASSWORD,
  host: process.env.RABBITMQ_HOST,
  queue: process.env.RABBITMQ_STATUS_QUEUE
};
const amqp_host = `${amqp_options.protocol}://${amqp_options.username}:${amqp_options.password}@${amqp_options.host}`;
const amqp_exchange = 'amq.topic';
const publishOptions = { persistent: true }; // sets delivery_mode to 2 (message survives a broker restart)
let messageChannel;
//END AMQP
//Set the data dir for debug
const data_dir="./data";
let file_ext="dat";
//HTTP
import * as https from 'https';
import * as qs from 'querystring';

// These lines make "require" available
import { createRequire } from "module";
const require = createRequire(import.meta.url);
var unescapeJs = require('unescape-js');

const host_url=process.env.HOST_URL;

import Pl from 'pg'
import { json } from 'stream/consumers';
const { Pool } = Pl;
const pool = new Pool({
  host: process.env.POSTGRES_HOST,
  port: process.env.POSTGRES_PORT,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PWD,
  max: process.env.POSTGRES_MAX_CLIENTS,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});


//get the API data
const api_auth=process.env.AUTH_TOKEN;
const api_email=process.env.AUTH_EMAIL;

const workspace=process.env.WORKSPACE;

//post error to poison queue
const putError = async (data = {}, service = "", error = "", error_code = 0 ) => {
  return new Promise((resolve) => {
    console.log("ERROR - data=" + JSON.stringify(data) + ",service=" + service + ",error=" + error + ",error code=" + error_code);
    if (service) {
      //put message on data insert queue
      (async () => {
        try{
          const res = await pool.query('INSERT into errors (error_source,error_code,error_msg,error_data,error_datetime) VALUES ($1,$2,$3,$4,now()) RETURNING error_id',[service,error_code,error,JSON.stringify(data)]);
          console.log("Error Message Saved: " + res.rows.length);
          resolve(true);
        } catch(err) {
          console.log("Failed to insert Error Message - " + err.message);
          resolve(false);
        }
      })();
    } else {
      //missing data
      resolve(false);
    }
  });
}

const addError = async (workspace = "", data = {}) => {
  console.log("Updating Zoho Property data: " + JSON.stringify(data));
  //const now = FormattedDate(new Date());
  const dashboard = "Error Log";
  const post_data = qs.stringify({
    "ZOHO_ACTION": "ADDROW",
    "ZOHO_OUTPUT_FORMAT": "JSON",
    "ZOHO_ERROR_FORMAT": "JSON",
    "ZOHO_API_VERSION": "1.0",
    "authtoken": api_auth,
    "Error Description" : data.error,
    "Customer Reference" : data.customer_ref,
  });
  console.log(post_data);

  let post_options = {
    hostname: 'dashboard.resisure.co.uk',
    path: "/api/" + api_email + "/" + encodeURIComponent(workspace) + "/" + encodeURIComponent(dashboard), 
    port: 443,
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  };
return new Promise((resolve, reject) => {
    let post_req = https.request(post_options, (post_res) => {
      try {
      let chunks = [];
      post_res.on("data", function (chunk) {
        chunks.push(chunk);
      });
      post_res.on('end', () => {
        let body = Buffer.concat(chunks);
        body = unescapeJs(body.toString('utf8'));
        resolve(JSON.parse(body));
      });
    } catch (err) {
      console.log("Error processing response: " + err.message);
      reject(err);
    }
    });
    post_req.on('error', (e) => {
      reject(e);
    });
    post_req.write(post_data);
    post_req.end();
  
  });
}



function FormattedDate(date) {
  const now = date;
  return now.getFullYear() + '-' +
  String(now.getMonth() + 1).padStart(2, '0') +
  '-' + String(now.getDate()).padStart(2, '0') + ' ' +
  String(now.getHours()).padStart(2, '0') + ':' +
  String(now.getMinutes()).padStart(2, '0') + ':' +
  String(now.getSeconds()).padStart(2, '0');
}

const updateCustomerZoho = async (workspace = "", customer = 0, data = {}, dashboard = "") => {
  console.log("Updating Zoho Customer data: " + JSON.stringify(data));
  let formattedDateAdded, formattedDateUpdated, error;

  //format date for zoho dashboard
  let post_data;
  if (data.action == "amend" || data.action == "deactivate" || data.action == "reactivate") {
    if (data.date_updated) {
      formattedDateUpdated = FormattedDate(new Date(data.date_updated));
    }
    if (data.error) {
      error = data.error.replace(/"/g, '');
    }
    post_data = qs.stringify({
    "ZOHO_ACTION": "UPDATE",
    "ZOHO_OUTPUT_FORMAT": "JSON",
    "ZOHO_ERROR_FORMAT": "JSON",
    "ZOHO_API_VERSION": "1.0",
    "authtoken": api_auth,
    "Customer ID" : data.customer_id, //set customer id
    "status" : data.status, //set status
    "Action to execute": "None", // Default to 'None' after changes are made
    "Date_Updated": formattedDateUpdated,
    "Error message": error,
    "ZOHO_CRITERIA" : "(customer=" + customer + ")" 
  });
  } else if (data.action == "new") {
    if (data.date_added) {
      formattedDateAdded = FormattedDate(new Date(data.date_added));
    }
    else {
      formattedDateAdded = null;
    }
    if (data.error) {
      error = data.error.replace(/"/g, '');
    }
    post_data = qs.stringify({
    "ZOHO_ACTION": "UPDATE",
    "ZOHO_OUTPUT_FORMAT": "JSON",
    "ZOHO_ERROR_FORMAT": "JSON",
    "ZOHO_API_VERSION": "1.0",
    "authtoken": api_auth,
    "Customer ID" : data.customer_id, //set customer id
    "Status" : data.status, //set status
    "Action to execute": "None", // Default to 'None' after changes are made
    "Date Added" : formattedDateAdded,
    "Error message": error,
    "ZOHO_CRITERIA" : "(customer=" + customer + ")" 
  });
  } 
  console.log(post_data);

  let post_options = {
    hostname: 'dashboard.resisure.co.uk',
    path: "/api/" + api_email + "/" + encodeURIComponent(workspace) + "/" + encodeURIComponent(dashboard), 
    port: 443,
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  };
  return new Promise((resolve, reject) => {
    let post_req = https.request(post_options, (post_res) => {
      try {
      let chunks = [];
      post_res.on("data", function (chunk) {
        chunks.push(chunk);
      });
      post_res.on('end', () => {
        let body = Buffer.concat(chunks);
        body = unescapeJs(body.toString('utf8'));
        resolve(JSON.parse(body));
      });
    } catch (err) {
      console.log("Error processing response: " + err.message);
      reject(err);
    }
    });
    post_req.on('error', (e) => {
      reject(e);
    });
    post_req.write(post_data);
    post_req.end();
  });
}

const updatePropertyZoho = async (workspace = "", property = 0, data = {}, dashboard = "") => {
  console.log("Updating Zoho Property data: " + JSON.stringify(data));
  let formattedDateAdded, formattedDateUpdated, error, post_data;
  if (data.action == "amend" || data.action == "deactivate" || data.action == "reactivate") {
    if (data.date_updated) {
      formattedDateUpdated = FormattedDate(new Date(data.date_updated));
    }
    if (data.error) {
      error = data.error.replace(/"/g, '');
    }
    post_data = qs.stringify({
    "ZOHO_ACTION": "UPDATE",
    "ZOHO_OUTPUT_FORMAT": "JSON",
    "ZOHO_ERROR_FORMAT": "JSON",
    "ZOHO_API_VERSION": "1.0",
    "authtoken": api_auth,
    "Property ID" : data.property_id, //set customer id
    "Status" : data.status, //set status
    "Property Action": "None", // Default to 'None' after changes are made
    "Date Updated": formattedDateUpdated,
    "Error message": error,
    "ZOHO_CRITERIA" : "(property=" + property + ")" 
  });
  } else if (data.action == "new") {
    if (data.date_added) {
      formattedDateAdded = FormattedDate(new Date(data.date_added));
    }
    else {
      formattedDateAdded = null;
    }
    if (data.error) {
      error = data.error.replace(/"/g, '');
    }
    post_data = qs.stringify({
    "ZOHO_ACTION": "UPDATE",
    "ZOHO_OUTPUT_FORMAT": "JSON",
    "ZOHO_ERROR_FORMAT": "JSON",
    "ZOHO_API_VERSION": "1.0",
    "authtoken": api_auth,
    "Property ID" : data.property_id, //set customer id
    "Status" : data.status, //set status
    "Property Action": "None", // Default to 'None' after changes are made
    "Date Added" : formattedDateAdded,
    "Error message": error,
    "ZOHO_CRITERIA" : "(property=" + property + ")" 
  });
  } 
  console.log(post_data);

  let post_options = {
    hostname: 'dashboard.resisure.co.uk',
    path: "/api/" + api_email + "/" + encodeURIComponent(workspace) + "/" + encodeURIComponent(dashboard), 
    port: 443,
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  };

  return new Promise((resolve, reject) => {
    let post_req = https.request(post_options, (post_res) => {
      try {
      let chunks = [];
      post_res.on("data", function (chunk) {
        chunks.push(chunk);
      });
      post_res.on('end', () => {
        let body = Buffer.concat(chunks);
        body = unescapeJs(body.toString('utf8'));
        resolve(JSON.parse(body));
      });
    } catch (err) {
      console.log("Error processing response: " + err.message);
      reject(err);
    }
    });
    post_req.on('error', (e) => {
      reject(e);
    });
    post_req.write(post_data);
    post_req.end();
  
  });
}

const updateAreaZoho = async (workspace = "", area = 0, data = {}, dashboard = "") => {
  console.log("Updating Zoho Area data: " + JSON.stringify(data));
  let formattedDateAdded, error, post_data;
  if (data.action == "amend") {
    if (data.error) {
      error = data.error.replace(/"/g, '');
    }
    post_data = qs.stringify({
    "ZOHO_ACTION": "UPDATE",
    "ZOHO_OUTPUT_FORMAT": "JSON",
    "ZOHO_ERROR_FORMAT": "JSON",
    "ZOHO_API_VERSION": "1.0",
    "authtoken": api_auth,
    "Area ID" : data.area_id, //set area id
    "Area Action": "None", // Default to 'None' after changes are made
    "Error message": error,
    "ZOHO_CRITERIA" : "(area=" + area + ")" 
  });
  } else if (data.action == "new") {
    if (data.date_added) {
      formattedDateAdded = FormattedDate(new Date(data.date_added));
    }
    else {
      formattedDateAdded = null;
    }
    if (data.error) {
      error = data.error.replace(/"/g, '');
    }
    post_data = qs.stringify({
    "ZOHO_ACTION": "UPDATE",
    "ZOHO_OUTPUT_FORMAT": "JSON",
    "ZOHO_ERROR_FORMAT": "JSON",
    "ZOHO_API_VERSION": "1.0",
    "authtoken": api_auth,
    "Area ID" : data.area_id, //set customer id
    "Area Action": "None", // Default to 'None' after changes are made
    "Date Added" : formattedDateAdded,
    "Error message": error,
    "ZOHO_CRITERIA" : "(area=" + area + ")" 
  });
  } 
  console.log(post_data);

  let post_options = {
    hostname: 'dashboard.resisure.co.uk',
    path: "/api/" + api_email + "/" + encodeURIComponent(workspace) + "/" + encodeURIComponent(dashboard), 
    port: 443,
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  };

  return new Promise((resolve, reject) => {
    let post_req = https.request(post_options, (post_res) => {
      try {
      let chunks = [];
      post_res.on("data", function (chunk) {
        chunks.push(chunk);
      });
      post_res.on('end', () => {
        let body = Buffer.concat(chunks);
        body = unescapeJs(body.toString('utf8'));
        resolve(JSON.parse(body));
      });
    } catch (err) {
      console.log("Error processing response: " + err.message);
      reject(err);
    }
    });
    post_req.on('error', (e) => {
      reject(e);
    });
    post_req.write(post_data);
    post_req.end();
  
  });
}

const getStatus = async (reconnectDelay = 500) => {{
  console.log(amqp_options);
  try {
    const connection = await amqp.connect(amqp_host);
    console.log('Connected to message broker');
    messageChannel = await connection.createChannel();
    console.log('Message channel created');
    // Set Queue and routing key
    const queue = amqp_options.queue;
    const routingKey = queue;
    // Assert the queue
    // If the queue is already in existence (i.e. because chirpstack created it), but durable does not match, this will
    // fail and this service will fail to start (it will throw an informative error)
    const amqpOptions = {
      durable: true, // Queue survives a broker restart
      arguments: { confirm: true }
    };
    await messageChannel.assertQueue(queue, amqpOptions);
    await messageChannel.bindQueue(queue, amqp_exchange, routingKey);
    const queueDetails = await messageChannel.checkQueue(queue);
    await messageChannel.prefetch(1);
    //Process each message
    await messageChannel.consume(queue, amqpEncodedMessage => {
      (async () => {
        //Process message
        const message = amqpEncodedMessage.content.toString();
        try {
          const messageJSON = JSON.parse(message);
          console.log("MSG:");
          console.log(messageJSON);
          for (let i = 0; i < messageJSON.length; i++) {
            if (messageJSON[i].dashboard === 'customers') {
              const dashboard = "Customer Admin";
              console.log("Processing Customer Dashboard Message: " + JSON.stringify(messageJSON));
              const status = await updateCustomerZoho(workspace, messageJSON[i].customer, messageJSON[i], dashboard);
              console.log("Customer Dashboard Updated: " + JSON.stringify(status));

            }
            else if (messageJSON[i].dashboard === 'properties') {
            const dashboard = "Property Admin";
            console.log("Processing Property Dashboard Message: " + JSON.stringify(messageJSON));
            const status = await updatePropertyZoho(workspace, messageJSON[i].property, messageJSON[i], dashboard);
            console.log("Property Dashboard Updated: " + JSON.stringify(status));
            } 
            else if (messageJSON[i].dashboard === 'areas') {
              const dashboard = "Area Admin";
              console.log("Processing Area Dashboard Message: " + JSON.stringify(messageJSON));
              //currently no area update function
              const status = await updateAreaZoho(workspace, messageJSON[i].area, messageJSON[i], dashboard);
              console.log("Area Dashboard Updated: " + JSON.stringify(status));
            }
            // else if (messageJSON[i].dashboard === 'errors') {
            //   console.log("Processing Error Dashboard Message: " + JSON.stringify(messageJSON));
            //   const statusErr = await addError(workspace, messageJSON[i]);
            //   console.log("Error Dashboard Updated: " + JSON.stringify(statusErr));
            // }
            // messageChannel.ack(amqpEncodedMessage);
        }
          //clear the message from the queue
          messageChannel.ack(amqpEncodedMessage);
        } catch (err) {
          console.log('Failed to process Message: ' + err.message);
          //log error
          const errJSON = {
            "process": "messageChannel.consume",
            "error": "Failed to process Message: " + err.message,
            "data": message
          }
          const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
          messageChannel.ack(amqpEncodedMessage);
        }
      })();
      
    });
    //if the message channel connection is lost reconnect
    messageChannel.on('close', async () => {
      await connection.close();
      console.log('Lost connection to rabbitMQ');
      await getStatus(); // Reconnect
    });
  } catch (err) {
    console.log('ERROR connecting to RabbitMQ:', err.message);
    // Pause will double each time, capped to a maximum of 60 seconds
    reconnectDelay = Math.min(60000, reconnectDelay *= 2);
    await waitMs(reconnectDelay);
    await getStatus(reconnectDelay);

  }
  finally {
    console.log('Waiting for Messages on Queue: ' + amqp_options.queue);
  }
}};

(async () => {
  await getStatus();
})();