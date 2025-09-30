// Teddy Hayward
// 2025-07-01
// ResiSure Admin Service for adding customer data
// This service is responsible for importing customer data 
import { createRequire } from "module";
const require = createRequire(import.meta.url);

//check for version request
import * as fs from 'fs';
const verJSON = JSON.parse(fs.readFileSync('version.json'));
if (process.argv.length > 2) {
  process.argv.forEach(function (val, index, array) {
    if (val === '-v') {
      verJSON.forEach(function (ver, index, array) {
        console.log('\r\n' + 'version number: ' + ver.version + ', date:' + ver.date + '')
        console.log(ver.information)
      }
      )
      process.exit(0)
    }
  }
  )
} else {
  console.log('version number: ' + verJSON[0].version + ', date:' + verJSON[0].date + '')
  console.log(verJSON[0].information)
}
//Config File reader
import * as dotenv from 'dotenv';
dotenv.config({ path: 'config.env' });
//Setup Postgres
import Pl from 'pg'
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
//AMQP
import * as amqp from 'amqplib';
import { parse } from "path";

//get the API data
const api_auth=process.env.AUTH_TOKEN;
const api_email=process.env.AUTH_EMAIL;

//HTTP
import * as https from 'https';
import * as qs from 'querystring';

const waitMs = milliseconds => new Promise(resolve => setTimeout(() => resolve(), milliseconds));
const amqp_options = {
  protocol: process.env.RABBITMQ_PROTOCOL,
  username: process.env.RABBITMQ_USERNAME,
  password: process.env.RABBITMQ_PASSWORD,
  host: process.env.RABBITMQ_HOST
};
const amqp_host = `${amqp_options.protocol}://${amqp_options.username}:${amqp_options.password}@${amqp_options.host}`;
const amqp_exchange = 'amq.topic';
let messageChannel;

//post error to poison queue
const putError =(data = {}, service = "", error = "", error_code = 0) =>{
  return new Promise((resolve) => {
    console.log("ERROR - data=" + JSON.stringify(data) + ",service=" + service + ",error=" + error + ",error code=" + error_code);
    if (service) {
      //put message on data insert queue
      (async () => {
        try {
          const res = await pool.query('INSERT into errors (error_source,error_code,error_msg,error_data,error_datetime) VALUES ($1,$2,$3,$4,now()) RETURNING error_id', [service, error_code, error, JSON.stringify(data)]);
          console.log("Error Message Saved: " + res.rows.length);
          resolve(true);
        } catch (err) {
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


//Post a JSON message to a queue
const postQueue = (msgJSON) =>{
  return new Promise((resolve) => {
    //put message on data insert queue
    (async () => {
      //assign AMQP constants
      const routingKey = 'resisure_admin_status';
      const queue = routingKey;
      // Assert the queue
      const amqpOptions = {
        durable: true, // Queue survives a broker restart
        arguments: { confirm: true }
      };
      let ret = {
        "result": true
      };
      try {
        console.log("Message to Queue: " + JSON.stringify(msgJSON));
        //Queue Message
        let queueMsg = JSON.stringify(msgJSON);
        //console.log("Report Queue Data - " + queueMsg);
        await messageChannel.assertQueue(queue, amqpOptions);
        await messageChannel.bindQueue(queue, amqp_exchange, routingKey);
        //Add the Msg to the service queue
        const buffer = Buffer.from(queueMsg);
        await messageChannel.publish(amqp_exchange, queue, buffer/*, publishOptions*/);
        console.log("Message Queued: " + queue);
        resolve(ret);
      } catch (err) {
        const errJSON = {
          "process": "adminStatusQueue",
          "error": err.message,
          "data": msgJSON
        }
        await putError(errJSON, "resisure_admin", errJSON.error, err.code);
        console.log("Failed to post msg to Status Queue: " + errJSON.error);
        ret.result = false;
        resolve(ret);
      }
    })();
  });
}



const connectMessageBroker = async (reconnectDelay = 500, service = "", message = {}) => {
  console.log(amqp_options);
  try {
    const connection = await amqp.connect(amqp_host);
    console.log('Connected to message broker');
    messageChannel = await connection.createChannel();
    console.log('Message channel created');
    // Set Queue and routing key
    const queue = 'resisure_admin';
    const routingKey = "customer";
    // Assert the queue
    // If the queue is already in existence (i.e. because chirpstack created it), but durable does not match, this will
    // fail and this service will fail to start (it will throw an informative error)
    const amqpOptions = {
      durable: true, // Queue survives a broker restart
      arguments: { confirm: true }
    };
    // Assert the queue and bind it to the exchange with the routing key
    await messageChannel.assertQueue(queue, amqpOptions);
    await messageChannel.bindQueue(queue, amqp_exchange, routingKey);
    // Check if the queue exists and get its details
    const queueDetails = await messageChannel.checkQueue(queue);
    console.log(`Queue '${queue}' asserted. Waiting message count: ${queueDetails.messageCount}`);
    await messageChannel.prefetch(1);
    //Process each message
    await messageChannel.consume(queue, amqpEncodedMessage => {
      (async () => {
        //assign the message
        const message = amqpEncodedMessage.content.toString();
        try {
          //Process message
          const messageJSON = JSON.parse(message);
          console.log("MSG:");
          console.log(messageJSON);
          /*
          The message should be in the following format for a manual message:
          {
          "type" : "manual",
          "database" : <properties or customers>,
          "datetime" : <datetime in ISO format yyyy-mm-dd HH:MM:SS:MS>,
          "customers" : [
          { "action" : "<new or amend or deactivate>" ,
          "customer_id": customer_id,
          "customer_ref": customer_ref,
          "customer_name": customer_name,
          "email": email,
          "customer_contact": customer_contact,
          "summary_reports": summary_reports,
          "property_reports": property_reports,
          "date_updated": date_updated,
          "status": cStatus},
        ]} 
          The message should be in the following format for an scheduled message:
          { TO DO: make a format and process for schedualed messages }
        */
          //console.log("Date Added: " + dateAdded);
          //determine if the action is new, amend or deactivate
          //determine if the message is a manual or automatic message
          
          if (messageJSON.table === "customers") {
            let status = [];
            let statusMsg = {};
            console.log("Processing a " + messageJSON.type + " request for processing " + messageJSON.customers.length + " customers");
              if (messageJSON.type === "manual") {
              //manual message
              for (let i in messageJSON.customers) {
                const customer = messageJSON.customers[i];
                try {
                  statusMsg = {
                    "dashboard": "customers",
                    "customer" : customer.customer,
                    "action": customer.action,
                    "customer_id": customer.customer_id,
                    "customer_ref": customer.customer_ref,
                    "customer_name": customer.customer_name,
                    "email": customer.email,
                    "contact_name": customer.customer_contact,
                    "summary_reports": customer.summary_reports,
                    "property_reports": customer.property_reports,
                  }

                  //check the action type
                  //this should add a new customer and amend or deactivate an existing customer
                  //customer_id is only required for amend and deactivate actions
                  //if inputted for adding it will ignore it as the database will auto-generate it
                  if (customer.action === "new") {
                    console.log("Adding Customer: " + customer.customer_ref);
                    //insert the customer into the database
                    let dateAdded = new Date()

                    const res = await pool.query('INSERT INTO customers (customer_ref, customer_name, customer_email, date_added, customer_contact, status) VALUES ($1, $2, $3, $4, $5, $6) RETURNING customer_id', [customer.customer_ref, customer.customer_name, customer.email, dateAdded, customer.customer_contact, 1]);
                    console.log("Customer Inserted: " + res.rows[0].customer_id)
                    const sRep = await pool.query('INSERT INTO customer_reports (customer_id, report_structure_property_id, report_output, status, summary_report) VALUES ($1, $2, $3, $4, $5) RETURNING customer_reports_id', [res.rows[0].customer_id, customer.property_reports, 1, 1 , customer.summary_reports]);
                    
                    
                    statusMsg.status = 1; 
                    statusMsg.date_added = customer.datetime;
                    
                    //console.log("Customer Reports Inserted: " + sRep.rows[0].customer_reports_id + ", " + pRep.rows[0].customer_reports_id)
                    //set the status message
                    statusMsg.customer_id = res.rows[0].customer_id;
                    status.push(statusMsg);
                    
                    console.log("Status Message Posted: " + JSON.stringify(statusMsg));

                  } else if (customer.action === "amend") {
                    console.log("Updating Customer: " + customer.customer_id);

                    //update the customer in the database

                    let dateUpdated = new Date()

                    const res = await pool.query('UPDATE customers set customer_ref = $1, customer_name = $2, customer_email = $3, date_updated = $5, customer_contact = $6, status = $7 WHERE customer_id = $4 RETURNING customer_id', [customer.customer_ref, customer.customer_name, customer.email, customer.customer_id, dateUpdated, customer.customer_contact, 1])
                    console.log("Customer Updated: " + res.rows[0].customer_id)
                    const sRep = await pool.query('UPDATE customer_reports set report_structure_property_id = $2, summary_report = $3 WHERE customer_id = $1 RETURNING customer_reports_id', [customer.customer_id, customer.property_report ,customer.summary_reports]);
                    

                    statusMsg.status = 1; //set status to 1 for amended customers
                    statusMsg.date_updated = customer.datetime;

                    //console.log("Customer Reports Updated: " + sRep.rows[0].customer_reports_id + ", " + pRep.rows[0].customer_reports_id)
                    //set the status message

                    statusMsg.customer_id = res.rows[0].customer_id;
                    status.push(statusMsg);
                    
                    console.log("Status Message Posted: " + JSON.stringify(statusMsg));

                  //deactivate the customer
                  } else if (customer.action === "deactivate") {
                    console.log("Deactivating Customer: " + customer.customer_id);
                    //deactivate the customer in the database
                    const res = await pool.query('UPDATE customers set status = 0 WHERE customer_id = $1 RETURNING customer_id', [customer.customer_id]);
                    const rep = await pool.query('UPDATE customer_reports set status = 0 WHERE customer_id = $1', [customer.customer_id]);
                    console.log("Customer Deactivated: " + res.rows[0].customer_id)
                    //set the status message
                    statusMsg.customer_id = res.rows[0].customer_id;
                    statusMsg.status = 0; //set status to 0 for deactivated customers
                    statusMsg.date_updated = customer.datetime;
                    status.push(statusMsg);
                  } else if (customer.action === "reactivate") {
                    console.log("Reactivating Customer: " + customer.customer_id);
                    //reactivate the customer in the database
                    const res = await pool.query('UPDATE customers set status = 1 WHERE customer_id = $1 RETURNING customer_id', [customer.customer_id]);
                    const rep = await pool.query('UPDATE customer_reports set status = 1 WHERE customer_id = $1', [customer.customer_id]);
                    console.log("Customer Reactivated: " + res.rows[0].customer_id)
                    //set the status message
                    statusMsg.customer_id = res.rows[0].customer_id;
                    statusMsg.status = 1; //set status to 1 for reactivated customers
                    statusMsg.date_updated = customer.datetime;
                    status.push(statusMsg);
                  }
                  
                  else if (customer.action === "dbToZohoUpdate") {
                    console.log("Updating Customer in Zoho: " + customer.customer_id);
                    statusMsg.error = customer.error;
                    //update the customer in Zoho
                    status.push(statusMsg);
                    // action already checked in dashboard api
                  }
                } catch (err) {
                  console.log("Failed to process customer: " + err.message);
                  const errJSON = {
                    "process": "databaseInsert",
                    "error": "Failed to process customer: " + err.message,
                    "data": customer
                  }
                  await putError(errJSON, 'resisure_admin', errJSON.error, 0);
                  statusMsg.status = 2;
                  statusMsg.error = err.message;
                  status.push(statusMsg);
                };
              }
              if (status.length > 0){
              await postQueue(status);
              }

            }else{

            }
            } else if (messageJSON.table === "properties") {
              let status = [];
              let statusMsg = {};
              if (messageJSON.type === "manual")
              {   console.log("Processing a " + messageJSON.type + " request for processing " + messageJSON.properties.length + " properties");
              //manual message
              for (let i in messageJSON.properties) {
                const property = messageJSON.properties[i];
                try {
                  statusMsg = {
                  "dashboard": "properties",
                  "property": property.property,
                  "action": property.action,
                  "property_id": property.property_id,
                  "customer_id": property.customer_id,
                  "property_code": property.property_code,
                  "status": property.status,
                  "area_id": property.area_id,
                  "mould_sensitivity": property.mould_sensitivity,
                  "mould_index": property.mould_index,
                  "mould_decline": property.mould_decline,
                  "flat_number": property.flat_number,
                  "street_number": property.street_number,
                  "street_name": property.street_name,
                  "town_name": property.town_name,
                  }
                if (property.action === "new") {
                    console.log("Adding property: " + property.property_code);
                    //insert the property into the database

                    let dateAdded = new Date()

                    const res = await pool.query('INSERT INTO properties (customer_id, property_code, date_added, area_id, status, mould_sensitivity, mould_index, mould_decline_coefficient) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING property_id', [property.customer_id, property.property_code, dateAdded, property.area_id, 1, property.mould_sensitivity, property.mould_index, property.mould_decline]);
                    console.log("property Inserted: " + res.rows[0].property_id)
                    const InfoRes = await pool.query ('INSERT INTO property_info (flat_number, street_number, street_name, town_name, property_id, postcode) VALUES ($1, $2, $3, $4, $5) RETURNING property_info_id', [property.flat_number, property.street_number, property.street_name, property.town_name, res.rows[0].property_id, property.postcode]);
                    console.log("Property Info Inserted: " + InfoRes.rows[0].property_info_id)
                    //set the status message
                    statusMsg.property_id = res.rows[0].property_id;
                    statusMsg.status = 1; //default status is 1 for new properties
                    statusMsg.date_added = dateAdded;
                    status.push(statusMsg);
                    console.log("Status Message Posted: " + JSON.stringify(statusMsg));

                  } else if (property.action === "amend") {
                    console.log("Updating Property: " + property.property_id);

                    //update the property in the database
                    property.status = parseInt(property.status);
                    property.customer_id = parseInt(property.customer_id);
                    let dateUpdated = new Date()
                    const res = await pool.query('UPDATE properties set customer_id = $2, property_code = $3, area_id = $4, date_updated = $5, status = $6, mould_sensitivity = $7, mould_index = $8, mould_decline_coefficient = $9 WHERE property_id = $1 RETURNING property_id', [property.property_id, property.customer_id, property.property_code, property.area_id, dateUpdated, property.status, property.mould_sensitivity, property.mould_index, property.mould_decline])
                    console.log("Property Updated: " + res.rows[0].property_id)
                    const InfoRes = await pool.query ('UPDATE property_info set flat_number = $2, street_number = $3, street_name = $4, town_name = $5 WHERE property_id = $1 RETURNING property_info_id', [property.property_id, property.flat_number, property.street_number, property.street_name, property.town_name]);

                    //set the status message
                    statusMsg.date_updated = dateUpdated;
                    statusMsg.status = 1; //set status to 1 for amended properties
                    status.push(statusMsg);
                    console.log("Status Message Posted: " + JSON.stringify(statusMsg));

                  //deactivate the property
                  } else if (property.action === "deactivate") {
                    console.log("Deactivating Property: " + property.property_id);
                    //deactivate the property in the database
                    property.property_id = parseInt(property.property_id);
                    const res = await pool.query('UPDATE properties set status = 0 WHERE property_id = $1 RETURNING property_id', [property.property_id]
                    );

                    console.log("Property Deactivated: " + res.rows[0].property_id)
                    //set the status message
                    statusMsg.status = 0; //set status to 0 for deactivated properties
                    let dateUpdated = new Date()
                    statusMsg.date_updated = dateUpdated;
                    status.push(statusMsg);
                    console.log("Status Message Posted: " + JSON.stringify(statusMsg));
                  } else if (property.action === "reactivate") {
                    console.log("Reactivating property: " + property.property_id);
                    //reactivate the property in the database
                    const res = await pool.query('UPDATE properties set status = 1 WHERE property_id = $1 RETURNING customer_id', [property.property_id]
                    );
                    console.log("Property Reactivated: " + res.rows[0].property_id)
                    //set the status message
                    statusMsg.property_id = res.rows[0].property_id;
                    statusMsg.status = 1; //set status to 1 for reactivated properties
                    statusMsg.date_updated = property.datetime;
                    status.push(statusMsg);
                  }
                  

                  else {
                    // action already checked in dashboard api
                  }
                }
                catch (err) {
                  console.log("Failed to process property: " + err.message);
                  const errJSON = {
                    "process": "databaseInsert",
                    "error": "Failed to process property: " + err.message,
                    "data": property
                  }
                  await putError(errJSON, 'resisure_admin', errJSON.error, 0);
                  statusMsg.status = 2;
                  statusMsg.error = err.message;
                  status.push(statusMsg);
                }
                }
                //post status message
                const dashboard = "properties";
                await postQueue(status, dashboard);
              }
            else {

            }
            } else if (messageJSON.table === "Areas") {
              let status = [];
              let statusMsg = {};
              if (messageJSON.type === "manual"){
                console.log("Processing a " + messageJSON.type + " request for processing " + messageJSON.areas.length + " areas");
                //manual message
                for (let i in messageJSON.areas) {
                  const area = messageJSON.areas[i];
                  try {
                    statusMsg = {
                    "dashboard": "areas",
                    "area": area.area,
                    "action": area.action,
                    "area_id": area.area_id,
                    "area_code": area.area_code,
                    "area_city": area.area_city,
                    "area_county": area.area_county,
                    }
                  if (area.action === "new") {
                      console.log("Adding area: " + area.area_code);
                      //insert the area into the database
                      let dateAdded = new Date()
                      const res = await pool.query('INSERT INTO areas (area_code, area_city, area_county, date_added) VALUES ($1, $2, $3, $4) RETURNING area_id', [area.area_code, area.area_city, area.area_county, dateAdded]);
                      console.log("area Inserted: " + res.rows[0].area_id)
                      //set the status message
                      statusMsg.date_added = dateAdded;
                      statusMsg.area_id = res.rows[0].area_id;
                      status.push(statusMsg);

                  } else if (area.action === "amend") {
                      console.log("Updating Area: " + area.area_id);
                      //update the area in the database
                      const res = await pool.query('UPDATE areas set area_code = $2, area_city = $3, area_county = $4 WHERE area_id = $1 RETURNING area_id', [area.area_id, area.area_code, area.area_city, area.area_county])
                      console.log("Area Updated: " + res.rows[0].area_id)
                      //set the status message
                      statusMsg.area_id = res.rows[0].area_id;
                      status.push(statusMsg);
                  } else {
                    // action already checked in dashboard api
                  }

                } catch (err) {
                  console.log("Failed to process Area: " + err.message);
                  const errJSON = {
                    "process": "databaseInsert",
                    "error": "Failed to process Area: " + err.message,
                    "data": area
                  }
                  await putError(errJSON, 'resisure_admin', errJSON.error, 0);
                  statusMsg.error = err.message;
                  status.push(statusMsg);
                }
                }
                //post status message
                await postQueue(status);
              }
            else {

            }

            } else {


            }
            messageChannel.ack(amqpEncodedMessage);

          } catch (err) {
          console.log('Failed to process Property: ' + err.message);
          //log error
          const errJSON = {
            "process": "connectMessageBroker",
            "error": "Failed to process Property: " + err.message,
            "data": message
          };

          //put error on the status queue
          // await postQueue({
          //   "error": err.message,
          //   "status": "error with processing Property"
          // });
          //console.log("error status Posted: " + JSON.stringify(errJSON));
          await putError(errJSON, 'resisure_admin', errJSON.error, 0);
          messageChannel.ack(amqpEncodedMessage);
        }
        console.log("Waiting for Messages");
      })();
    });
    //if the message channel connection is lost reconnect
    messageChannel.on('close', async () => {
      await connection.close();
      console.log('Lost connection to rabbitMQ');
      await connectMessageBroker(); // Reconnect
    });
  } catch (err) {
    console.log('ERROR connecting to RabbitMQ:', err.message);
    // Pause will double each time, capped to a maximum of 60 seconds
    reconnectDelay = Math.min(60000, reconnectDelay *= 2);
    await waitMs(reconnectDelay);
    await connectMessageBroker(reconnectDelay);
  }
};

//Connect to the message broker
(async () => {
  await connectMessageBroker();
})();