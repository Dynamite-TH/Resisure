//check for version request
import * as fs from 'fs';
const verJSON = JSON.parse(fs.readFileSync('version.json'));
if (process.argv.length > 2) {
  process.argv.forEach(function (val, index, array) {
    if (val == '-v') {
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
  queue: process.env.RABBITMQ_QUEUE
};
const amqp_host = `${amqp_options.protocol}://${amqp_options.username}:${amqp_options.password}@${amqp_options.host}`;
const amqp_exchange = 'amq.topic';
const publishOptions = { persistent: true }; // sets delivery_mode to 2 (message survives a broker restart)
let messageChannel;
//END AMQP
//Set the data dir for debug
const data_dir = "./data";
let file_ext = "dat";
//HTTP
import * as https from 'https';
import * as qs from 'querystring';

// These lines make "require" available
import { createRequire } from "module";
const require = createRequire(import.meta.url);
var unescapeJs = require('unescape-js');

const host_url = process.env.HOST_URL;

//Setup Postgres
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
const api_auth = process.env.AUTH_TOKEN;
const api_email = process.env.AUTH_EMAIL;

// workspace attached to process
const workspace = process.env.WORKSPACE;
//post error to poison queue
const putError = async (data = {}, service = "", error = "", error_code = 0) => {
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




const getAdminRequest = (workspace = "", dashboard = "") => {
  return new Promise((resolve) => {
    (async () => {
      //set return JSON
      let retJSON = { "result": true, "data": "", "error": "", "status": 0 };
      try {
        console.log("Fetching Customer admin Requests for Workspace: " + workspace + " Dashboard: " + dashboard);
        const post_data = qs.stringify({
          "ZOHO_ACTION": "EXPORT",
          "ZOHO_OUTPUT_FORMAT": "JSON",
          "ZOHO_ERROR_FORMAT": "JSON",
          "ZOHO_API_VERSION": "1.0",
          "authtoken": api_auth
        });
        //console.log(post_data);
        let post_options = {
          hostname: 'dashboard.resisure.co.uk',
          path: "/api/" + api_email + "/" + encodeURIComponent(workspace) + "/" + encodeURIComponent(dashboard),
          port: 443,
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        };
        console.log(post_options);

        //create HTTPS request
        let post_req = https.request(post_options, (post_res) => {
          let chunks = [];
          //process response data
          post_res.on("data", function (chunk) {
            chunks.push(chunk);
          });
          //process POST complete
          post_res.on('end', () => {
            //console.log("POST Response: " + ret);
            console.log('statusCode:', post_res.statusCode);
            //Process the return BODY data
            let body = Buffer.concat(chunks);
            body = unescapeJs(body.toString('utf8'));
            //console.log(body);
            //Set StatusCode
            retJSON.status = post_res.statusCode;
            //check statuscode
            if (post_res.statusCode == 200) {
              retJSON.result = true;
              retJSON.data = JSON.parse(body);
            } else {
              retJSON.result = false;
              retJSON.error = body;
            }
            resolve(retJSON);
          });
        });
        //process POST error
        post_req.on('error', (e) => {
          console.log(e);
          retJSON.error = e;
          retJSON.status = e.errno;
          retJSON.result = false;
          resolve(retJSON);
        });
        console.log("Sending Request");
        //Execute POST
        post_req.write(post_data);
        post_req.end();
      } catch (err) {
        //log error
        const errJSON = {
          "process": "getReports",
          "error": "Failed to Get Report Requests: " + err.message,
          "data": {}
        };
        const err_res = putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
        console.log(errJSON.error);
        retJSON.result = false;
        resolve(retJSON);
      }
    })();
  });
};

//Post a JSON message to a queue
const postQueue = (table, msgJSON) => {
  return new Promise((resolve) => {
    //put message on data insert queue
    (async () => {
      //assign AMQP constants
      const routingKey = 'resisure_admin';
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
        const date = new Date();
        let jsonData = {};
        if (table === "customers") {
          jsonData = {
            "type": "manual",
            "table": table,
            "datetime": date,
            "customers": msgJSON
          }
        }
        else if (table === "properties") {
          jsonData = {
            "type": "manual",
            "table": table,
            "datetime": date,
            "properties": msgJSON
          }
        } else if (table === "Areas") {
          jsonData = {
            "type": "manual",
            "table": table,
            "datetime": date,
            "areas": msgJSON
          }
        } else if (table === "devices") {
          jsonData = {
            "type": "manual",
            "table": table,
            "datetime": date,
            "devices": msgJSON
          }
        }
        console.log("Message to Queue: " + JSON.stringify(jsonData));
        //Queue Message
        let queueMsg = JSON.stringify(jsonData);;
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
          "process": "adminQueue",
          "error": err.message,
          "data": msgJSON
        }
        await putError(errJSON, "resisure_dashboard_api", errJSON.error, err.code);
        console.log("Failed to post msg to admin Queue: " + errJSON.error);
        ret.result = false;
        resolve(ret);
      }
    })();
  });
}



const processCustomers = () => {
  return new Promise((resolve) => {
    (async () => {
      //set return JSON
      let retJSON = { "result": false };
      try {
        //TODO Get Workspace froma CONFIG file
        const dashboard = "Customer Admin";
        //Get Report Requests to process
        let retCust = await getAdminRequest(workspace, dashboard);
        //check for customer updates
        if (retCust.status == 200) {
          const custData = retCust.data["response"]["result"];
          //check we have records to process
          if (custData.rows.length > 0) {
            //work out which column is which
            let cCustRef, cCustName, cContName, cCustEmail, cSumReport, cPropReport, cCustAction, cCustID, cStatus, cCustomer, cReportOutput;
            for (let c = 0; c < custData.column_order.length; c++) {
              switch (custData.column_order[c].toString().toLowerCase()) {
                case "customer reference":
                  cCustRef = c;
                  break;
                case "customer name":
                  cCustName = c;
                  break;
                case "contact name":
                  cContName = c;
                  break;
                case "customer email":
                  cCustEmail = c;
                  break;
                case "summary_reports":
                  cSumReport = c;
                  break;
                case "property_reports":
                  cPropReport = c;
                  break;
                case "status":
                  cStatus = c;
                  break;
                case "action":
                  cCustAction = c;
                  break;
                case "customer_id":
                  cCustID = c;
                  break;
                case "customer":
                  cCustomer = c;
                  break;
                case "report_output":
                  cReportOutput = c;
                default:
                  break;
              }
            }
            //process each record
            let messages = [];
            let cID, cRef, cName, cCName, cEmail, cSReports, cPReports, cAction, cStat, cCust, cRepOutput;

            for (let r = 0; r < custData.rows.length; r++) {

              cID = parseInt(custData.rows[r][cCustID]);
              cRef = custData.rows[r][cCustRef];
              cName = custData.rows[r][cCustName];
              cCName = custData.rows[r][cContName];
              cEmail = custData.rows[r][cCustEmail];
              cSReports = (cSumReport != null) ? (parseInt(custData.rows[r][cSumReport])) : 0;
              cPReports = (cPropReport != null) ? (parseInt(custData.rows[r][cPropReport])) : 0;
              cStat = parseInt(custData.rows[r][cStatus]);
              cAction = parseInt(custData.rows[r][cCustAction]);
              cCust = parseInt(custData.rows[r][cCustomer]);
              cRepOutput = parseInt(custData.rows[r][cReportOutput]);
              console.log(cID, cRef, cName, cCName, cEmail, cAction, cSReports, cPReports + " Customer Data");
              //check if we have a customer ID
              //if we do not have a customer ID then we need to create a new customer

              let msgJSON = {
                "customer": cCust,
                "action": cAction,
                "customer_id": cID,
                "customer_ref": cRef,
                "customer_name": cName,
                "customer_contact": cCName,
                "email": cEmail,
                "summary_reports": cSReports,
                "property_reports": cPReports,
                "status": cStat,
                "report_output": cRepOutput
              }

              //check the action
              //1 = new, 2 = amend, 3 = deactivate
              try {

                if (!cID && cAction === 1) {
                  //post the message to the queue
                  msgJSON.action = "new";
                  messages.push(msgJSON);

                } else if (cAction == 2 && cID > 0) {
                  //we have a customer ID so we need to update the customer
                  //post the message to the queue
                  msgJSON.action = "amend";

                  messages.push(msgJSON);
                  console.log("Customer Updated: " + cID);
                } else if (cAction == 3 && cID > 0) {
                  //post the message to the queue
                  msgJSON.action = "deactivate";
                  messages.push(msgJSON);
                  console.log("Customer Deactivated: " + cID);

                } else if (cAction == 4 && cID > 0) {
                  //post the message to the queue
                  msgJSON.action = "reactivate";
                  messages.push(msgJSON);
                  console.log("Customer Reactivated: " + cID);


                } else {
                  console.log("No Customer Admin Requests to Process");
                  try {
                  const dbSync = await pool.query('SELECT * FROM customers WHERE customer_id=$1', [cID]);
                  const dbCustReport = await pool.query('SELECT * FROM customer_reports WHERE customer_id=$1', [cID]);
                  const dbPropertyReports = dbCustReport.rows[0].report_structure_property_id;
                  const dbSummaryReports = dbCustReport.rows[0].summary_report;
                  const dbReportOutput = dbCustReport.rows[0].report_output;
                  const dbCustName = dbSync.rows[0].customer_name;
                  const dbCustRef = dbSync.rows[0].customer_ref;
                  const dbCustContact = dbSync.rows[0].customer_contact;
                  const dbCustEmail = dbSync.rows[0].customer_email;
                  const dbStatus = dbSync.rows[0].status;
                  console.log(dbCustName, dbCustRef, dbCustContact, dbCustEmail, dbSummaryReports, dbPropertyReports, dbStatus + " DB Customer Data");

                  //check for mismatches

                  if (dbSync){
                    let dataMismatch = [];
                    if (dbCustName != cName) {
                      console.log("Customer Name Mismatch for ID " + cID + " Dashboard: " + cName + " DB: " + dbCustName);
                      msgJSON.customer_name = dbCustName;
                      msgJSON.action = "dbToZohoUpdate";
                      dataMismatch.push("Customer Name");
                    }
                    if (dbCustRef != cRef) {
                      console.log("Customer Ref Mismatch for ID " + cID + " Dashboard: " + cRef + " DB: " + dbCustRef);
                      msgJSON.customer_ref = dbCustRef;
                      msgJSON.action = "dbToZohoUpdate";
                      dataMismatch.push("Customer Ref");
                      
                    }
                    if (dbCustContact != cCName) {
                      console.log("Customer Contact Mismatch for ID " + cID + " Dashboard: " + cCName + " DB: " + dbCustContact);
                      msgJSON.customer_contact = dbCustContact;
                      msgJSON.action = "dbToZohoUpdate"; 
                      dataMismatch.push("Customer Contact");
                    }
                    if (dbCustEmail != cEmail) {
                      console.log("Customer Email Mismatch for ID " + cID + " Dashboard: " + cEmail + " DB: " + dbCustEmail);
                      msgJSON.email = dbCustEmail;
                      msgJSON.action = "dbToZohoUpdate";
                      dataMismatch.push("Customer Email");
                      
                    }
                    if (dbSummaryReports != cSReports) {
                      console.log("Customer Summary Reports Mismatch for ID " + cID + " Dashboard: " + cSReports + " DB: " + dbSummaryReports);
                      msgJSON.summary_reports = dbSummaryReports;
                      msgJSON.action = "dbToZohoUpdate";
                      dataMismatch.push("Summary Reports");
                      
                    }
                    if (dbPropertyReports != cPReports) {
                      console.log("Customer Property Reports Mismatch for ID " + cID + " Dashboard: " + cPReports + " DB: " + dbPropertyReports);
                      msgJSON.property_reports = dbPropertyReports;
                      msgJSON.action = "dbToZohoUpdate";
                      dataMismatch.push("Property Reports");
                      
                    }
                    if (dbStatus != cStat) {
                      console.log("Customer Status Mismatch for ID " + cID + " Dashboard: " + cStat + " DB: " + dbStatus);
                      msgJSON.status = dbStatus;
                      msgJSON.action = "dbToZohoUpdate";
                      dataMismatch.push("Status");
                    }
                    if (dbReportOutput != cRepOutput) {
                      console.log("Customer Report Output Mismatch for ID " + cID + " Dashboard: " + cRepOutput + " DB: " + dbReportOutput);
                      msgJSON.report_output = dbReportOutput;
                      msgJSON.action = "dbToZohoUpdate";
                      dataMismatch.push("Report Output");
                    }
                    if (msgJSON.action == "dbToZohoUpdate") {
                      msgJSON.error = "Data Discrepency, data overwritten for the following fields: " + dataMismatch.join(", ");
                      messages.push(msgJSON);
                    }
                    else{
                      console.log("No Data Mismatches for Customer ID: " + cID);
                    }
                  //post the message to the queue
                  
                } else {
                  throw new Error("No Customer found in DB for ID: " + cID);
                }
                } catch (err) {
                  console.log("Failed to sync customer data: " + err.message);
                  const errJSON = {
                    "process": "processCustomers",
                    "error": "Failed to sync customer data: " + err.message,
                    "data": msgJSON
                  }
                  const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
                  console.log(errJSON.error);
                }




              }
            } catch (err) {
                //log error
                const errJSON = {
                  "process": "processCustomers",
                  "error": "Failed to process Customer Admin Request: " + err.message,
                  "data": msgJSON
                }
                const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
                console.log(errJSON.error);
              }
            
            if (messages.length > 0) {
              //console.log("Messages to Queue: " + JSON.stringify(messages));
              const table = "customers";
              let custData = await postQueue(table, messages);
              //console.log("Messages Posted to Queue: " + JSON.stringify(messages))
              //console.log(messages);
              if (custData.result) {
                console.log("Messages Posted to Queue");
                retJSON.result = true;
              } else {
                console.log("Failed to post messages to queue");
                retJSON.result = false;
              }
            } else {
              //No messages to queue
            }
          }} else {
            //Failed to get report requests
            //Error logged in getReports
            retJSON.result = false;
          }
        }
        resolve(retJSON);
      } catch (err) {
        //log error
        const errJSON = {
          "process": "processCustomers",
          "error": "Failed to process Customers: " + err.message,
          "data": {}
        }
        const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
        console.log(errJSON.error);
        resolve(retJSON);
      }
    })();
  });
};

const processProperties = () => {
  return new Promise((resolve) => {
    (async () => {
      //set return JSON
      let retJSON = { "result": false };
      try {
        //TODO Get Workspace froma CONFIG file
        const dashboard = "Property_admin_view";
        //Get Report Requests to process
        let retProp = await getAdminRequest(workspace, dashboard);
        //check for customer updates
        if (retProp.status == 200) {
          const PropData = retProp.data["response"]["result"];
          //check we have records to process
          if (PropData.rows.length > 0) {
            //work out which column is which
            let pPropID, pCustID, pPropCode, pStatus, pAreaID, pPropertyAction, pProperty, pTown, pStreet, pStreetNum, pFlatNum, pCounty, pCity;
            for (let c = 0; c < PropData.column_order.length; c++) {
              switch (PropData.column_order[c].toString().toLowerCase()) {
                case "property id":
                  pPropID = c;
                  break;
                case "customer_id":
                  pCustID = c;
                  break;
                case "property code":
                  pPropCode = c;
                  break;
                case "status":
                  pStatus = c;
                  break;
                case "area_id":
                  pAreaID = c;
                  break;
                case "action":
                  pPropertyAction = c;
                  break;
                case "property":
                  pProperty = c;
                  break;
                case "town":
                  pTown = c;
                  break;
                case "street":
                  pStreet = c;
                  break;
                case "street number":
                  pStreetNum = c;
                  break;
                case "flat number":
                  pFlatNum = c;
                  break;
                case "area_county":
                  pCounty = c;
                  break;
                case "city":
                  pCity = c;
                  break;
                default:
                  break;
              }
            }
            //process each record
            let messages = [];
            let pID, pCID, pCode, pStat, pAreID, pAction, pProp, pTownName, pStreetName, pStreetNumVal, pFlatNumVal, pCount, pCityName;

            for (let r = 0; r < PropData.rows.length; r++) {

              pID = parseInt(PropData.rows[r][pPropID]);
              pCID = parseInt(PropData.rows[r][pCustID]);
              pCode = PropData.rows[r][pPropCode];
              pStat = parseInt(PropData.rows[r][pStatus]);
              pAreID = parseInt(PropData.rows[r][pAreaID]);
              pAction = parseInt(PropData.rows[r][pPropertyAction]);
              pProp = parseInt(PropData.rows[r][pProperty]);
              pTownName = PropData.rows[r][pTown];
              pStreetName = PropData.rows[r][pStreet];
              pStreetNumVal = PropData.rows[r][pStreetNum];
              pFlatNumVal = PropData.rows[r][pFlatNum];
              pCount = PropData.rows[r][pCounty];
              pCityName = PropData.rows[r][pCity];

              console.log(pProp, pID, pCID, pCode, pStat, pAreID, pAction + " Property Data");
              //check if we have a customer ID

              let msgJSON = {
                "property": pProp,
                "action": pAction,
                "property_id": pID,
                "customer_id": pCID,
                "property_code": pCode,
                "status": pStat,
                "area_id": pAreID,
                "town": pTownName,
                "street": pStreetName,
                "street_number": pStreetNumVal,
                "flat_number": pFlatNumVal,
                "county": pCount,
                "city": pCityName
              }

              //check the action
              //1 = new, 2 = amend, 3 = deactivate
              try {
                if (pCID) {
                  if (!pID && pAction === 1) {
                    //we do not have a property ID so we can create a new property
                    console.log("Creating New Property: " + pCode);
                    //post the message to the queue
                    msgJSON.action = "new";
                    messages.push(msgJSON);

                  } else if (pAction == 2 && pID > 0) {
                    //we have a property ID so we can update the property
                    msgJSON.date_updated = new Date();
                    console.log("Updating Property: " + pID);
                    //post the message to the queue
                    msgJSON.action = "amend";
                    messages.push(msgJSON);

                  } else if (pAction == 3 && pID) {
                    //we have a property ID so we can deactivate the property
                    console.log("Deactivating Property: " + pID);
                    //post the message to the queue
                    msgJSON.action = "deactivate";
                    messages.push(msgJSON);
                  } else if (pAction == 4 && pID) {
                    //we have a property ID so we can reactivate the property
                    console.log("Reactivating Property: " + pID);
                    //post the message to the queue
                    msgJSON.action = "reactivate";
                    messages.push(msgJSON);
                  }
                } else if (!pCID) {
                  console.log("Customer ID not found for Property: " + pID);
                  throw new Error("Customer ID not found for Property: ");
                


                } else {  
                  console.log("No Property Admin Requests to Process");
                  try {
                    const dbSync = await pool.query('SELECT * FROM properties WHERE property_id=$1', [pID]);
                    const dbPropCode = dbSync.rows[0].property_code;
                    const dbCustID = dbSync.rows[0].customer_id;
                    const dbAreaID = dbSync.rows[0].area_id;
                    const dbStatus = dbSync.rows[0].status;
                    console.log(dbPropCode, dbCustID, dbAreaID, dbStatus + " DB Property Data");
                    //check for mismatches
                    if (dbSync) {
                      let dataMismatch = ""
                      if (dbPropCode != pCode) {
                        console.log("Property Code Mismatch for ID " + pID + " Dashboard: " + pCode + " DB: " + dbPropCode);
                        msgJSON.property_code = dbPropCode;
                        msgJSON.action = "dbToZohoUpdate";
                        dataMismatch = dataMismatch + " Property Code, ";
                      }
                      if (dbCustID != pCID) {
                        console.log("Customer ID Mismatch for Property ID " + pID + " Dashboard: " + pCID + " DB: " + dbCustID);
                        msgJSON.customer_id = dbCustID;
                        msgJSON.action = "dbToZohoUpdate";
                        dataMismatch = dataMismatch + " Customer ID, ";
                      }
                      if (dbAreaID != pAreID) {
                        console.log("Area ID Mismatch for Property ID " + pID + " Dashboard: " + pAreID + " DB: " + dbAreaID);
                        msgJSON.area_id = dbAreaID;
                        msgJSON.action = "dbToZohoUpdate";
                        dataMismatch = dataMismatch + " Area ID, ";
                      }
                      if (dbStatus != pStat) {
                        console.log("Property Status Mismatch for ID " + pID + " Dashboard: " + pStat + " DB: " + dbStatus);
                        msgJSON.status = dbStatus;
                        msgJSON.action = "dbToZohoUpdate";
                        dataMismatch = dataMismatch + " Status, ";
                      }
                      if (msgJSON.action == "dbToZohoUpdate") {
                        msgJSON.error = "Data Discrepency, data overwritten for the following fields: " + dataMismatch.slice(0, -2);
                        messages.push(msgJSON);
                      }
                        //post the message to the queue

                      } else {
                        //no property found
                        console.log("No Property found in DB for ID: " + pID);
                      }
                    } catch (err) {
                      console.log("Failed to sync property data: " + err.message);
                      const errJSON = {
                        "process": "processProperties",
                        "error": "Failed to sync property data: " + err.message,
                        "data": msgJSON
                      }
                      const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
                      console.log(errJSON.error);
                  }
                }



              } catch (err) {
                //log error
                const errJSON = {
                  "process": "processProperties",
                  "error": "Failed to process Customer Admin Request: " + err.message,
                  "data": msgJSON
                }
                const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
                console.log(errJSON.error);
              }
            }
            if (messages.length > 0) {
              //console.log("Messages to Queue: " + JSON.stringify(messages));
              const table = "properties";
              let repdata = await postQueue(table, messages);
              //console.log(messages);
              if (repdata.result) {
                console.log("Messages Posted to Queue");
                retJSON.result = true;
              } else {
                console.log("Failed to post messages to queue");
                retJSON.result = false;
              }
            } else {
              //No messages to queue
            }
          } else {
            //Failed to get report requests
            //Error logged in getReports
            retJSON.result = false;
          }
        }
        resolve(retJSON);
      } catch (err) {
        //log error
        const errJSON = {
          "process": "processCustomers",
          "error": "Failed to process Customers: " + err.message,
          "data": {}
        }
        const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
        console.log(errJSON.error);
        resolve(retJSON);
      }
    })();
  });
};

const processDevices = () => {
  return new Promise((resolve) => {
    (async () => {
      //set return JSON
      let retJSON = { "result": false };
      try {
        //TODO Get Workspace froma CONFIG file
        const dashboard = "device_admin_view";
        //Get Report Requests to process
        let retDev = await getAdminRequest(workspace, dashboard);
        //check for customer updates
        if (retDev.status == 200) {
          const DevData = retDev.data["response"]["result"];
          //check we have records to process
          if (DevData.rows.length > 0) {
            //work out which column is which
            let dPropID, dDevID, dAction, dDevice;
            for (let c = 0; c < DevData.column_order.length; c++) {
              switch (DevData.column_order[c].toString().toLowerCase()) {
                case "property_id":
                  dPropID = c;
                  break;
                case "device_id":
                  dDevID = c;
                  break;
                case "action_code":
                  dAction = c;
                  break;
                case "device":
                  dDevice = c;
                  break;
                default:
                  break;
              }
            }
            //process each record
            let messages = [];
            let dPropertyID, dDeviceID, dActionType, dDeviceType;

            for (let r = 0; r < DevData.rows.length; r++) {

              dPropertyID = parseInt(DevData.rows[r][dPropID]);
              dDeviceID = parseInt(DevData.rows[r][dDevID]);
              dActionType = parseInt(DevData.rows[r][dAction]);
              dDeviceType = parseInt(DevData.rows[r][dDevice]);
              console.log(dPropertyID, dDeviceID, dActionType + " Device Data");
              //check if we have a customer ID

              let msgJSON = {
                "device": dDeviceType,
                "action": dActionType,
                "device_id": dDeviceID,
                "property_id": dPropertyID
              }

              //check the action
              //1 = new, 2 = amend, 3 = deactivate
              try {
                if (dPropertyID) {
                  if (dActionType === 1) {
                    //we do not have a property ID so we can create a new property
                    console.log("Editing Device: " + dDeviceID);
                    //post the message to the queue
                    msgJSON.action = "Edit";
                    messages.push(msgJSON);
                  } 
                } else {  
                  console.log("No Device Admin Requests to Process");
                  try {
                  //   const dbSync = await pool.query('SELECT * FROM properties WHERE property_id=$1', [pID]);
                  //   const dbPropCode = dbSync.rows[0].property_code;
                  //   const dbCustID = dbSync.rows[0].customer_id;
                  //   const dbAreaID = dbSync.rows[0].area_id;
                  //   const dbStatus = dbSync.rows[0].status;
                  //   console.log(dbPropCode, dbCustID, dbAreaID, dbStatus + " DB Property Data");
                  //   //check for mismatches
                  //   if (dbSync) {
                  //     let dataMismatch = ""
                  //     if (dbPropCode != pCode) {
                  //       console.log("Property Code Mismatch for ID " + pID + " Dashboard: " + pCode + " DB: " + dbPropCode);
                  //       msgJSON.property_code = dbPropCode;
                  //       msgJSON.action = "dbToZohoUpdate";
                  //       dataMismatch = dataMismatch + " Property Code, ";
                  //     }
                  //     if (dbCustID != pCID) {
                  //       console.log("Customer ID Mismatch for Property ID " + pID + " Dashboard: " + pCID + " DB: " + dbCustID);
                  //       msgJSON.customer_id = dbCustID;
                  //       msgJSON.action = "dbToZohoUpdate";
                  //       dataMismatch = dataMismatch + " Customer ID, ";
                  //     }
                  //     if (dbAreaID != pAreID) {
                  //       console.log("Area ID Mismatch for Property ID " + pID + " Dashboard: " + pAreID + " DB: " + dbAreaID);
                  //       msgJSON.area_id = dbAreaID;
                  //       msgJSON.action = "dbToZohoUpdate";
                  //       dataMismatch = dataMismatch + " Area ID, ";
                  //     }
                  //     if (dbStatus != pStat) {
                  //       console.log("Property Status Mismatch for ID " + pID + " Dashboard: " + pStat + " DB: " + dbStatus);
                  //       msgJSON.status = dbStatus;
                  //       msgJSON.action = "dbToZohoUpdate";
                  //       dataMismatch = dataMismatch + " Status, ";
                  //     }
                  //     if (msgJSON.action == "dbToZohoUpdate") {
                  //       msgJSON.error = "Data Discrepency, data overwritten for the following fields: " + dataMismatch.slice(0, -2);
                  //       messages.push(msgJSON);
                  //     }
                  //       //post the message to the queue

                  //     } else {
                  //       //no property found
                  //       console.log("No Property found in DB for ID: " + pID);
                  //     }
                    } catch (err) {
                      console.log("Failed to sync property data: " + err.message);
                      const errJSON = {
                        "process": "processProperties",
                        "error": "Failed to sync property data: " + err.message,
                        "data": msgJSON
                      }
                      const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
                      console.log(errJSON.error);
                  }
                }



              } catch (err) {
                //log error
                const errJSON = {
                  "process": "processProperties",
                  "error": "Failed to process Customer Admin Request: " + err.message,
                  "data": msgJSON
                }
                const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
                console.log(errJSON.error);
              }
            }
            if (messages.length > 0) {
              //console.log("Messages to Queue: " + JSON.stringify(messages));
              const table = "devices";
              let repdata = await postQueue(table, messages);
              //console.log(messages);
              if (repdata.result) {
                console.log("Messages Posted to Queue");
                retJSON.result = true;
              } else {
                console.log("Failed to post messages to queue");
                retJSON.result = false;
              }
            } else {
              //No messages to queue
            }
          } else {
            //Failed to get report requests
            //Error logged in getReports
            retJSON.result = false;
          }
        }
        resolve(retJSON);
      } catch (err) {
        //log error
        const errJSON = {
          "process": "processCustomers",
          "error": "Failed to process Customers: " + err.message,
          "data": {}
        }
        const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
        console.log(errJSON.error);
        resolve(retJSON);
      }
    })();
  });
};


const processAreas = () => {
  return new Promise((resolve) => {
    (async () => {
      //set return JSON
      let retJSON = { "result": false };
      try {
        //TODO Get Workspace froma CONFIG file
        const dashboard = "Area Admin";
        //Get Report Requests to process
        let retArea = await getAdminRequest(workspace, dashboard);
        //check for customer updates
        if (retArea.status == 200) {
          const AreaData = retArea.data["response"]["result"];
          //check we have records to process
          if (AreaData.rows.length > 0) {
            //work out which column is which
            let areaCode, areaCity, areaCounty, areaAction, Area, areaID;
            for (let c = 0; c < AreaData.column_order.length; c++) {
              switch (AreaData.column_order[c].toString().toLowerCase()) {
                case "area id":
                  areaID = c;
                  break;
                case "area code":
                  areaCode = c;
                  break;
                case "area city":
                  areaCity = c;
                  break;
                case "area county":
                  areaCounty = c;
                  break;
                case "action":
                  areaAction = c;
                  break;
                case "area":
                  Area = c;
                  break;
                default:
                  break;
              }
            }
            //process each record
            let messages = [];
            let aCode, aCity, aCounty, aAction, aArea, aID;

            for (let r = 0; r < AreaData.rows.length; r++) {

              aID = parseInt(AreaData.rows[r][areaID]);
              aCode = AreaData.rows[r][areaCode];
              aCity = AreaData.rows[r][areaCity];
              aCounty = AreaData.rows[r][areaCounty];
              aAction = parseInt(AreaData.rows[r][areaAction]);
              aArea = parseInt(AreaData.rows[r][Area]);
              console.log(aCode, aCity, aCounty, aAction, aArea, aID + " Area Data");
              //check if we have a customer ID

              let msgJSON = {
                "area": aArea,
                "action": aAction,
                "area_id": aID,
                "area_code": aCode,
                "area_city": aCity,
                "area_county": aCounty
              }

              //check the action
              //1 = new, 2 = amend, 3 = deactivate
              try {
                if (!aID && aAction === 1) {
                  //we do not have a property ID so we can create a new property
                  console.log("Creating New Area: " + aCode);
                  //post the message to the queue
                  msgJSON.action = "new";
                  messages.push(msgJSON);

                } else if (aAction == 2 && aID > 0) {
                  //we have a property ID so we can update the property
                  msgJSON.date_updated = new Date();
                  console.log("Updating Area: " + aID);
                  //post the message to the queue
                  msgJSON.action = "amend";
                  messages.push(msgJSON);


                } else {
                  console.log("No Area Admin Requests to Process");
                }



              } catch (err) {
                //log error
                const errJSON = {
                  "process": "processAreas",
                  "error": "Failed to process Area Admin Request: " + err.message,
                  "data": msgJSON
                }
                const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
                console.log(errJSON.error);
              }
            }
            if (messages.length > 0) {
              //console.log("Messages to Queue: " + JSON.stringify(messages));
              const table = "Areas";
              let repdata = await postQueue(table, messages);
              //console.log(messages);
              if (repdata.result) {
                console.log("Messages Posted to Queue");
                retJSON.result = true;
              } else {
                console.log("Failed to post messages to queue");
                retJSON.result = false;
              }
            } else {
              //No messages to queue
            }
          } else {
            //Failed to get report requests
            //Error logged in getReports
            retJSON.result = false;
          }
        }
        resolve(retJSON);
      } catch (err) {
        //log error
        const errJSON = {
          "process": "processAreas",
          "error": "Failed to process Areas: " + err.message,
          "data": {}
        }
        const err_res = await putError(errJSON, 'resisure_dashboard_api', errJSON.error, 0);
        console.log(errJSON.error);
        resolve(retJSON);
      }
    })();
  });
};


//Connect to AMQP message broker and process messages
const connectMessageBroker = async (reconnectDelay = 500) => {
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
    console.log(`Queue '${queue}' asserted. Waiting message count: ${queueDetails.messageCount}`);
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
          switch (messageJSON.type.toString().toLowerCase()) {
            case "scheduled":
              //process customer admin requests
              const custRep = await processCustomers();
              if (custRep.result) {
                console.log("Customers Processed Successfully");
              } else {

              }
              const areaRep = await processAreas();
              if (areaRep.result) {
                console.log("Areas Processed Successfully");
              } else {

              }

              await waitMs(2000); //wait 2 seconds before processing properties to allow for customer and area creation if needed
              //process properties admin requests
              const propRep = await processProperties();
              if (propRep.result) {
                console.log("Properties Processed Successfully");
              } else {

              }
              await waitMs(2000); //wait 2 seconds before processing devices to allow for property creation if needed
              //process device admin requests
              const devRep = await processDevices();
              if (devRep.result) {
                console.log("Devices Processed Successfully");
              } else {

              }


              break;
            default:
              break;
          }
          console.log("Waiting for next message");
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
// initialisation
(async () => {
  await connectMessageBroker();
})();