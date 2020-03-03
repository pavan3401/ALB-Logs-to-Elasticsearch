/*
 * This project based on https://github.com/awslabs/amazon-elasticsearch-lambda-samples,https://github.com/blmr/aws-elb-logs-to-elasticsearch.git
 * Function for AWS Lambda to get AWS ELB log files from S3, parse
 * and add them to an Amazon Elasticsearch Service domain.
 *
 *
 * Copyright 2015- Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at http://aws.amazon.com/asl/
 * or in the "license" file accompanying this file.  This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

/* Imports */
const AWS = require('aws-sdk');
const LineStream = require('byline').LineStream;
const stream = require('stream');
const zlib = require('zlib');
const ES = require('elasticsearch');
const geoip = require('geoip-lite');

/* Geo IP Parsing */
const geoip_enabled = process.env.GEOIP_LOOKUP_ENABLED === "true";
if (geoip_enabled) {
    console.log("GeoIP parsing is enabled!")
}
else {
    console.log("GeoIP parsing is NOT enabled. Set GEOIP_LOOKUP_ENABLED to true to enable.")
}

/* Globals */
let indexTimestamp;
let esDomain;
let elasticsearch;
let s3 = new AWS.S3();

// Bulk indexing and stats
let totalIndexedLines = 0;
let totalStreamedLines = 0;
let bulkBuffer = [];
let bulkTransactions = 0;

// ES configs
const esTimeout = 100000;
// var esMaxSockets = 20;

/* Lambda "main": Execution starts here */
exports.handler = function(event, context) {

    // Set indexTimestamp and esDomain index fresh on each run
    indexTimestamp = new Date().toISOString().replace(/\-/g, '.').replace(/T.+/, '');

    // Compose index name; add a timestamp to index. Example: alblogs-2015.03.31
    const indexName = process.env.ES_INDEX_PREFIX + '-' + indexTimestamp;

    esDomain = {
        endpoint: process.env.ES_ENDPOINT,
        index: indexName,
        doctype: process.env.ES_DOCTYPE,
        extraFields: JSON.parse(process.env.ES_EXTRA_FIELDS || '{}'),
        maxBulkIndexLines: process.env.ES_BULKSIZE // Max Number of log lines to send per bulk interaction with ES
    };

    /**
     * Get connected to Elasticsearch using the official
     * elasticsearch.js client and the http-aws-es Connection
     * Class for signing requests using IAM creds.
     *
     * The AWS credentials are picked up from the environment.
     * They belong to the IAM role assigned to the Lambda function.
     * Since the ES requests are signed using these credentials,
     * make sure to apply a policy that permits ES domain operations
     * to the role.
     */
    elasticsearch = new ES.Client({
        host: esDomain.endpoint,
        connectionClass: require('http-aws-es'),
        log: 'error',
        requestTimeout: esTimeout,
        // maxSockets: esMaxSockets
    });

    // Ensure Elasticsearch index has been configured.
    // We only need to force a datatype on the log field.
    prepareESIndexAsync(indexName, elasticsearch).then(() => {

        // Prepare bulk buffer
        initBulkBuffer();

        /* == Streams ==
         * To avoid loading an entire (typically large) log file into memory,
         * this is implemented as a pipeline of filters, streaming log data
         * from S3 to ES.
         * Flow: S3 file stream -> Log Line stream -> Log Record stream -> Lambda buffer -> ES Bulk API
         */

        let recordStream = new stream.Transform({
            objectMode: true
        });

        recordStream._transform = function(line, encoding, done) {

            // Add standard fields to help with searching
            let logRecord = {
                ...parse(line.toString()),
                ...esDomain.extraFields
            };

            // Handle GeoIP parsing if enabled
            if (geoip_enabled) {

                const geo = geoip.lookup(logRecord['client']);

                // Lat/Lon
                logRecord['location'] = {
                    'lat': geo['ll'][0],
                    'lon': geo['ll'][1]
                };

                // GeoIP's own city guesses. Don't keep everything.
                logRecord['geo'] = {
                    'country': geo['country'],
                    'region': geo['region'],
                    'city': geo['city'],
                    'timezone': geo['timezone']
                }
            }

            this.push(JSON.stringify(logRecord)); // what's "this" here?
            totalStreamedLines++;
            done();

        };

        return recordStream;

    }).then(recordStream => {

        let lineStream = new LineStream();
        event.Records.forEach(record => {
            const bucket = record.s3.bucket.name;
            const objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
            s3LogsToES(bucket, objKey, context, lineStream, recordStream);
        });

    }).catch(e => {
        console.log("Unexpected error:");
        console.log(e)
    });

};

/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 *
 * Note: The Lambda function should be configured to filter for 
 * .log.gz files (as part of the Event Source "suffix" setting).
 */
function s3LogsToES(bucket, key, context, lineStream, recordStream) {

    var s3Stream = s3.getObject({
        Bucket: bucket,
        Key: key
    }).createReadStream();

    var gunzipStream = zlib.createGunzip();

    s3Stream
        .pipe(gunzipStream)
        .pipe(lineStream)
        .pipe(recordStream)
        .on('data', function(parsedEntry) {

            // Add this log entry to the buffer
            addToBulkBuffer(parsedEntry);

            // See if it's time to flush and proceed
            checkFlushBuffer();
        })
        .on('error', function() {
            console.log(
                'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
                'Make sure they exist and your bucket is in the same region as this function.');
            context.fail();
        })
        .on('finish', function() {
            flushBuffer();
            console.log("Process complete. "+totalIndexedLines+" out of "+totalStreamedLines+" added in "+bulkTransactions+" transactions.");
            context.succeed();
        })
}


function prepareESIndexAsync(index, client) {

  return new Promise((resolve, reject) => {

    return client.indices.exists({index}).then(exists => {

      if(!exists) {
        console.log(`Index ${index} doesn't exist, creating...`);
        return client.indices.create({index});
      } else {
        console.log(`Index ${index} exists.`);
        return exists;
      }

    }).then(() => {

      console.log(`Putting the GeoIP mapping into index ${index}`);
      return client.indices.putMapping({
        index,
        type: process.env.ES_DOCTYPE,
        body: {
          properties: {
            location: {
              type: "geo_point"
            }
          }
        }
      });

    }).then(() => {

      resolve() // work is done

    }).catch(e => {
      console.log("Failure preparing ES Index:");
      console.log(e);
      reject(e);
    })

  });

}

/*
 * ES Index Functions
 */
async function prepareESIndex(indexName, client) {

    var location_mapping = {
        "properties": {
            "location": {
                "type": "geo_point"
            }
        }
    }

    console.log("Does the index exist?");
    try {
        var index_exists = await client.indices.exists({ index: indexName });
        console.log(index_exists);
    }
    catch (e) {
        console.log("Couldn't even check.")
        console.log(e)
        throw e;
    }


    if (index_exists) {
        console.log("Index " + indexName + " exists. Putting the GeoIP mapping to be sure it's there.");
        await client.indices.putMapping({
            index: indexName,
            type: process.env.ES_DOCTYPE,
            body: location_mapping
        });
        console.log("Ready to log!")
    }
    else {
        console.log("Index " + indexName + " does NOT exist. Creating it.")
        await client.indices.create({
            index: indexName
        });
        console.log("Also putting mapping for location field");
        await client.indices.putMapping({
            index: indexName,
            type: process.env.ES_DOCTYPE,
            body: location_mapping
        });
        console.log("Ready to log!")
    }
}

/*
 * Bulk Buffering Functions
 */
function initBulkBuffer() {
    bulkBuffer = [];
}

function addToBulkBuffer(doc) {
    bulkBuffer.push(doc);
}

function checkFlushBuffer() {
    if (bulkBuffer.length >= esDomain.maxBulkIndexLines) {
        flushBuffer();
    }
}

function flushBuffer() {
    // Map the raw lines into an ES bulk transaction body
    bulkBody = convertBufferToBulkBody(bulkBuffer);

    // Submit to ES
    postBulkDocumentsToES(bulkBody);

    // Keep stats
    numLines = bulkBody.length / 2;
    totalIndexedLines += numLines;
    bulkTransactions++;

    // Clear the buffer
    initBulkBuffer();
}

function convertBufferToBulkBody(buffer) {

    bulkBody = [];

    for (var i in buffer) {
        logEntry = buffer[i];

        bulkBody.push({ index: { _index: esDomain.index, _type: esDomain.doctype } });
        bulkBody.push(logEntry);
    }

    return bulkBody;
}

function postBulkDocumentsToES(bulkBody) {
    elasticsearch.bulk({ body: bulkBody, timeout: "200s" }, {requestTimeout: 200000, maxRetries: 5});
}

/**
 * Line Parser.
 * It would have been much easier with Logstash Grok...
 */
function parse(line) {

    var url = require('url');

    // Fields in log lines are essentially space separated,
    // but are also quote-enclosed for strings containing spaces.
    var field_names = [
        'type',
        esDomain.timestampFieldName,
        'elb',
        'client',
        'target',
        'request_processing_time',
        'target_processing_time',
        'response_processing_time',
        'elb_status_code',
        'target_status_code',
        'received_bytes',
        'sent_bytes',
        'request',
        'user_agent',
        'ssl_cipher',
        'ssl_protocol',
        'target_group_arn',
        'trace_id',
        'domain_name',
        'chosen_cert_arn',
        'waf_number',
        'waf_time',
        'waf_message'
    ];

    // First phase, separate out the fields
    within_quotes = false;
    current_field = 0;
    current_value = '';
    current_numeric = NaN;

    var parsed = {};

    // Remove trailing newline
    if (line.match(/\n$/)) {
        line = line.slice(0, line.length - 1);
    }

    // Character by character
    for (var i in line) {

        c = line[i];

        if (!within_quotes) {
            if (c == '"') {
                // Beginning a quoted field.
                within_quotes = true;
            } else if (c == " ") {
                // Separator. Moving on to the next field.

                // Convert to numeric type if appropriate.
                // This is needed to make sure Elasticsearch gets the
                // dynamic mapping correct.
                current_numeric = Number(current_value)
                if (!isNaN(current_numeric)) {
                    current_value = current_numeric;
                }

                // Save current and reset.
                parsed[field_names[current_field]] = current_value;
                current_field++;
                current_value = '';

            } else {
                // Part of this field.
                current_value += c;
            }
        } else {
            if (c == '"') {
                // Ending a quoted field.
                within_quotes = false;
            } else {
                // Part of this quoted field.
                current_value += c;
            }
        }
    }

    // Save off the last one
    parsed[field_names[current_field]] = current_value;

    // Second phase, cleanups.

    // Breaking out the port for the client and target, if there's a colon present
    colon_sep = ['client', 'target']
    for (var i in colon_sep) {
        var orig = parsed[colon_sep[i]];

        if (orig.indexOf(":") > 0) {
            splat = orig.split(":");
            parsed[colon_sep[i]] = splat[0]
            parsed[colon_sep[i] + "_port"] = Number(splat[1])
        }
    }

    // Dropping the target status code if there isn't one
    if (parsed['target_status_code'] == '-') {
        delete parsed['target_status_code']
    }
    if (parsed['undefined']) delete parsed['undefined']

    // Third phase, parsing out the request into more fields
    // Only do this if there's actually data in that field
    if (parsed['request'].trim() != '- - -') {

        splat = parsed['request'].split(" ");

        // Basic values
        parsed['request_method'] = splat[0]
        parsed['request_uri'] = splat[1]
        parsed['request_http_version'] = splat[2]

        // If we can parse the URL, we can populate other fields properly
        try {
            uri = url.parse(splat[1]);

            parsed['request_uri_scheme'] = uri.protocol ? uri.protocol : '';
            parsed['request_uri_host'] = uri.hostname ? uri.hostname : '';
            parsed['request_uri_port'] = uri.port ? uri.port : '';
            parsed['request_uri_path'] = uri.pathname ? uri.pathname : '';
            parsed['request_uri_query'] = uri.query ? uri.query : '';
        }

        // Otherwise, we just leave them out.
        catch (e) {}
    }

    // All done.
    return parsed;
}
