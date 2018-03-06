/*
 * This project based on https://github.com/awslabs/amazon-elasticsearch-lambda-samples,https://github.com/blmr/aws-elb-logs-to-elasticsearch.git
 * Sample code for AWS Lambda to get AWS ELB log files from S3, parse
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
var AWS = require('aws-sdk');
var LineStream = require('byline').LineStream;
var path = require('path');
var stream = require('stream');
var indexTimestamp = new Date().toISOString().replace(/\-/g, '.').replace(/T.+/, '');
var zlib     = require('zlib');
/* Globals */
var esDomain = {
    endpoint: process.env.ES_ENDPOINT,
    region: process.env.ES_REGION,
    index: process.env.ES_INDEX_PREFIX + '-' + indexTimestamp, // adds a timestamp to index. Example: alblogs-2015.03.31
    doctype: process.env.ES_DOCTYPE
};
var endpoint =  new AWS.Endpoint(esDomain.endpoint);
var s3 = new AWS.S3();
var totLogLines = 0;    // Total number of log lines in the file
var numDocsAdded = 0;   // Number of log lines added to ES so far
/*
 * The AWS credentials are picked up from the environment.
 * They belong to the IAM role assigned to the Lambda function.
 * Since the ES requests are signed using these credentials,
 * make sure to apply a policy that permits ES domain operations
 * to the role.
 */
var creds = new AWS.EnvironmentCredentials('AWS');
/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 */
function s3LogsToES(bucket, key, context, lineStream, recordStream) {
    // Note: The Lambda function should be configured to filter for .log.gz files
    // (as part of the Event Source "suffix" setting).
   
    var s3Stream = s3.getObject({Bucket: bucket, Key: key}).createReadStream();
    var gunzipStream = zlib.createGunzip();
    // Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
    s3Stream
        .pipe(gunzipStream)
        .pipe(lineStream)
        .pipe(recordStream)
        .on('data', function(parsedEntry) {
            postDocumentToES(parsedEntry, context);
        });
    s3Stream.on('error', function() {
        console.log(
            'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
            'Make sure they exist and your bucket is in the same region as this function.');
        context.fail();
    });
}
/*
 * Add the given document to the ES domain.
 * If all records are successfully added, indicate success to lambda
 * (using the "context" parameter).
 */
function postDocumentToES(doc, context) {
    var req = new AWS.HttpRequest(endpoint);
    req.method = 'POST';
    req.path = path.join('/', esDomain.index, esDomain.doctype);
    req.region = esDomain.region;
    req.body = doc;
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    // Sign the request (Sigv4)
    var signer = new AWS.Signers.V4(req, 'es');
    signer.addAuthorization(creds, new Date());
    // Post document to ES
    var send = new AWS.NodeHttpClient();
    send.handleRequest(req, null, function(httpResp) {
        var body = '';
        httpResp.on('data', function (chunk) {
            body += chunk;
        });
        httpResp.on('end', function (chunk) {
            numDocsAdded ++;
            if (numDocsAdded === totLogLines) {
                // Mark lambda success.  If not done so, it will be retried.
                console.log('All ' + numDocsAdded + ' log records added to ES.');
                context.succeed();
            }
        });
    }, function(err) {
        console.log('Error: ' + err);
        console.log(numDocsAdded + 'of ' + totLogLines + ' log records added to ES.');
        context.fail();
    });
}
/* Lambda "main": Execution starts here */
exports.handler = function(event, context) {
    console.log('Received event: ', JSON.stringify(event, null, 2));
    /* == Streams ==
     * To avoid loading an entire (typically large) log file into memory,
     * this is implemented as a pipeline of filters, streaming log data
     * from S3 to ES.
     * Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
     */
    var lineStream = new LineStream();
    // A stream of log records, from parsing each log line
    var recordStream = new stream.Transform({objectMode: true})
    recordStream._transform = function(line, encoding, done) {
        var logRecord = parse(line.toString());
        var serializedRecord = JSON.stringify(logRecord);
        this.push(serializedRecord);
        totLogLines ++;
        done();
    }
    event.Records.forEach(function(record) {
        var bucket = record.s3.bucket.name;
        var objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
        s3LogsToES(bucket, objKey, context, lineStream, recordStream);
    });
}

function parse(line) {

    var url = require('url');
    
    // Fields in log lines are essentially space separated,
    // but are also quote-enclosed for strings containing spaces.
    var field_names = [
        'type',
        'timestamp',
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
        'chosen_cert_arn'          
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
            }
            else if (c == " ") {
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
            }
            else {
                // Part of this field.
                current_value += c;
            }
        }
        else {
            if (c == '"') {
                // Ending a quoted field.
                within_quotes = false;
            }
            
            else {
                // Part of this quoted field.
                current_value += c;
            }
        }
    }

    // Second phase, breaking out the port for the client and target, if there's a colon present
    colon_sep = ['client', 'target']
    for (var i in colon_sep) {
        var orig = parsed[colon_sep[i]];

        if (orig.indexOf(":") > 0) {
            splat = orig.split(":");
            parsed[colon_sep[i]] = splat[0]
            parsed[colon_sep[i] + "_port"] = Number(splat[1])
        }
    }

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

            parsed['request_uri_scheme'] = uri.protocol;
            parsed['request_uri_host']   = uri.hostname;
            parsed['request_uri_port']   = uri.port;
            parsed['request_uri_path']   = uri.pathname;
            parsed['request_uri_query']  = uri.query;
        } 

        // Otherwise, we just leave them out.
        catch (e) {}
    } 

    // All done.
    return parsed;
}
