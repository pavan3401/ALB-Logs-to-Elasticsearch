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
    endpoint: 'Elasticsearch Endpoint',
    region: 'us-east-1',
    index: 'alblogs-' + indexTimestamp, // adds a timestamp to index. Example: elblogs-2016.03.31
    doctype: 'alb-access-logs'
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
    var parsed = {};
    var url = require('url');
    var request_labels =
        [
            'request_method',
            'request_uri',
            'request_http_version',
            'request_uri_scheme',
            'request_uri_host',
            'request_uri_port',
            'request_uri_path',
            'request_uri_query'
        ];
    //
    // Trailing newline? NOTHX
    //
    if (line.match(/\n$/)) {
        line = line.slice(0, line.length - 1);
    }
    [
        { 'type'                        : ' '   },
        { 'timestamp'                   : ' '   },
        { 'elb'                         : ' '   },
        { 'client'                      : ':'   },
        { 'client_port'                 : ' '   },
        { 'target'                      : ':'   },
        { 'target_port'                 : ' '   },
        { 'request_processing_time'     : ' '   },
        { 'target_processing_time'      : ' '   },
        { 'response_processing_time'    : ' '   },
        { 'elb_status_code'             : ' '   },
        { 'target_status_code'          : ' '   },
        { 'received_bytes'              : ' '   },
        { 'sent_bytes'                  : ' "'  },
        { 'request'                     : '" "' },
        { 'user_agent'                  : '" '  },
        { 'ssl_cipher'                  : ' '   },
        { 'ssl_protocol'                : ' '   },
        { 'target_group_arn'            : ' "'  },
        { 'trace_id'                    : '" '  }
    ].some(function (t) {
        var label = Object.keys(t)[0];
        delimiter = t[label]
        var m = line.match(delimiter);
        if (m === null) {
            //
            // No match. Try to pick off the last element.
            //
            m = line.match(delimiter.slice(0, 1));
            if (m === null) {
                field = line;
            }
            else {
                field = line.substr(0, m.index);
            }
            parsed[label] = field;
            return true;
        }
        field = line.substr(0, m.index);
        line = line.substr(m.index + delimiter.length);
        parsed[label] = field;
    });
    // target
    if(parsed.target != -1) {
        parsed['target_port'] = parsed.target.split(":")[1];
        parsed['target'] = parsed.target.split(":")[0];
    } else {
        parsed['target_port'] = '-1';
    }
    // console.log('request: [' + JSON.stringify(parsed) + ']');
    // request
    if(parsed.request != '- - - ') {
        var i = 0;
        var method = parsed.request.split(" ")[0];
        var url = url.parse(parsed.request.split(" ")[1]);
        var http_version = parsed.request.split(" ")[2];
        parsed[request_labels[i]] = method;
        i++;
        parsed[request_labels[i]] = url.href;
        i++;
        parsed[request_labels[i]] = http_version;
        i++;
        parsed[request_labels[i]] = url.protocol;
        i++;
        parsed[request_labels[i]] = url.hostname;
        i++;
        parsed[request_labels[i]] = url.port;
        i++;
        parsed[request_labels[i]] = url.pathname;
        i++;
        parsed[request_labels[i]] = url.query;
    } else {
        request_labels.forEach(function(label) {
            parsed[label] = '-';
        });
    }
    return parsed;
};
