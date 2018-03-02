# ALB-Logs-to-Elasticsearch
#Send ALB logs from S3 bucket to ElasticSearch using AWS Lambda.
This project based on [awslabs/amazon-elasticsearch-lambda-samples,blmr/aws-elb-logs-to-elasticsearch](https://github.com/awslabs/amazon-elasticsearch-lambda-samples,https://github.com/blmr/aws-elb-logs-to-elasticsearch.git)
Sample code for AWS Lambda to get AWS ELB log files from S3, parse
and add them to an Amazon Elasticsearch Service domain.


#### Deployment Package Creation
1. On your development machine, download and install [Node.js](https://nodejs.org/en/).
2. Go to root folder of the repository and install node dependencies by running:

   ```
   npm install
   ```

   Verify that these are installed within the `node_modules` subdirectory.
3. Create a zip file to package the *index.js* and the `node_modules` directory

The zip file thus created is the Lambda Deployment Package.

### AWS Configuration 

Set up the Lambda function and the S3 bucket. You can reffer to for more details >
[Lambda-S3 Walkthrough](http://docs.aws.amazon.com/lambda/latest/dg/walkthrough-s3-events-adminuser.html).

Please keep in mind the following notes and configuration overrides:

* The S3 bucket must be created in the same region as Lambda is, so that it
  can push events to Lambda.

* When registering the S3 bucket as the data-source in Lambda, add a filter
  for files having `.log.gz` suffix, so that Lambda picks up only apache log files.

* You need to set Lambda environment variables for the following:
```
 ES_DOCTYPE: for the `type` field in Elasticsearch
 ES_ENDPOINT: the FQDN of your AWS Elasticsearch Service
 ES_INDEX_PREFIX: the prefix for your indices, which will be suffixed with the date
 ES_REGION: The AWS region, e.g. us-west-1, of your Elasticsearch instance
 ```

* The following authorizations are required:

  1. Lambda permits S3 to push event notification to it
  2. S3 permits Lambda to fetch the created objects from a given bucket
  3. ES permits Lambda to add documents to the given domain
  4. Lambda handler is set to `index.handler`
  5. Don't forget the ES domain parameters in index.js

  The Lambda console provides a simple way to create an IAM role with policies
  for (1).  
  For (2), when creating the IAM role, choose the "S3 execution role"
  option; this will load the role with permissions to read from the S3
  bucket.  
  For (3), add the following access policy to permit ES operations
  to the role.
  
```
{
      "Sid": "AllowLambdaAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:role/lambda_s3_exec_role"
      },
      "Action": "es:*",
      "Resource": "arn:aws:es:eu-west-1:123456789012:domain/elastic-search-domain/*"
}
```
For (5)

```
var esDomain = {
    endpoint: 'elastic-search-domain-123456.us-east-1.es.amazonaws.com',
    region: 'us-east-1',
    index: 'alb-access-logs-' + indexTimestamp,
    doctype: 'alb-access-logs'
};
```

**Event source**
Add Event source for your lambda function

`Event source type: S3`

`Bucket: s3-elb-access-logs`

`Event type: Object Created (All)`

`Suffix: .log.gz`


#License 
ASL

[https://aws.amazon.com/asl/](https://aws.amazon.com/asl/)
