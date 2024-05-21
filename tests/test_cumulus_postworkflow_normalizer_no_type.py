import pytest
import unittest
import boto3
from moto import mock_s3

null = None
false = False
true = True

event = {
    "resource": "arn:aws:lambda:us-west-2:123456789012:function:sliu-forge",
    "input": {
        "cma": {
            "task_config": {
                "collection": "{$.meta.collection}",
                "cumulus_message": {
                    "input": "{$.payload}"
                }
            },
            "event":{
               "cumulus_meta":{
                  "cumulus_version":"11.1.4",
                  "execution_name":"5d9ab68e-c5d4-4a31-ba1a-9f8d972cf968",
                  "message_source":"sfn",
                  "queueExecutionLimits":{
                     "https://sqs.us-west-2.amazonaws.com/123456789012/podaac-sit-cumulus-background-job-queue":100,
                     "https://sqs.us-west-2.amazonaws.com/123456789012/podaac-sit-cumulus-backgroundProcessing":5,
                     "https://sqs.us-west-2.amazonaws.com/123456789012/podaac-sit-cumulus-big-background-job-queue":100,
                     "https://sqs.us-west-2.amazonaws.com/123456789012/podaac-sit-cumulus-dmrpp-background-job-queue":50,
                     "https://sqs.us-west-2.amazonaws.com/123456789012/podaac-sit-cumulus-forge-background-job-queue":100,
                     "https://sqs.us-west-2.amazonaws.com/123456789012/podaac-sit-cumulus-tig-background-job-queue":100
                  },
                  "state_machine":"arn:aws:states:us-west-2:123456789012:stateMachine:podaac-sit-cumulus-DMRPPWorkflow",
                  "system_bucket":"podaac-sit-cumulus-internal",
                  "workflow_start_time":1660947744747,
                  "asyncOperationId":"e281cc42-c6b9-4fa3-adc2-5926ef45e1f0",
                  "queueUrl":"arn:aws:sqs:us-west-2:123456789012:podaac-sit-cumulus-startSF"
               },
               "exception":null,
               "meta":{
                  "buckets":{
                     "documentation":{
                        "name":"podaac-sit-cumulus-docs",
                        "type":"public"
                     },
                     "ecco-staging":{
                        "name":"podaac-ecco-v4r4",
                        "type":"internal"
                     },
                     "glacier":{
                        "name":"podaac-sit-cumulus-glacier",
                        "type":"orca"
                     },
                     "internal":{
                        "name":"podaac-sit-cumulus-internal",
                        "type":"internal"
                     },
                     "podaac-dev":{
                        "name":"podaac-dev-*",
                        "type":"internal"
                     },
                     "private":{
                        "name":"podaac-sit-cumulus-private",
                        "type":"private"
                     },
                     "protected":{
                        "name":"podaac-sit-cumulus-protected",
                        "type":"protected"
                     },
                     "public":{
                        "name":"podaac-sit-cumulus-public",
                        "type":"public"
                     },
                     "sit-test":{
                        "name":"podaac-sit-cumulus-test-input",
                        "type":"internal"
                     },
                     "staging":{
                        "name":"podaac-sit-cumulus-staging",
                        "type":"internal"
                     }
                  },
                  "cmr":{
                     "clientId":"POCUMULUS",
                     "cmrEnvironment":"UAT",
                     "cmrLimit":100,
                     "cmrPageSize":50,
                     "oauthProvider":"launchpad",
                     "passwordSecretName":"password",
                     "provider":"POCUMULUS",
                     "username":"podaaccumulus"
                  },
                  "collection":{
                     "createdAt":1644219864883,
                     "updatedAt":1644566326869,
                     "name":"AVHRRF_MC-STAR-L3U-v2.80",
                     "version":"2.80",
                     "url_path":"{cmrMetadata.CollectionReference.ShortName}/{dateFormat(cmrMetadata.TemporalExtent.RangeDateTime.BeginningDateTime, YYYY/DDDD)}",
                     "duplicateHandling":"replace",
                     "granuleId":"^[0-9]{14}-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2\\.80-v02\\.0-fv01\\.0$",
                     "granuleIdExtraction":"^([0-9]{14}-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2\\.80-v02\\.0-fv01\\.0)((\\.nc)|(\\.nc\\.md5)|(\\.cmr\\.json)|(\\.nc\\.dmrpp))?$",
                     "files":[
                        {
                           "type":"data",
                           "regex":"^[0-9]{14}-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2\\.80-v02\\.0-fv01\\.0\\.nc$",
                           "bucket":"protected",
                           "sampleFileName":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc"
                        },
                        {
                           "type":"metadata",
                           "regex":"^[0-9]{14}-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2\\.80-v02\\.0-fv01\\.0\\.nc\\.md5$",
                           "bucket":"public",
                           "sampleFileName":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.md5"
                        },
                        {
                           "type":"metadata",
                           "regex":"^[0-9]{14}-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2\\.80-v02\\.0-fv01\\.0\\.cmr\\.json$",
                           "bucket":"private",
                           "sampleFileName":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.cmr.json"
                        },
                        {
                           "type":"metadata",
                           "regex":"^[0-9]{14}-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2\\.80-v02\\.0-fv01\\.0\\.nc\\.dmrpp$",
                           "bucket":"protected",
                           "sampleFileName":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.dmrpp"
                        }
                     ],
                     "sampleFileName":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc",
                     "meta":{
                        "glacier-bucket":"podaac-sit-cumulus-glacier",
                        "workflowChoice":{
                           "dmrpp":true,
                           "glacier":true,
                           "compressed":false,
                           "convertNetCDF":false,
                           "readDataFileForMetadata":true
                        },
                        "response-endpoint":[
                           "arn:aws:sns:us-west-2:244527681937:podaac-sit-cumulus-provider-response-sns",
                           "arn:aws:sns:us-west-2:254524764682:sit-cumulus-response-topic"
                        ],
                        "granuleRecoveryWorkflow":"DrRecoveryWorkflow"
                     }
                  },
                  "distribution_endpoint":"https://archive.podaac.sit.earthdata.nasa.gov/",
                  "launchpad":{
                     "api":"https://api.launchpad.nasa.gov/icam/api/sm/v1",
                     "certificate":"launchpad.pfx",
                     "passphraseSecretName":"podaac-sit-cumulus-message-template-launchpad-passphrase2020022107311360810000000c"
                  },
                  "provider":{
                     "id":"podaac-sit-on-prem-migration",
                     "globalConnectionLimit":1000,
                     "host":"podaac-sit-cumulus-staging",
                     "protocol":"s3",
                     "createdAt":1586977762763,
                     "updatedAt":1620058912038
                  },
                  "stack":"podaac-sit-cumulus",
                  "template":"s3://podaac-sit-cumulus-internal/podaac-sit-cumulus/workflow_template.json",
                  "workflow_name":"DMRPPWorkflow",
                  "workflow_tasks":{
                     
                  }
               },
               "payload":{
                  "granules":[
                     {
                        "beginningDateTime":"2022-02-06T22:50:00.000Z",
                        "cmrLink":"https://cmr.uat.earthdata.nasa.gov/search/concepts/G1243219234-POCUMULUS.umm_json",
                        "collectionId":"AVHRRF_MC-STAR-L3U-v2.80___2.80",
                        "createdAt":1644566671238,
                        "duration":87.853,
                        "endingDateTime":"2022-02-06T22:58:15.000Z",
                        "error":{
                           
                        },
                        "execution":"https://console.aws.amazon.com/states/home?region=us-west-2#/executions/details/arn:aws:states:us-west-2:244527681937:execution:podaac-sit-cumulus-IngestWorkflow:45fc4569-1908-4e00-96b1-d6011407b88d",
                        "files":[
                           {
                              "bucket":"podaac-sit-cumulus-private",
                              "fileName":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.cmr.json",
                              "key":"AVHRRF_MC-STAR-L3U-v2.80/2022/037/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.cmr.json",
                              "size":1563
                           },
                           {
                              "bucket":"podaac-sit-cumulus-protected",
                              "fileName":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.dmrpp",
                              "key":"AVHRRF_MC-STAR-L3U-v2.80/2022/037/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.dmrpp",
                              "size":41075,
                              "source":"s3://podaac-sit-cumulus-staging/AVHRRF_MC-STAR-L3U-v2.80/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.dmrpp"
                           },
                           {
                              "bucket":"podaac-sit-cumulus-public",
                              "fileName":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.md5",
                              "key":"AVHRRF_MC-STAR-L3U-v2.80/2022/037/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.md5",
                              "size":114,
                              "source":"s3://podaac-sit-cumulus-staging/AVHRRF_MC-STAR-L3U-v2.80/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.md5"
                           },
                           {
                              "bucket":"podaac-sit-cumulus-protected",
                              "checksum":"558e1ee831c5b111c087c1e17b9cd0c1",
                              "checksumType":"md5",
                              "fileName":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc",
                              "key":"AVHRRF_MC-STAR-L3U-v2.80/2022/037/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc",
                              "size":823405,
                              "source":"s3://podaac-sit-cumulus-staging/AVHRRF_MC-STAR-L3U-v2.80/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc"
                           }
                        ],
                        "granuleId":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0",
                        "lastUpdateDateTime":"2022-02-11T08:05:00.752Z",
                        "processingEndDateTime":"2022-02-11T08:05:58.065Z",
                        "processingStartDateTime":"2022-02-11T08:04:31.375Z",
                        "productionDateTime":"2022-02-06T23:51:37.000Z",
                        "productVolume":"866157",
                        "provider":"podaac-sit-on-prem-migration",
                        "published":true,
                        "queryFields":{
                           "cnm":{
                              "product":{
                                 "name":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0",
                                 "files":[
                                    {
                                       "uri":"https://archive.podaac.sit.earthdata.nasa.gov/podaac-sit-cumulus-protected/AVHRRF_MC-STAR-L3U-v2.80/2022/037/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc",
                                       "name":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc",
                                       "size":823405,
                                       "type":"data",
                                       "checksum":"558e1ee831c5b111c087c1e17b9cd0c1",
                                       "checksumType":"md5"
                                    },
                                    {
                                       "uri":"https://archive.podaac.sit.earthdata.nasa.gov/podaac-sit-cumulus-public/AVHRRF_MC-STAR-L3U-v2.80/2022/037/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.md5",
                                       "name":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.md5",
                                       "size":114,
                                       "type":"metadata"
                                    },
                                    {
                                       "uri":"https://archive.podaac.sit.earthdata.nasa.gov/podaac-sit-cumulus-private/AVHRRF_MC-STAR-L3U-v2.80/2022/037/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.cmr.json",
                                       "name":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.cmr.json",
                                       "size":1563,
                                       "type":"metadata"
                                    },
                                    {
                                       "uri":"https://archive.podaac.sit.earthdata.nasa.gov/podaac-sit-cumulus-protected/AVHRRF_MC-STAR-L3U-v2.80/2022/037/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.dmrpp",
                                       "name":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc.dmrpp",
                                       "size":41075,
                                       "type":"metadata"
                                    }
                                 ],
                                 "dataVersion":"2.80"
                              },
                              "version":"1.4",
                              "provider":"NASA/JPL/PO.DAAC",
                              "response":{
                                 "status":"SUCCESS",
                                 "ingestionMetadata":{
                                    "catalogId":"G1243219234-POCUMULUS",
                                    "catalogUrl":"https://cmr.uat.earthdata.nasa.gov/search/concepts/G1243219234-POCUMULUS.umm_json"
                                 }
                              },
                              "collection":"AVHRRF_MC-STAR-L3U-v2.80",
                              "identifier":"20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0",
                              "receivedTime":"2022-02-11T08:04:42.688Z",
                              "submissionTime":"2022-02-11T08:01:05.505175",
                              "processCompleteTime":"2022-02-11T08:05:53.930Z"
                           }
                        },
                        "status":"completed",
                        "timestamp":1644566759091,
                        "timeToArchive":21.779,
                        "timeToPreprocess":0.882,
                        "updatedAt":1644566759091
                     }
                  ]
               }
            }
        }
    },
    "inputDetails": {
        "truncated": false
    },
    "timeoutInSeconds": null
}


@mock_s3
class TestPostworkflowNormalizer(unittest.TestCase):

    def setup_method(self, method):
        """set up aws resources for test"""

        self.bucket = 'podaac-sit-cumulus-protected'
        self.key = 'AVHRRF_MC-STAR-L3U-v2.80/2022/037/20220206225000-STAR-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO_V2.80-v02.0-fv01.0.nc'

        aws_s3 = boto3.client('s3', region_name='us-east-1')
        res = aws_s3.create_bucket(Bucket=self.bucket)

        aws_s3.put_object(Bucket=self.bucket,
                          Key=self.key,
                          Body=b'hello world')

    def teardown_method(self, method):
        """clean up resources"""

        aws_s3 = boto3.client('s3', region_name='us-east-1')
        aws_s3.delete_object(Bucket=self.bucket, Key=self.key)
        aws_s3.delete_bucket(Bucket=self.bucket)

    def test_postworkflow_normalizer(self):
        
        from cumulus_postworkflow_normalizer import lambda_handler

        result = lambda_handler.handler(event["input"], None)
        assert len(result.get('payload').get('granules')[0].get('files')) == 1
        assert 'cmrConceptId' in result.get('payload').get('granules')[0]
        assert 'G1243219234-POCUMULUS' == result.get('payload').get('granules')[0]['cmrConceptId']
        assert 'type' in result.get('payload').get('granules')[0]['files'][0]
