import pytest
import unittest
import boto3
from moto import mock_aws

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
            "event": {
                "cumulus_meta": {
                    "cumulus_version": "11.1.0",
                    "execution_name": "9d560872-a476-49a4-ba41-74de88723224",
                    "message_source": "sfn",
                    "queueExecutionLimits": {
                        "https://sqs.us-west-2.amazonaws.com/123456789012/sliu-background-job-queue": 50,
                        "https://sqs.us-west-2.amazonaws.com/123456789012/sliu-backgroundProcessing": 5,
                        "https://sqs.us-west-2.amazonaws.com/123456789012/sliu-big-background-job-queue": 50,
                        "https://sqs.us-west-2.amazonaws.com/123456789012/sliu-dmrpp-background-job-queue": 50,
                        "https://sqs.us-west-2.amazonaws.com/123456789012/sliu-forge-background-job-queue": 50,
                        "https://sqs.us-west-2.amazonaws.com/123456789012/sliu-tig-background-job-queue": 50
                    },
                    "state_machine": "arn:aws:states:us-west-2:123456789012:stateMachine:sliu-ForgeWorkflow",
                    "system_bucket": "sliu-internal",
                    "workflow_start_time": 1657564471453,
                    "asyncOperationId": "54d07403-4f8b-480d-9479-7d036b2e4dd0",
                    "queueUrl": "arn:aws:sqs:us-west-2:123456789012:sliu-startSF"
                },
                "exception": null,
                "meta": {
                    "buckets": {
                        "dashboard": {
                            "name": "sliu-dashboard",
                            "type": "private"
                        },
                        "glacier": {
                            "name": "sliu-glacier",
                            "type": "orca"
                        },
                        "internal": {
                            "name": "sliu-internal",
                            "type": "internal"
                        },
                        "pre-swot-staging": {
                            "name": "podaac-dev-pre-swot-ocean-sim",
                            "type": "internal"
                        },
                        "private": {
                            "name": "sliu-private",
                            "type": "private"
                        },
                        "protected": {
                            "name": "sliu-protected",
                            "type": "protected"
                        },
                        "public": {
                            "name": "sliu-public",
                            "type": "public"
                        },
                        "test": {
                            "name": "podaac-dev-cumulus-test-input-v2",
                            "type": "internal"
                        },
                        "test-staging": {
                            "name": "podaac-sndbx-staging",
                            "type": "internal"
                        }
                    },
                    "cmr": {
                        "clientId": "POCUMULUS",
                        "cmrEnvironment": "UAT",
                        "cmrLimit": 100,
                        "cmrPageSize": 50,
                        "oauthProvider": "launchpad",
                        "passwordSecretName":"password",
                        "provider": "POCUMULUS",
                        "username":"podaaccumulus"
                    },
                    "collection": {
                        "createdAt": 1598302172443,
                        "updatedAt": 1656527072310,
                        "name": "MODIS_A-JPL-L2P-v2019.0",
                        "version": "2019.0",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "duplicateHandling": "replace",
                        "granuleId": "^[0-9]{14}-JPL-L2P_GHRSST-SSTskin-MODIS_A-[DN]-v02\\.0-fv01\\.0$",
                        "granuleIdExtraction": "^([0-9]{14}-JPL-L2P_GHRSST-SSTskin-MODIS_A-[DN]-v02\\.0-fv01\\.0)((\\.nc)|(\\.nc\\.md5)|(\\.cmr\\.json)|(\\.nc\\.dmrpp))?$",
                        "files": [
                            {
                                "type": "data",
                                "regex": "^[0-9]{14}-JPL-L2P_GHRSST-SSTskin-MODIS_A-[DN]-v02\\.0-fv01\\.0\\.nc$",
                                "bucket": "protected",
                                "sampleFileName": "20200101000000-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc"
                            },
                            {
                                "type": "metadata",
                                "regex": "^[0-9]{14}-JPL-L2P_GHRSST-SSTskin-MODIS_A-[DN]-v02\\.0-fv01\\.0\\.nc\\.md5$",
                                "bucket": "public",
                                "sampleFileName": "20200101000000-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.md5"
                            },
                            {
                                "type": "metadata",
                                "regex": "^[0-9]{14}-JPL-L2P_GHRSST-SSTskin-MODIS_A-[DN]-v02\\.0-fv01\\.0\\.cmr\\.json$",
                                "bucket": "private",
                                "sampleFileName": "20200101000000-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.cmr.json"
                            },
                            {
                                "type": "metadata",
                                "regex": "^[0-9]{14}-JPL-L2P_GHRSST-SSTskin-MODIS_A-[DN]-v02\\.0-fv01\\.0\\.nc\\.dmrpp$",
                                "bucket": "protected",
                                "sampleFileName": "20200101000000-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.dmrpp"
                            }
                        ],
                        "sampleFileName": "20200101000000-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
                        "meta": {
                            "glacier-bucket": "sliu-glacier",
                            "workflowChoice": {
                                "dmrpp": true,
                                "image": true,
                                "glacier": true,
                                "footprint": true,
                                "compressed": false,
                                "convertNetCDF": false,
                                "readDataFileForMetadata": true
                            },
                            "response-endpoint": "arn:aws:sns:us-west-2:065089468788:sliu-provider-response-sns",
                            "granuleRecoveryWorkflow": "OrcaRecoveryWorkflow"
                        }
                    },
                    "distribution_endpoint": "https://w7leoc6233.execute-api.us-west-2.amazonaws.com/DEV/",
                    "launchpad": {
                        "api": "https://api.launchpad.nasa.gov/icam/api/sm/v1",
                        "certificate": "launchpad.pfx",
                        "passphraseSecretName": "sliu-message-template-launchpad-passphrase20200804225948237500000008"
                    },
                    "provider": {
                        "id": "podaac-test-s3",
                        "globalConnectionLimit": 1000,
                        "host": "podaac-dev-cumulus-test-input-v2",
                        "protocol": "s3",
                        "createdAt": 1584730564096,
                        "updatedAt": 1584730564096
                    },
                    "stack": "sliu",
                    "template": "s3://sliu-internal/sliu/workflow_template.json",
                    "workflow_name": "ForgeWorkflow",
                    "workflow_tasks": {}
                },
                "payload": {
                    "granules": [
                        {
                            "beginningDateTime": "2021-05-01T06:50:01.000Z",
                            "cmrLink": "https://cmr.uat.earthdata.nasa.gov/search/concepts/G1243703485-POCUMULUS.umm_json",
                            "collectionId": "MODIS_A-JPL-L2P-v2019.0___2019.0",
                            "createdAt": 1657563862307,
                            "duration": 113.518,
                            "endingDateTime": "2021-05-01T06:55:00.000Z",
                            "error": {
                                "Cause": "None",
                                "Error": "Unknown Error"
                            },
                            "execution": "https://console.aws.amazon.com/states/home?region=us-west-2#/executions/details/arn:aws:states:us-west-2:065089468788:execution:sliu-ThumbnailImageWorkflow:4d38b818-2bf0-44ef-8b11-2261c703261f",
                            "files": [
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-d761143c-386e-4df6-9717-104b5dd9824d.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-d761143c-386e-4df6-9717-104b5dd9824d.cmr.json",
                                    "size": 4743
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-f59cc32e-b8b8-4974-a906-d736dd4eac41.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-f59cc32e-b8b8-4974-a906-d736dd4eac41.cmr.json",
                                    "size": 3647
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-0fcc29ec-2c28-4c86-a3f5-2b583fb005ed.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-0fcc29ec-2c28-4c86-a3f5-2b583fb005ed.cmr.json",
                                    "size": 4743
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.md5",
                                    "key": "file-staging/sliu/MODIS_A-JPL-L2P-v2019.0___2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.md5",
                                    "size": 98,
                                    "source": "Simon_Collection/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.md5",
                                    "type": "metadata"
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-ed594dc1-a79a-427b-9789-61c2b61e80f2.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-ed594dc1-a79a-427b-9789-61c2b61e80f2.cmr.json",
                                    "size": 4743
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-25d6e74a-faa4-4ae6-a74c-ad0990d33e0f.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-25d6e74a-faa4-4ae6-a74c-ad0990d33e0f.cmr.json",
                                    "size": 3588
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-5ca22fb3-8a53-b81d-69dc-96e150a2a85b.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-5ca22fb3-8a53-b81d-69dc-96e150a2a85b.cmr.json",
                                    "size": 4743
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "checksum": "62cb066dceed01090134cb4bfb649091",
                                    "checksumType": "md5",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
                                    "key": "file-staging/sliu/MODIS_A-JPL-L2P-v2019.0___2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
                                    "size": 19923752,
                                    "source": "Simon_Collection/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
                                    "type": "data"
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-16401cdb-81ea-ecd3-c121-09c4dd2265de.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-16401cdb-81ea-ecd3-c121-09c4dd2265de.cmr.json",
                                    "size": 4743
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-84f9d0f4-f211-4d5d-99ec-bb2375b3074e.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-84f9d0f4-f211-4d5d-99ec-bb2375b3074e.cmr.json",
                                    "size": 3408
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-4d38b818-2bf0-44ef-8b11-2261c703261f.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-4d38b818-2bf0-44ef-8b11-2261c703261f.cmr.json",
                                    "size": 3827
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.fp",
                                    "key": "dataset-metadata/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.fp",
                                    "size": 539,
                                    "type": "metadata"
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-d0f5a408-07e1-4eac-92f1-93038f8f862e.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-d0f5a408-07e1-4eac-92f1-93038f8f862e.cmr.json",
                                    "size": 3408
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-fc8e62e4-b33a-4d21-9162-816d745cbf73.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-fc8e62e4-b33a-4d21-9162-816d745cbf73.cmr.json",
                                    "size": 4504
                                },
                                {
                                    "bucket": "sliu-public",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.sses_standard_deviation.png",
                                    "key": "MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.sses_standard_deviation.png",
                                    "size": 2758,
                                    "type": "metadata"
                                },
                                {
                                    "bucket": "sliu-public",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.sses_bias.png",
                                    "key": "MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.sses_bias.png",
                                    "size": 2828,
                                    "type": "metadata"
                                },
                                {
                                    "bucket": "sliu-public",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.quality_level.png",
                                    "key": "MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.quality_level.png",
                                    "size": 6753,
                                    "type": "metadata"
                                },
                                {
                                    "bucket": "sliu-public",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.sea_surface_temperature.png",
                                    "key": "MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.sea_surface_temperature.png",
                                    "size": 2825,
                                    "type": "metadata"
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-bd515456-58a4-4912-adf8-0a6dfeec370f.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-bd515456-58a4-4912-adf8-0a6dfeec370f.cmr.json",
                                    "size": 3408
                                },
                                {
                                    "bucket": "sliu-private",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.cmr.json",
                                    "key": "MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.cmr.json",
                                    "size": 1451,
                                    "type": "metadata"
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-ae3aa0a0-f747-47ac-b7ce-bf01df7d6b10.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-ae3aa0a0-f747-47ac-b7ce-bf01df7d6b10.cmr.json",
                                    "size": 4172
                                },
                                {
                                    "bucket": "fake-sliu-protected",
                                    "checksum": "62cb066dceed01090134cb4bfb649091",
                                    "checksumType": "md5",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
                                    "key": "MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
                                    "size": 19923752,
                                    "source": "Simon_Collection/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
                                    "type": "data"
                                },
                                {
                                    "bucket": "sliu-public",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.md5",
                                    "key": "MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.md5",
                                    "size": 98,
                                    "source": "Simon_Collection/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.md5",
                                    "type": "metadata"
                                },
                                {
                                    "bucket": "sliu-public",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.cmr.json",
                                    "key": "MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.cmr.json",
                                    "size": 1473
                                },
                                {
                                    "bucket": "sliu-protected",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.dmrpp",
                                    "key": "MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.dmrpp",
                                    "size": 31469,
                                    "type": "metadata"
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-cbb486ed-8ecb-43c2-8eee-c8cd26294273.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-cbb486ed-8ecb-43c2-8eee-c8cd26294273.cmr.json",
                                    "size": 4192
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-004e30db-fbca-485f-8e1e-75da3b7e85ff.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-004e30db-fbca-485f-8e1e-75da3b7e85ff.cmr.json",
                                    "size": 4192
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-63be7f8c-0eba-4133-8dc9-5f8ce431000b.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-63be7f8c-0eba-4133-8dc9-5f8ce431000b.cmr.json",
                                    "size": 4743
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-6b753ee6-a31d-4271-a932-c9293458ed14.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-6b753ee6-a31d-4271-a932-c9293458ed14.cmr.json",
                                    "size": 3827
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-52d6afb5-d4ee-466c-a3dd-e6b1781f5620.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-52d6afb5-d4ee-466c-a3dd-e6b1781f5620.cmr.json",
                                    "size": 3588
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-0b147d47-43d4-4bec-aca0-61a2d8e7c13f.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-0b147d47-43d4-4bec-aca0-61a2d8e7c13f.cmr.json",
                                    "size": 3408
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-d25a3a12-7927-4f5e-b65c-3c7c42c33afd.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-d25a3a12-7927-4f5e-b65c-3c7c42c33afd.cmr.json",
                                    "size": 3408
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-2610ac40-8598-43e8-bc20-29a461a7f29d.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-2610ac40-8598-43e8-bc20-29a461a7f29d.cmr.json",
                                    "size": 4192
                                },
                                {
                                    "bucket": "sliu-internal",
                                    "fileName": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-c28571e0-5bc9-4199-9f37-433755aa65bf.cmr.json",
                                    "key": "CMR/MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0-c28571e0-5bc9-4199-9f37-433755aa65bf.cmr.json",
                                    "size": 4192
                                }
                            ],
                            "granuleId": "20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0",
                            "lastUpdateDateTime": "2022-07-11T18:25:20.273Z",
                            "processingEndDateTime": "2022-07-11T18:28:17.080Z",
                            "processingStartDateTime": "2022-07-11T18:26:24.879Z",
                            "productionDateTime": "2021-10-25T02:43:54.000Z",
                            "productVolume": "19942841",
                            "provider": "podaac-test-s3",
                            "published": true,
                            "status": "completed",
                            "timestamp": 1657564098066,
                            "timeToArchive": 0.652,
                            "timeToPreprocess": 3.283,
                            "updatedAt": 1657564098066
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


@mock_aws
class TestPostworkflowNormalizer(unittest.TestCase):

    def setup_method(self, method):
        """set up aws resources for test"""

        self.bucket = 'fake-sliu-protected'
        self.key = 'MODIS_A-JPL-L2P-v2019.0/20210501065001-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc'

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
        assert 'G1243703485-POCUMULUS' == result.get('payload').get('granules')[0]['cmrConceptId']
        assert 'type' in result.get('payload').get('granules')[0]['files'][0]
        assert 'ecs_lambda' in result['meta']['collection']['meta']['workflowChoice']
        assert result['meta']['collection']['meta']['workflowChoice']['ecs_lambda'] == 'lambda'
