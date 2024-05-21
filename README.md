# cumulus-postworkflow-normalizer
Lambda function that normalizes cumulus message in post ingest workflows

During cumulus bulk operation, a previous ingested files and exceptions will be retrieved
and replayed to targeted sub-workflows.  This lambda cleanse the un-necessary items from payload 
for the following steps/tasks to work.
