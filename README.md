## how to install spanner emulator

https://cloud.google.com/spanner/docs/emulator

Install Gcloud SDK.

Install Docker.

Run Gcloud SDK Shell as admin

1st run only:

gcloud components update

gcloud beta emulators spanner start

Non-1st run:

Start the docker manually, or run: gcloud beta emulators spanner start
gcloud config configurations create emulator

Run:

gcloud config set auth/disable_credentials true

gcloud config set api_endpoint_overrides/spanner http://localhost:9020/

gcloud config set project spannerproject

gcloud spanner instances create apollo-test --config=emulator-config --description="Apollo Test Instance" --nodes=1

gcloud spanner databases create apollodbtest --instance="apollo-test"

Remark: can add --ddl switch with ddl commands to create tables and indices�

DDL updates:

gcloud spanner databases ddl update apollodbtest --instance="apollo-test" --ddl="CREATE TABLE TEST_TABLE (a INT64) PRIMARY KEY (a)"

DML updates:

gcloud spanner rows insert --table="TEST_TABLE" --database="apollodbtest" --instance="apollo-test" --data="a=1"

drop all dbs in instance: gcloud beta spanner databases delete example-db --instance=test-instance

drop instance and its dbs: gcloud beta spanner instances delete apollo-test
