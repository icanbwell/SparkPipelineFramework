CREATE TABLE business_events.ActivityDefinition (
  master_person_id STRING NOT NULL,
  client_person_id STRING NOT NULL,
  organization_id STRING NOT NULL,
  client_slug STRING NOT NULL,
  activity_name STRING NOT NULL,
  activity_id STRING NOT NULL,
  created_date TIMESTAMP NOT NULL,
  last_updated_date TIMESTAMP NOT NULL
  --- add additional fields ---
)
USING DELTA;
