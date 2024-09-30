CREATE TABLE business_events.UserProfileFields (
  master_person_id STRING NOT NULL,
  client_person_id STRING NOT NULL,
  organization_id STRING NOT NULL,
  client_slug STRING NOT NULL,
  created_date TIMESTAMP NOT NULL,
  last_updated_date TIMESTAMP NOT NULL,
  field_name STRING NOT NULL,
  field_value STRING
)
USING delta;
