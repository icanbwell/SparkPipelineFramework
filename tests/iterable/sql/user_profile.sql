CREATE TABLE business_events.UserProfile (
  master_person_id STRING NOT NULL,
  client_person_id STRING NOT NULL,
  organization_id STRING NOT NULL,
  client_slug STRING NOT NULL,
  created_date TIMESTAMP NOT NULL,
  last_updated_date TIMESTAMP NOT NULL
)
USING delta;
