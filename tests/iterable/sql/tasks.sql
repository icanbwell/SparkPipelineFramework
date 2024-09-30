CREATE TABLE business_events.Tasks (
    master_person_id STRING NOT NULL,
    client_person_id STRING NOT NULL,
    organization_id STRING NOT NULL,
    client_slug STRING NOT NULL,
    created_date TIMESTAMP NOT NULL,
    last_updated_date TIMESTAMP NOT NULL,
    activity_definition_id STRING NOT NULL,
    task_name STRING NOT NULL,
    task_id STRING NOT NULL,
    completed_date TIMESTAMP
)
USING DELTA;
