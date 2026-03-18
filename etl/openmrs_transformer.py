

def transform_encounter_provider(drop_create=False):
    # TODO: Read from _encounter table
    # TODO: Join with _provider table

    insert_sql = "INSERT INTO encounter_provider (encounter_id, provider_id) VALUES (:encounter_id, :provider_id)"

    pass

