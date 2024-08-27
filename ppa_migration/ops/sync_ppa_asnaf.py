from dagster import op
from dagster_airbyte import airbyte_sync_op

# Airbyte sync operation
@op
def sync_ppa_asnaf():
    return airbyte_sync_op.configured(
        {"connection_id": "0ea080d7-e172-4a82-8ae5-ecb691b9ec86"},
        name="sync_ppa_asnaf"
    )
