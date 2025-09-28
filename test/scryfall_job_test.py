from mtgv2.definitions import defs
from dagster import DagsterInstance


def test_scryfall_job():
    # Create an DagsterInstance
    instance = DagsterInstance.ephemeral()
    result = defs.resolve_job_def("scryfall_job_test").execute_in_process(
        instance=instance
    )
    assert result.success
