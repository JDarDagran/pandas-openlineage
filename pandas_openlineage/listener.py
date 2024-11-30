import datetime
from openlineage.client import OpenLineageClient
from openlineage.client import set_producer
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    job_type_job,
)
from openlineage.client.uuid import generate_static_uuid
import attr
import pandas as pd
import logging

import atexit

log = logging.getLogger(__name__)

_listener = None

set_producer("http://github.com/JDarDagran/pandas-openlineage")


@attr.define
class DataFrameLineageMetadata:
    dataframe: pd.DataFrame
    path: str


class OpenLineagePandasListener:
    def __init__(self):
        self._client: OpenLineageClient = None
        self.script_location: str | None = None
        self.inputs: dict[str, DataFrameLineageMetadata] = {}
        self.outputs: dict[str, DataFrameLineageMetadata] = {}
        self._start_event_emitted = False
        self._run_id: str | None = None

    @property
    def client(self):
        if self._client is None:
            self._client = OpenLineageClient()
        return self._client

    def register_input_dataframe(self, dataframe_metadata: DataFrameLineageMetadata):
        self.inputs[id(dataframe_metadata.dataframe)] = dataframe_metadata

    def register_output_dataframe(self, dataframe_metadata: DataFrameLineageMetadata):
        self.outputs[id(dataframe_metadata.dataframe)] = dataframe_metadata

    def on_read(self, dataframe_metadata: DataFrameLineageMetadata):
        self.register_input_dataframe(dataframe_metadata)

    def on_write(self, dataframe_metadata: DataFrameLineageMetadata):
        self.register_output_dataframe(dataframe_metadata)

    def on_exit(self):
        self.emit(RunState.COMPLETE)

    @property
    def run_id(self) -> str:
        if not self._run_id:
            self._run_id = str(
                generate_static_uuid(
                    instant=datetime.datetime.now(),
                    data="pandas".encode(),
                )
            )
        return self._run_id

    def on_transform(self, dataframe_metadata: DataFrameLineageMetadata):
        log.info(f"Transform dataframe with path {dataframe_metadata.path}")
        # TODO: self.register_transformation(dataframe_metadata)

    def attempt_to_emit_start_event(self):
        if not self._start_event_emitted:
            self.emit(RunState.START)
            self._start_event_emitted = True

    @property
    def has_script_location_set(self):
        return self.script_location is not None

    def set_script_location(self, script_location: str):
        self.script_location = script_location

    def emit(self, state: RunState = RunState.START):
        from pandas_openlineage.utils import convert_filepath_to_openlineage_dataset

        event_time = datetime.datetime.now()

        event = RunEvent(
            eventType=state,
            eventTime=event_time.isoformat(),
            run=Run(str(self.run_id)),
            job=Job(
                "pandas",
                self.script_location,
                facets={
                    "jobType": job_type_job.JobTypeJobFacet(
                        jobType="TASK", integration="PANDAS", processingType="BATCH"
                    )
                },
            ),
            inputs=[
                convert_filepath_to_openlineage_dataset(df)
                for df in self.inputs.values()
            ],
            outputs=[
                convert_filepath_to_openlineage_dataset(df)
                for df in self.outputs.values()
            ],
        )
        self.client.emit(event)


def get_openlineage_listener() -> OpenLineagePandasListener:
    global _listener
    if _listener is None:
        _listener = OpenLineagePandasListener()
        atexit.register(_listener.on_exit)
    return _listener
