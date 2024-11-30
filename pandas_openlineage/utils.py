import fsspec
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import schema_dataset
from pandas_openlineage.listener import DataFrameLineageMetadata


def convert_filepath_to_openlineage_dataset(df: DataFrameLineageMetadata) -> str:
    """
    Convert a file path to an OpenLineage dataset.

    This is a simple method that converts a given file path to an OpenLineage dataset.
    """
    fs, _, paths = fsspec.get_fs_token_paths(df.path)
    scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
    # just an example for s3, should be handled in different way
    if scheme in ("s3", "s3a"):
        bucket, path, _ = fs.split_path(df.path)
        namespace = f"s3://{bucket}"
    else:
        namespace = f"{scheme}://"
        path = paths[0]
    return Dataset(
        name=path,
        namespace=namespace,
        facets={
            "schema": schema_dataset.SchemaDatasetFacet(
                fields=[
                    schema_dataset.SchemaDatasetFacetFields(name=col, type="string")
                    for col in df.dataframe.columns
                ]
            )
        },
    )
