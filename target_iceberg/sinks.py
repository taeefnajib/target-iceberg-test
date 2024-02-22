"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
import os
from itertools import islice
from typing import Dict, List, Optional
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError, NoSuchTableError
from requests import HTTPError
from pyarrow import fs

from .iceberg import singer_to_pyiceberg_schema

       

class IcebergSink(BatchSink):
    """Iceberg target sink class."""

    max_size = 10000  # Max records to write in one batch

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        self.stream_name = stream_name
        self.schema = schema

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """

        # Get the first few items of the context for debugging
        n_items = dict(islice(context.items(), 3))
        self.logger.warning(str(n_items))

        # Extract records from the context
        records = context.get("records", [])

        # Define fields to drop from the schema
        fields_to_drop = ["_sdc_deleted_at", "_sdc_table_version", "_sdc_extracted_at", "_sdc_received_at", "_sdc_batched_at", "_sdc_sequence", "_sdc_sync_started_at"]

        # Drop unnecessary fields from the schema
        singer_schema_narrow = {
            "type": "object",
            "properties": {
                key: value
                for key, value in self.schema["properties"].items()
                if key not in fields_to_drop
            },
        }

        # Convert records to a pyarrow table
        df = pa.Table.from_pydict({"data": records})

        # Create a PyArrow FileSystem object for S3
        fs = fs.S3FileSystem()

        # Load the Iceberg catalog
        region = fs.resolve_s3_region(self.config.get("s3_bucket"))
        catalog_name = self.config.get("iceberg_catalog_name")
        catalog = load_catalog(
            catalog_name,
            uri=self.config.get("iceberg_rest_uri"),
            s3_endpoint=os.environ.get(
                "PYICEBERG_CATALOG__ICEBERGCATALOG__S3__ENDPOINT"
            ),
            py_io_impl="pyiceberg.io.pyarrow.PyArrowFileIO",
            s3_region=region,
            s3_access_key_id=os.environ.get(
                "PYICEBERG_CATALOG__ICEBERGCATALOG__S3__ACCESS_KEY_ID"
            ),
            s3_secret_access_key=os.environ.get(
                "PYICEBERG_CATALOG__ICEBERGCATALOG__S3__SECRET_ACCESS_KEY"
            ),
        )

        # List namespaces in the catalog
        nss = catalog.list_namespaces()
        self.logger.info(f"Namespaces: {nss}")

        # Create a namespace if it doesn't exist
        ns_name = self.config.get("iceberg_catalog_namespace_name")
        try:
            catalog.create_namespace(ns_name)
            self.logger.info(f"Namespace '{ns_name}' created")
        except (NamespaceAlreadyExistsError, NoSuchNamespaceError):
            self.logger.info(f"Namespace '{ns_name}' already exists")

        # Create a table if it doesn't exist
        table_name = self.stream_name
        table_id = f"{ns_name}.{table_name}"
        try:
            table = catalog.load_table(table_id)
            self.logger.info(f"Table '{table_id}' loaded")
        except NoSuchTableError as e:
            # Table doesn't exist, so create it
            table_schema = singer_to_pyiceberg_schema(singer_schema_narrow)
            table = catalog.create_table(table_id, schema=table_schema)
            self.logger.info(f"Table '{table_id}' created")

        # Add data to the table
        table.append(df)
