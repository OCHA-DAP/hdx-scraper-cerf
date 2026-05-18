#!/usr/bin/python
"""CERF scraper"""

import logging

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self.data = {}
        self.dates = {}
        self.headers = {}

    def get_data(self):
        for data_type in self._configuration["data_types"]:
            source_url = self._configuration[data_type]["source_url"]
            json = self._retriever.download_json(source_url)
            nested_variables = self._configuration[data_type]["nested_variables"]
            aggregation_method = self._configuration[data_type]["aggregation_method"]
            for row in json:
                new_row = {}
                aggregate_row = {}
                for key, value in row.items():
                    if isinstance(value, dict):
                        for _, subvalue in value.items():
                            for subrow in subvalue:
                                for subsubkey, subsubvalue in subrow.items():
                                    if subsubkey not in nested_variables:
                                        continue
                                    aggregate_key = nested_variables[subsubkey]
                                    self.add_header(aggregate_key, data_type)
                                    dict_of_lists_add(
                                        aggregate_row, aggregate_key, subsubvalue
                                    )
                    else:
                        self.add_header(key, data_type)
                        if key == "dateUSGSignature":
                            value = value[:10]
                            dict_of_lists_add(self.dates, data_type, value)
                        new_row[key] = value
                        if key == "latestDate":
                            self.add_header("latestDateAsDate", data_type)
                            new_row["latestDateAsDate"] = value[:10]
                            dict_of_lists_add(self.dates, data_type, value[:10])
                for key, value in aggregate_row.items():
                    if aggregation_method == "list":
                        new_row[key] = ", ".join(sorted(list(set(value))))
                    if aggregation_method == "sum":
                        new_row[key] = sum(value)
                dict_of_lists_add(self.data, data_type, new_row)
        return

    def generate_dataset(self, data_type: str) -> Dataset | None:
        dataset_info = self._configuration[data_type]
        dataset_title = dataset_info["title"]
        dataset_name = slugify(dataset_title)
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
                "notes": dataset_info["notes"],
            }
        )

        dataset.set_time_period(min(self.dates[data_type]), max(self.dates[data_type]))
        dataset.add_tags(self._configuration["tags"])
        dataset.add_other_location("world")

        # Add resources
        resourcedata = {
            "name": f"{dataset_title}.csv",
            "description": "",
        }
        dataset.generate_resource(
            self._tempdir,
            f"{dataset_title}.csv",
            self.data[data_type],
            resourcedata,
            self.headers[data_type],
        )

        resource = Resource(
            {
                "name": f"{dataset_title}.json",
                "description": "",
                "url": self._configuration[data_type]["source_url"],
            }
        )
        resource.set_format("json")
        dataset.add_update_resource(resource)
        return dataset

    def add_header(self, header: str, data_type: str) -> None:
        if data_type not in self.headers:
            self.headers[data_type] = []
        if header not in self.headers[data_type]:
            self.headers[data_type].append(header)
