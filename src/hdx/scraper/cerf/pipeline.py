#!/usr/bin/python
"""CERF scraper"""

import logging
from datetime import datetime

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        tempdir: str,
        today: datetime,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._today = today
        self.data = {}
        self.dates = {}
        self.headers = {}

    def get_data(self):
        for data_type in self._configuration["data_types"]:
            self.data[data_type] = {}
            self.dates[data_type] = {}
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
                        new_row[key] = value
                        if key == "latestDate":
                            self.add_header("latestDateAsDate", data_type)
                            new_row["latestDateAsDate"] = value[:10]
                for key, value in aggregate_row.items():
                    if aggregation_method == "list":
                        new_row[key] = ", ".join(sorted(list(set(value))))
                    if aggregation_method == "sum":
                        new_row[key] = sum(value)
                date_col = (
                    "latestDateAsDate"
                    if "latestDateAsDate" in new_row
                    else "dateUSGSignature"
                )
                date_val = parse_date(new_row[date_col])
                if data_type == "allocations":
                    country_code = new_row["countryCode"]
                    dict_of_lists_add(self.data[data_type], country_code, new_row)
                    dict_of_lists_add(self.dates[data_type], country_code, date_val)
                else:
                    dict_of_lists_add(self.data[data_type], "world", new_row)
                    dict_of_lists_add(self.dates[data_type], "world", date_val)
        return

    def generate_dataset(self, data_type: str, country_iso: str) -> Dataset | None:
        dataset_info = self._configuration[data_type]
        if country_iso == "world":
            country_name = "Global"
        else:
            country_name = Country.get_country_name_from_iso3(country_iso)
        dataset_title = f"{country_name} - {dataset_info['title']}"
        dataset_name = slugify(dataset_info["title"])
        if country_iso != "world":
            dataset_name = f"{dataset_name}-{country_iso.lower()}"
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )
        notes = dataset_info["notes"].format(country=f" for {country_name}")
        if country_name == "Global":
            notes = notes.replace(" for Global", "")
        dataset["notes"] = notes

        dates = self.dates[data_type][country_iso]
        end_date = max(dates)
        if end_date > self._today:
            end_date = self._today
        dataset.set_time_period(min(dates), end_date)
        dataset.add_tags(self._configuration["tags"])
        if country_iso == "world":
            dataset.add_other_location("world")
        else:
            try:
                dataset.add_country_location(country_iso)
            except HDXError:
                logger.error(f"Could not find country iso {country_iso}")
                return None

        # Add resources
        resource_name = f"{dataset_info['title']}.csv"
        if country_iso != "world":
            resource_name = f"{country_iso} {resource_name}"
        resourcedata = {
            "name": resource_name,
            "description": "",
        }
        dataset.generate_resource(
            self._tempdir,
            resource_name,
            self.data[data_type][country_iso],
            resourcedata,
            self.headers[data_type],
        )

        resource = Resource(
            {
                "name": f"{dataset_info['title']}.json",
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
