from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.cerf.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestCERF",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                today = parse_date("05-18-2026")
                pipeline = Pipeline(configuration, retriever, tempdir, today)
                pipeline.get_data()

                dataset = pipeline.generate_dataset("allocations", "SDN")
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "cerf-allocations-sdn",
                    "title": "Sudan - CERF Allocations",
                    "notes": "This dataset lists project funding allocations from OCHA's Central Emergency Response Fund (CERF) for Sudan. CERF allocations are made to ensure a rapid response to sudden-onset emergencies or to rapidly deteriorating conditions in an existing emergency and to support humanitarian response activities within an underfunded emergency.",
                    "dataset_date": "[2006-05-11T00:00:00 TO 2026-05-06T23:59:59]",
                    "tags": [
                        {
                            "name": "funding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "groups": [{"name": "sdn"}],
                    "license_id": "cc-by-igo",
                    "methodology": "Registry",
                    "caveats": "None",
                    "dataset_source": "CERF",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "ccc6d05d-d2fd-45f9-be24-05c340968b4b",
                    "owner_org": "cerf",
                    "data_update_frequency": 7,
                    "subnational": 0,
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "SDN CERF Allocations.csv",
                        "description": "",
                        "format": "csv",
                    },
                    {
                        "name": "CERF Allocations.json",
                        "description": "",
                        "url": "http://cerfgms-webapi.unocha.org/v1/hdxproject/all.json",
                        "format": "json",
                    },
                ]
                file = "SDN CERF Allocations.csv"
                assert_files_same(join(fixtures_dir, file), join(tempdir, file))

                dataset = pipeline.generate_dataset("donor-contributions", "world")
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "cerf-donor-contributions",
                    "title": "Global - CERF Donor Contributions",
                    "notes": "This dataset lists all contributions made by donors to the Central Emergency Response Fund (CERF). CERF receives broad support from United Nations Member States, observers, regional governments and international organizations, and the private sector, including corporations, non-governmental organizations and individuals.",
                    "dataset_date": "[2015-07-03T00:00:00 TO 2026-05-12T23:59:59]",
                    "tags": [
                        {
                            "name": "funding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "groups": [{"name": "world"}],
                    "license_id": "cc-by-igo",
                    "methodology": "Registry",
                    "caveats": "None",
                    "dataset_source": "CERF",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "ccc6d05d-d2fd-45f9-be24-05c340968b4b",
                    "owner_org": "cerf",
                    "data_update_frequency": 7,
                    "subnational": 0,
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "CERF Donor Contributions.csv",
                        "description": "",
                        "format": "csv",
                    },
                    {
                        "name": "CERF Donor Contributions.json",
                        "description": "",
                        "url": "http://cerfgms-webapi.unocha.org/v1/donorcontribution.json",
                        "format": "json",
                    },
                ]
                file = "CERF Donor Contributions.csv"
                assert_files_same(join(fixtures_dir, file), join(tempdir, file))
