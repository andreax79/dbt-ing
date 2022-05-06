#!/usr/bin/env python

from unittest import TestCase, main


class TestImport(TestCase):
    def test_import_batch_to_datalake(self):
        from dbting.batch_to_datalake import batch_to_datalake

        self.assertIsNotNone(batch_to_datalake)

    def test_import_external_tables(self):
        from dbting.external_tables import create_external_tables, drop_external_tables, repair_external_tables

        self.assertIsNotNone(create_external_tables)
        self.assertIsNotNone(drop_external_tables)
        self.assertIsNotNone(repair_external_tables)

    def test_import_qm(self):
        from dbting.qm import QueryManager

        self.assertIsNotNone(QueryManager)

    def test_import_utils(self):
        from dbting.utils import load_mapping, load_config, run, to_bool

        self.assertIsNotNone(load_mapping)
        self.assertIsNotNone(load_config)
        self.assertIsNotNone(run)
        self.assertIsNotNone(to_bool)

    def test_import_xlsx_to_json_mapping(self):
        from dbting.xlsx_to_json_mapping import xlsx_to_json_mapping

        self.assertIsNotNone(xlsx_to_json_mapping)


if __name__ == "__main__":
    main()
