# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
import logging
import sys

from .export import LeanplumExporter


@click.command()
@click.option("--app-id", required=True)
@click.option("--client-key", required=True)
@click.option("--date", required=True)
@click.option("--bucket", required=True)
@click.option("--prefix", default="")
@click.option("--bq-dataset", required=True)
@click.option("--table-prefix", default=None)
@click.option("--version", default=1)
def export_leanplum(app_id, client_key, date, bucket, prefix, bq_dataset, table_prefix, version):
    exporter = LeanplumExporter(app_id, client_key)
    exporter.export(date, bucket, prefix, bq_dataset, table_prefix, version)


@click.group()
def main(args=None):
    """Command line utility"""
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)


main.add_command(export_leanplum)


if __name__ == "__main__":
    sys.exit(main())
