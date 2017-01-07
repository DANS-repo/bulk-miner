#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import csv

import logging
import os

import sys

import time
from fedora.rest.api import Fedora
from fedora.rest.ds import DatastreamProfile

LOG = logging.getLogger(__name__)


class NlDialect(object):
    delimiter = ';'
    quotechar = '"'
    escapechar = None
    doublequote = True
    skipinitialspace = False
    lineterminator = '\r\n'
    quoting = csv.QUOTE_MINIMAL


class Miner(object):

    def __init__(self, read_dialect=NlDialect, write_dialect=csv.excel):
        self.fedora = Fedora()
        self.read_dialect = read_dialect
        self.write_dialect = write_dialect

    def create_pdf_file_list(self, input_file, output_file):
        with open(input_file, "r", newline='') as in_file, open(output_file, "w", newline='') as out_file:
            reader = csv.reader(in_file, dialect=self.read_dialect)
            writer = csv.writer(out_file, dialect=self.write_dialect)
            headers = next(reader, None)
            LOG.debug("Input headers: % s" % headers)
            writer.writerow(["dataset_id", "file_id", "media_type" "label", "SHA1", "size"])
            ds_count = 0
            ds_errors = 0
            file_errors = 0
            for row in reader:
                time.sleep(1)
                dataset_id = row[0]
                ds_count += 1
                try:
                    file_ids = self.list_files(dataset_id)
                except:
                    ds_errors += 1
                    file_ids = []
                    writer.writerow([dataset_id, "ERROR", "ERROR", "ERROR", "ERROR", "ERROR"])
                    LOG.exception("Failed dataset")

                for file_id in file_ids:
                    time.sleep(1)
                    try:
                        dsp = DatastreamProfile(file_id, "EASY_FILE")
                        dsp.fetch()
                        writer.writerow([dataset_id, file_id, dsp.ds_mime, dsp.ds_label, dsp.ds_checksum, dsp.ds_size])
                        LOG.debug("%d, %d, %d, %s %s" % (ds_count, ds_errors, file_errors, file_id, dsp.ds_label))
                    except:
                        file_errors += 1
                        writer.writerow([dataset_id, file_id, "ERROR", "ERROR", "ERROR", "ERROR"])
                        LOG.exception("Failed file")

    def list_files(self, dataset_id):
        query = \
            "PREFIX dans: <http://dans.knaw.nl/ontologies/relations#> " \
            + "PREFIX fmodel: <info:fedora/fedora-system:def/model#> " \
            \
            + "SELECT ?s " \
            + "WHERE " \
            + "{ " \
            + "   ?s dans:isSubordinateTo <info:fedora/" + dataset_id + "> . " \
            + "   ?s fmodel:hasModel <info:fedora/easy-model:EDM1FILE> " \
            + "}"

        result = self.fedora.risearch(query)
        lines = result.split('\n')
        file_ids = []
        for line in lines[1:]:
            if line.startswith("info:"):
                pid = line.split("/")
                file_ids.append(pid[1])
        return file_ids


if __name__ == '__main__':
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    input_file = os.path.expanduser("~/SURFdrive/mining/recent_arch_datesets.csv")
    # input_file = os.path.expanduser("~/SURFdrive/mining/test.csv")
    output_file = os.path.expanduser("~/SURFdrive/mining/recent_pdf_files.csv")

    miner = Miner()
    miner.create_pdf_file_list(input_file, output_file)







