#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# Files created in miner.py have errors due to a bad VPN connection
# 1. Split files over correct lines and error lines
# 2. get metadata for error lines
import csv
import os

import time

import logging

import sys
from fedora.rest.api import Fedora
from fedora.rest.ds import DatastreamProfile

LOG = logging.getLogger(__name__)


class Corrector(object):

    def __init__(self):
        self.fedora = Fedora()

    def split(self, file_incompletes, start=1, end=2):
        with open(file_incompletes, "w", newline='') as out_inc_file:
            incomplete_writer = csv.writer(out_inc_file)
            incomplete_writer.writerow(["dataset_id", "file_id"])
            for x in (start, end):
                in_file = os.path.expanduser("~/SURFdrive/mining/recent_arch_files_" + str(x) + ".csv")
                out_file = os.path.expanduser("~/SURFdrive/mining/recent_arch_files_corr_" + str(x) + ".csv")
                with open(in_file, "r", newline='') as read_file, open(out_file, "w", newline='') as write_file:
                    reader = csv.reader(read_file)
                    writer = csv.writer(write_file)
                    headers = next(reader, None)
                    writer.writerow(headers)
                    for row in reader:
                        if row[2] == "ERROR":
                            incomplete_writer.writerow(row[0:2])
                        else:
                            writer.writerow(row)

    def get_metedata(self, file_incompletes, file_completes, file_err, count):
        with open(file_incompletes, "r", newline='') as in_file, \
                open(file_completes, "a", newline='') as out_file, \
                open(file_err, "w", newline='') as err_file:
            reader = csv.reader(in_file)
            writer = csv.writer(out_file)
            err_writer = csv.writer(err_file)
            next(reader, None)
            if count == 1:
                writer.writerow(["dataset_id", "file_id", "media_type", "label", "SHA1", "size"])
            err_writer.writerow(["dataset_id", "file_id"])
            errors = 0
            for row in reader:
                time.sleep(1)
                dataset_id = row[0]
                file_id = row[1]
                try:
                    dsp = DatastreamProfile(file_id, "EASY_FILE")
                    dsp.fetch()
                    writer.writerow([dataset_id, file_id, dsp.ds_mime, dsp.ds_label, dsp.ds_checksum, dsp.ds_size])
                    LOG.debug("%d, %s %s" % (errors, file_id, dsp.ds_label))
                except:
                    errors += 1
                    err_writer.writerow([dataset_id, file_id])
                    LOG.exception("Failed")
        return errors


if __name__ == '__main__':
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    start = 3
    end = 3
    file_incompletes = os.path.expanduser("~/SURFdrive/mining/incomplete_file_.csv")
    file_completes = os.path.expanduser("~/SURFdrive/mining/recent_arch_files_errata.csv")
    file_err = os.path.expanduser("~/SURFdrive/mining/err_file_.csv")
    corrector = Corrector()
    corrector.split(file_incompletes, start, end)
    errors = -1
    counter = 0
    while errors != 0:
        counter += 1
        LOG.debug("Fetching incomplete metdada # %d" % counter)
        errors = corrector.get_metedata(file_incompletes, file_completes, file_err, counter)
        os.remove(file_incompletes)
        os.rename(file_err, file_incompletes)

