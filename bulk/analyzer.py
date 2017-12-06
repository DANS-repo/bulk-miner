#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import logging
import os

LOG = logging.getLogger(__name__)


class Analyzer(object):

    def count_datasets(self):
        counter = {}
        teller = 0
        for x in range(1, 5):
            corr_file = os.path.expanduser("~/SURFdrive/mining/recent_arch_files_corr_" + str(x) + ".csv")

            with open(corr_file, "r", newline='') as read_file:
                reader = csv.reader(read_file)
                headers = next(reader, None)
                for row in reader:
                    teller += 1
                    dataset_id = row[0]
                    if dataset_id in counter:
                        count = counter[dataset_id]
                    else:
                        count = 0
                    count += 1
                    counter.update({dataset_id: count})
            print(corr_file, str(len(counter)), str(teller))

        file_count = os.path.expanduser("~/SURFdrive/mining/count_files_per_ds.csv")
        with open(file_count, "w", newline='') as write_file:
            writer = csv.writer(write_file)
            for row in counter.items():
                writer.writerow([row[0], row[1]])

        print("total:", str(len(counter)), str(teller))

    def count_mediaTypes(self):
        counter = {}
        teller = 0
        for x in range(1, 5):
            corr_file = os.path.expanduser("~/SURFdrive/mining/recent_arch_files_corr_" + str(x) + ".csv")

            with open(corr_file, "r", newline='') as read_file:
                reader = csv.reader(read_file)
                headers = next(reader, None)
                for row in reader:
                    teller += 1
                    mediatype = row[2]
                    if mediatype in counter:
                        count = counter[mediatype]
                    else:
                        count = 0
                    count += 1
                    counter.update({mediatype: count})
            print(corr_file, str(len(counter)), str(teller))

        file_count = os.path.expanduser("~/SURFdrive/mining/count_files_per_media_type.csv")
        with open(file_count, "w", newline='') as write_file:
            writer = csv.writer(write_file)
            for row in counter.items():
                writer.writerow([row[0], row[1]])

        print("total:", str(len(counter)), str(teller))

    def select_per_mediatype(self, mediatype):
        name = mediatype.replace("/", "_")
        type_file = os.path.expanduser("~/SURFdrive/mining/select_" + name + ".csv")
        with open(type_file, "w", newline='') as write_file:
            writer = csv.writer(write_file)
            writer.writerow(["dataset_id", "file_id", "media_type", "label", "SHA1", "size"])
            for x in range(1, 5):
                corr_file = os.path.expanduser("~/SURFdrive/mining/recent_arch_files_corr_" + str(x) + ".csv")
                with open(corr_file, "r", newline='') as read_file:
                    reader = csv.reader(read_file)
                    headers = next(reader, None)
                    for row in reader:
                        if row[2] == mediatype:
                            writer.writerow(row)


if __name__ == '__main__':
    analyzer = Analyzer()
    analyzer.select_per_mediatype("application/x-framemaker")
