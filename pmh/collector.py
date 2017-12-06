#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import csv

from pmh.harvester import Harvester

infile = "data/1_inDans_inArchis2.txt"
outfile = "data/inDans_inArchis2_doi.csv"
missing = "data/missing_inDans_inArchis2.csv"

host_address = "https://easy.dans.knaw.nl/oai/"


def collect():
    x = 0
    harvester = Harvester(host_address)
    with open(infile, "r", newline='') as in_file, \
            open(outfile, "w", newline='') as out_file, \
            open(missing, "w", newline='') as missing_file:
        reader = csv.reader(in_file)
        writer = csv.writer(out_file)
        missing_writer = csv.writer(missing_file)
        headers = next(reader, None)
        headers.append("doi")
        writer.writerow(headers)
        for row in reader:
            x += 1
            easy_id = row[1]
            print(x, easy_id)
            doi = harvester.find_doi(easy_id)
            if doi is None:
                missing_writer.writerow([row[0], easy_id, row[2]])
            else:
                row.append(doi)
                writer.writerow(row)


if __name__ == '__main__':
    collect()
