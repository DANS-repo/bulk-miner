#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import csv

from pmh.harvester import Harvester

class NlDialect(object):
    delimiter = ';'
    quotechar = '"'
    escapechar = None
    doublequote = True
    skipinitialspace = False
    lineterminator = '\r\n'
    quoting = csv.QUOTE_MINIMAL

headerfile = "data2/input/inDans_analoogArchis_doi_2.csv"

infile = "data2/input/inDans_analoogArchis_doi_2.csv"
outfile = "data2/output/inDans_analoogArchis_doi_2.csv"
missing = "data2/output/missing_inDans_analoogArchis.csv"
dialect = NlDialect

# infile = "data2/input/inDans_inArchis2_doi.csv"
# outfile = "data2/output/inDans_inArchis2_doi.csv"
# missing = "data2/output/missing_inDans_inArchis2.csv"
# dialect = csv.excel

host_address = "https://easy.dans.knaw.nl/oai/"


def get_header():
    with open(headerfile, "r", newline='') as h_file:
        reader = csv.reader(h_file, dialect=NlDialect)
        header = next(reader, None)
        return header


def collect2(new_header, row_dataset_id=1):
    x = 0
    harvester = Harvester(host_address)
    with open(infile, "r", newline='') as in_file, \
            open(outfile, "w", newline='') as out_file, \
            open(missing, "w", newline='') as missing_file:
        reader = csv.reader(in_file, dialect=dialect)
        writer = csv.writer(out_file, dialect=NlDialect)
        missing_writer = csv.writer(missing_file, dialect=NlDialect)
        old_headers = next(reader, None)
        r_k = []
        count = 0
        for head in old_headers:
            if head in new_header:
                r_k.append(True)
            else:
                r_k.append(False)
            count += 1

        headers = [x for x in old_headers if x in new_header]
        headers.append("dcmi_type")
        writer.writerow(headers)

        for row in reader:
            x += 1
            easy_id = row[row_dataset_id]

            new_row = []
            count = 0
            for val in row:
                if r_k[count]:
                    new_row.append(val)
                count += 1

            print(x, easy_id)
            dcmi_type = harvester.find_dcmi_type(easy_id)
            if dcmi_type is None:
                missing_writer.writerow([row[0], easy_id, row[2]])
                new_row.append("")
            else:
                new_row.append(dcmi_type)

            writer.writerow(new_row)

if __name__ == '__main__':
    new_header = get_header()
    new_header[0] = "datasetId"
    collect2(new_header, row_dataset_id=0)
