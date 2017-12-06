#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import urllib
from xml.etree import ElementTree

import requests

LOG = logging.getLogger(__name__)

class HttpError(RuntimeError):
    pass


class Harvester(object):

    def __init__(self, host_address):
        self.host_address = host_address

    def identify(self):
        url = self.host_address + "?verb=Identify"
        response = requests.get(url)
        content = response.content
        print(content)
        root = ElementTree.fromstring(content)
        print(root.tag, root.attrib)
        for child in root:
            print(child.tag, child.attrib, child.text)
            for kid in child:
                print("\t", kid.tag, "\t", kid.text)
                for k in kid:
                    print("\t", "\t", k.tag, k.text)
                    for x in k:
                        print("\t", "\t", "\t", x.tag, x.text)

    def get_record(self, easy_id, metadata_prefix="oai_dc"):
        url = self.host_address + "?verb=GetRecord&metadataPrefix=" + metadata_prefix \
              + "&identifier=oai:easy.dans.knaw.nl:" + easy_id
        response = requests.get(url)
        if response.status_code != requests.codes.ok:
            raise HttpError("Invalid response: " + response.status_code + " for " + url)
        content = response.content
        root = ElementTree.fromstring(content)
        for error in root.findall("{http://www.openarchives.org/OAI/2.0/}error"):
            LOG.error("Error response: " + str(error.attrib) + " [" + error.text + "] for " + easy_id)
            return root, error
        return root, None

    def find_doi(self, easy_id):
        """
        Find the doi for a given easy_id. If the record with the given id does not exists
        or if it has no doi we'll return None.
        :param easy_id: the dataset id
        :return: the doi of the dataset or None
        :raises: HttpError for transport errors.
        """
        root, error = self.get_record(easy_id)
        for identifier in root.iter("{http://purl.org/dc/elements/1.1/}identifier"):
            if identifier.text.startswith("doi:"):
                return identifier.text
        return None


if __name__ == '__main__':
    harvester = Harvester("https://easy.dans.knaw.nl/oai/")
    doi = harvester.find_doi("easy-dataset:660")
    print(doi)
