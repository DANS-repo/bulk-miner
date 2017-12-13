#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
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

    def find_dcmi_type(self, easy_id):
        """
        Find the `dcmi:type` for a given easy_id. If the record with the given id does not exists
        or if it has no `dcmi:type` we'll return None.
        :param easy_id: the dataset id
        :return: the `dcmi:type` of the dataset or None
        :raises: HttpError for transport errors.
        """
        root, error = self.get_record(easy_id, metadata_prefix="nl_didl")
        for mods_form in root.iter("{http://www.loc.gov/mods/v3}form"):
            if "authority" in mods_form.attrib and mods_form.attrib["authority"] == "http://purl.org/dc/terms/DCMIType":
                return mods_form.text
        return None


if __name__ == '__main__':
    # testing:
    harvester = Harvester("https://easy.dans.knaw.nl/oai/")

    #print(harvester.find_doi("easy-dataset:660"))
    print(harvester.find_dcmi_type("easy-dataset:5206"))
