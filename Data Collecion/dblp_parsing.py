from lxml import etree
import json
import time


def extract_text(elem,conf,start_year=2010,end_year=2021,strict_matching=False):
    """
    GET THE TEXT FROM THE SUBELEMENTS OF THE INPROCEEDING ELELEMNTS

    :param elem: lxml element from which we will extract the children
    :type elem: lxml.etree.Element object
    :param conf: DBLP key for the conferences
    :type conf: string or list of strings
    :param start_year: Year from which to include the papers for this conference
    :type start_year:int
    :param end_year: Year untill which to include the papers for this conferene
    :type end_year: int
    :param strict_matching: Indicates whether the conference id matching with the dblp key is strict. /conf/nips/2019-1 matches with /conf/nips/2019 with strict_matching=False. Used to include the workshops or if the publication is broken into multiple volumes.
    :type strict_matching: Boolean

    :return: Dictionary containing the children of the inproceeding with the child tag name as key, as the child text as value
    """
    #--------------------------------------------------------------------------------------
    # create a list of dblp keys for the conferences across the years we want to select
    #---------------------------------------------------------------------------------------

    if type(conf)==list:
            # conf includes keys for multiple conferences so loop through the conf as well
            conf_list=[con+"/"+str(i) for i in range(start_year,end_year) for con in conf]
    else:
        #conf is a simple string containg the key of the conference we are interested in
        conf_list=[conf+"/"+str(i) for i in range(start_year,end_year)]


    # dblp key is an attribute of the element itself, so add this to the element dict. Element dict will store the related metadata for a single publication
    element_dict = {"key": elem.attrib['key']}
    # iterate through the subelements and build the element dict
    for sub in elem:
        # do not include in json if the subelement has no text
        if sub.text:
            # add subelement and it's text as a new key,value pair in the dict. If the key exists, add to existing list
            feature_list = element_dict.get(sub.tag, [])
            feature_list += [sub.text]
            element_dict[sub.tag] = feature_list

    #----------------------------------------------------------------------------------------------------------
    # we will only return the subelement dict if it includes a crossref to one of our conferences in conf_list.
    #------------------------------------------------------------------------------------------------------------
    if strict_matching:
        # conference key has to exactly match the text in the crossref
        if element_dict.get("crossref",[""])[0] in conf_list:
            return element_dict
        else:
            #return empty dict if the crossref child does not exist or has a different value than the conference key we want
            return {}

    else:
        # get the key that is present in the current crossref tag of the element
        crossref_key=element_dict.get("crossref",[""])[0]
        # loop through all possible conference keys we're interested in and compare them to crossref_key
        for conference in conf_list:
            # substring check. If conference key is a substring of crossref_key, we will accept this as a match
            if conference in crossref_key:
                return element_dict
        # no matches return an empty list
        return {}


def to_json(dblp_file,out_file,conf):
    """
    CONVERT A DBLP XML FILE INTO A JSON FILE

    :param dblp_file: Path for the dblp xml file
    :type dblp_file: string
    :param out_file: Path for the resultant JSON file
    :type out_file: string
    :param conf: DBLP key for the conference we're extracting
    :type conf: string
    """

    #create a iterparse object to iteratively parse through the xml file since the xml file can't fit in memory
    parse = etree.iterparse(dblp_file, dtd_validation=True, load_dtd=True)
    results = []
    #-----------------------------------------------------------------------------------------------------------------------------
    # Iterate through the xml file, select elements the inproceedings tag and add every sub-element with a tag to the json file
    #-----------------------------------------------------------------------------------------------------------------------------
    for _, elem in parse:
        # filter by inproceedings, which is stand in for conference papers
        if elem.tag == "inproceedings":
            element_dict=extract_text(elem,conf)
            # add the individual element dict to the result, if the element_dict is not empty.
            if element_dict:
                results.append(element_dict)

        field_list=["author", "editor", "title", "booktitle", "pages", "year", "address", "journal", "volume", "number", "month", "url", "ee", "cdrom", "cite", "publisher", "note", "crossref", "isbn", "series", "school", "chapter"]
        if not elem.tag in field_list:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        #Clear the parsed element from memory

    # dump the dict
    json.dump(results,open(out_file,"w"))


if __name__=="__main__":
    to_json("dblp.xml","dblp.json",["conf/nips","conf/eccv","conf/cvpr","conf/icml","conf/iccv","conf/acl","conf/aaai","conf/emnlp","conf/chi","conf/kdd"])
