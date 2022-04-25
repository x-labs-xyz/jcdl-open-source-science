import requests
from requests.exceptions import ConnectionError,HTTPError
from selectolax.parser import HTMLParser
from database_operations import query_by_conference, update_collection_one, query_by_field_exists
from tqdm import tqdm
from bs4 import BeautifulSoup
import re
import json
from opnieuw import RetryException, retry

requests_headers= {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}


@retry(retry_on_exceptions=(RetryException,ConnectionError,HTTPError),max_calls_total=5,retry_window_after_first_call_in_seconds=30)
def scrape_ieeexplore(url,keywords=True):
    """
    Scrape IEEE Xplore page to get citation details

    :param url: DOI or XPLORE URL for the page
    :type url: basestring
    :param keywords: Whether or not to inclue IEEE Keywords list if avaiabable
    :type keywords: Boolean
    :return: Dictionary with keywords and citation details

    """

    return_dict={}

    # Get page html
    page=requests.get(url,headers=requests_headers)

    #Create a BS4 instance
    soup=BeautifulSoup(page.text,features="lxml")

    #-------------------------------------------------------------------------------------------------------------
    # IEEE xplore stores its metadata within the scripts tags, as a js variable called "global.document.metadata"
    # so we try to find that by looping through the scripts and locating it using regex
    # -------------------------------------------------------------------------------------------------------------

    metadata={}

    pattern=re.compile(r"global\.document\.metadata=({.*});")

    # Loop through all the scripts in the file
    for js in soup.find_all("script", type="text/javascript"):
        # check if the script has text inside it (eliminates those scripts that load it from an external source with src)
        if js.string:
            #regex check
            match = re.search(pattern, str(js.string))
            if match:
                # load the metadata using json
                metadata=json.loads(match.group(1))

    # any metadata has been found:
    if metadata:
        if metadata.get("metrics",[]):
            return_dict['IEEE_Metrics']=metadata.get("metrics")
        if keywords and metadata.get("keywords",[]):
            return_dict["IEEE_keywords"]=metadata.get("keywords")
    else:
        # In case metadata is empty because of IEEE Xplore limiting, raise the retry exception
        raise RetryException

    return return_dict

def format_cvf_href(href):
    """
    CVF Open Access uses relative HTML for linking PDFs, this function makes sure that the absolute URL path is being used instead
    :param href: the relative path of the pdf field
    :type href: string
    :return: a string with the absolute URL for the pdf file
    """
    domain_root = "https://openaccess.thecvf.com/"


    # relative url contains the domain name
    if domain_root in href:
        return href

    else:
        #take away the path prefixes and add the domain_root instead
        return domain_root+href[6:]

@retry(retry_on_exceptions=(RetryException,ConnectionError,HTTPError),max_calls_total=5,retry_window_after_first_call_in_seconds=30)
def scrape_cvf(url):
    """
    SCRAPE PDF Links and Bibtext from the CVF Open Access pages
    :param url: URL to scrape from
    :type url: string
    :return: Dicitonary of items to add to the monogdb database
    """

    return_dict={}

    # get page html
    page=requests.get(url,headers=requests_headers)

    # create parser object
    tree=HTMLParser(page.text)

    # for pdf link
    if tree.css("a:contains(pdf)"):
        return_dict['PDF']=[format_cvf_href(c.attributes.get("href","")) for c in tree.css("a:contains(pdf)")]

    #bibtex search
    if tree.css('.bibref'):
        #a page can't have multiple bibtex links so extract text from the first div with class as bibref
        return_dict["bib"]=[tree.css(".bibref")[0].text()]

    return return_dict


@retry(retry_on_exceptions=(RetryException,ConnectionError,HTTPError),max_calls_total=5,retry_window_after_first_call_in_seconds=30)
def scrape_nips(url):
    """
    SCRAPER FOR NIPS

    :param url: URL for the papers in the NIPS site. Taken from ee attribute in DBLP
    :type url: String

    :return: Dicitonary of items to add to the monogdb database
    """

    return_dict={}
    # Add the url for the pdf  of the paper to the return dict, no need to scrape
    return_dict["PDF"]=[url+".pdf"]

    #Add the bitex link for the paper
    return_dict["bib"]=[url+"/bibtex"]

    # Get the page html
    page=requests.get(url,headers=requests_headers)

    #Create a htmlparser object
    tree=HTMLParser(page.text)

    #Search for a sourcecode text in the page; it exists, add it to the return dict, if not ignore
    if tree.css("a:contains([Sourcecode])"):
        # if multiple sourcecodes exits, they are added as a array
        return_dict["SourceCode"]=[c.attributes.get('href',"") for c in tree.css("a:contains([Sourcecode])")]

    return return_dict


@retry(retry_on_exceptions=(RetryException,ConnectionError,HTTPError),max_calls_total=5,retry_window_after_first_call_in_seconds=30)
def scrape_icml(url):
    """
    SCRAPER FOR ICML

    :param url: URL for the papers in the ICML site. Taken from ee attribute in DBLP
    :type url: String

    :return:Dictionary of PDF and Bibtex from ICML
    """

    return_dict={}

    # No website, just direct link to the pdf
    if url[:-3]=="pdf":
        return_dict["PDF"]=url
    else:
        # get page and create a parse
        page=requests.get(url,headers=requests_headers)
        tree=HTMLParser(page.text)

        #select pdf link by searching for download pdf text within an a tag
        if tree.css("a:contains(Download PDF)"):
            return_dict["PDF"]=[tree.css("a:contains(Download PDF)")[0].attributes['href']]

        #bibtext is available as a tex so we donot need to add a bib field, can directly add a bibtex
        if tree.css("#bibtex"):
            return_dict['bibtex']=[tree.css("#bibtex")[0].text()]

    return return_dict

@retry(retry_on_exceptions=(RetryException,ConnectionError,HTTPError),max_calls_total=5,retry_window_after_first_call_in_seconds=30)
def scrape_ACLWeb(url):
    """
    SCRAPER FOR AML

    :param url:URL for the papers in the ACLWeb site. Taken from ee attribute in DBLP
    :return:Dictionary of PDF, Bibtex, Source, Datasets etc from ICML
    """
    return_dict={}

    # get page and create a parse
    page = requests.get(url,headers=requests_headers)
    tree = HTMLParser(page.text)

    #-------------------------------
    # Extracting PDF Link
    #-------------------------------
    if tree.css("span:contains(PDF)"):
        # in case parents don't exist, or the href attributes don't exist, an exception is raised
        try:
            pdf=tree.css("span:contains(PDF)")[0].parent.attributes['href']
        except:
            pdf=""

        #if we found the pdf button, and it's non-empty then only add to the return dict
        if pdf:
            return_dict["PDF"]=[pdf]

    # -------------------------------
    # Extracting Bib Link
    # -------------------------------

    if tree.css("span:contains(Bib)"):
        # in case parents don't exist, or the href attributes don't exist, an exception is raised
        try:
            bib = tree.css("span:contains(Bib)")[0].parent.attributes['href']
        except:
            bib = ""

        # if we found the bibtex button, and it's non-empty then only add to the return dict
        if bib:
            return_dict["bib"] =["https://www.aclweb.org"+ bib]

    # -------------------------------
    # Extracting SourceCode Link
    # -------------------------------

    if tree.css("span:contains(Source)"):
        # in case parents don't exist, or the href attributes don't exist, an exception is raised
        try:
            source = tree.css("span:contains(Source)")[0].parent.attributes['href']
        except:
            source = ""

        # if we found the source button, and it's non-empty then only add to the return dict
        if source:
            return_dict["SourceCode"] = [source]

    # -------------------------------
    # Extracting Dataset
    # -------------------------------

    if tree.css("span:contains(Dataset)"):
        # in case parents don't exist, or the href attributes don't exist, an exception is raised
        try:
            data = tree.css("span:contains(Dataset)")[0].parent.attributes['href']
        except:
            data = ""

        # if we found the dataset button, and it's non-empty then only add to the return dict
        if data:
            return_dict["Dataset"] = [data]


    return return_dict

@retry(retry_on_exceptions=(RetryException,ConnectionError,HTTPError),max_calls_total=5,retry_window_after_first_call_in_seconds=30)
def scrape_AAAI_extract_meta(url):
    """
    Scarper for AAAI conference site built using OCS and OJS to extract metadata that are stored in meta tags.

    :param url: URl of he OCS/OJS built site
    :type url: string
    :return: A dictionary of pdf links and keywords, if avaiable
    """

    # was throwing sslerror, because sometimes doi redirects to the ip address of the host instead of aaai.org. Some pdf links will also have this IP address but it is accessible, but ssl is a problem.
    page=requests.get(url,verify=False,headers=requests_headers)
    tree=HTMLParser(page.text)

    return_dict={}

    #PDF link
    if tree.css("meta[name=citation_pdf_url]"):
        return_dict["PDF"]=[c.attributes.get('content',"") for c in tree.css("meta[name=citation_pdf_url]")]

    #Attributes
    if tree.css("meta[name=keywords]"):
        #Attributes are in string form and are separated by "; ", so we split to create an array
        return_dict["keywords"] = tree.css("meta[name=keywords]")[0].attributes.get('content', "").split(';')

    return return_dict

@retry(retry_on_exceptions=(RetryException,ConnectionError,HTTPError),max_calls_total=5,retry_window_after_first_call_in_seconds=30)
def scrape_AAAI(url):

    """
    SCRAPER FOR AAAI

    :param url: URL for the papers in the ACLWeb site. Taken from ee attribute in DBLP
    :return: Dictionary of  pdf links, keywords etc from AAAI based on if available.
    """

    return_dict={}


    #AAAI conference websites are either made by OCS(https://pkp.sfu.ca/ocs/) or OJS (https://pkp.sfu.ca/ojs/), we first figure out which is being used from the URL

    pattern=re.compile(r"https?:\/\/(?:www.)?aaai.org\/(\w*)\/")

    # check if the url is of AAAI and figure out if it is OCS or OJS
    match=re.search(pattern,url)

    if match:
        if match.group(1)=="ocs":
            #actual content of the website is inside a frame within the page
            page=requests.get(url,headers=requests_headers)
            tree=HTMLParser(page.text)
            if tree.css("frame"):

                #extract the source of the frame, which contains the meta tags we want
                data_dict=scrape_AAAI_extract_meta(tree.css('frame')[0].attributes['src'])
                #update to return dict with the new values we got
                return_dict.update(data_dict)
        elif match.group(1)=="ojs":

            #no need to extract src from frame in a ojs
            data_dict=scrape_AAAI_extract_meta(url)
            return_dict.update(data_dict)

    # not an AAAI url and most probably a doi. Doi urls are OJS so we donot need to extract the src from frame
    else:
        data_dict=scrape_AAAI_extract_meta(url)
        return_dict.update(data_dict)

    return return_dict


def ACM_string_formatter(string):
    """FORMATS STRINGS EXTRACTED FROM ACM FOR CITATIONS AND METRICS TO RETURN A INTEGER
    :param string: string from ACM website
    :type string: string
    :return: integer
    """
    #string placeholder
    r_s = ""

    #loop through the string and add it to placeholder if it's digit
    for a in string:
        r_s += a if a.isdigit() else ""

    #return as int
    if r_s:
        return int(r_s)

@retry(retry_on_exceptions=(RetryException,ConnectionError,HTTPError),max_calls_total=5,retry_window_after_first_call_in_seconds=30)
def scrape_ACM(url):
    """
    SCRAPER FROM ACM DIGITAL LIBRARY, FOR KDD AND EMNLP

    :param url: URL of the ACM site. From ee attribute of DBLP
    :type url: string

    :return: PDF link and citation in a dict, if available
    """


    reutrn_dict={}


    #get page and create a htmlparser object
    page=requests.get(url,headers=requests_headers)
    tree=HTMLParser(page.text)

    #CREATE PDF FROM DOI LINK
    root_url = "https://dl.acm.org/doi/pdf/"

    #extract doi id from the URL link
    doi_link=re.search(r"doi.org/(.*)",url)

    # if match exits
    if doi_link:
        reutrn_dict["PDF"]=root_url+doi_link.group(1)
    else:
        reutrn_dict["PDF"]=[]


    #Citations and download metrics
    cites={}
    if tree.css("span.citation"):
        #get formatted citation count
        c=ACM_string_formatter(tree.css("span.citation")[0].text())
        cites['citaitons']=c

    if tree.css("span.metric"):
        d=ACM_string_formatter(tree.css("span.metric")[0].text())
        cites["downloads"]=d

    # if we got some citation metrics
    if cites:
        reutrn_dict["ACM_Metrics"]=cites


    return reutrn_dict




if __name__=="__main__":

    """
    # -------------------------------------------------------------------------------------
    # CVPR
    # -------------------------------------------------------------------------------------

    for i in range(2010,2020):
        confs, total_matched = query_by_conference("conf/cvpr/{}".format(i),return_count=True)
        for conf in tqdm(confs,total=total_matched):
            if conf.get("ee",[]):
                update_dict={}
                update_dict["id"]=conf["_id"]
                for link in conf.get("ee"):
                    if "openaccess.thecvf.com" in link.lower():
                        update_dict.update(scrape_cvf(link))
                    if "doi.org" in link.lower():
                        update_dict.update(scrape_ieeexplore(link))
                update_collection_one(update_dict)
    """

    """
    #--------------------------------------------------------------------------
    # NIPS
    #--------------------------------------------------------------------------
    confs,total_matched=query_by_conference("conf/nips/2019",return_count=True)

    # list of dicts
    for conf in tqdm(confs,total=total_matched):
        if conf.get("ee",[]):
            scraped_dict=scrape_nips(conf['ee'][0])
            scraped_dict["id"]=conf['_id']
            update_collection_one(scraped_dict)

    """

    """

    #--------------------------------------------------------------------------
    # ICML
    #--------------------------------------------------------------------------
    for i in range(2010,2020):
    
        confs,total_matched=query_by_conference("conf/icml/{}".format(i),return_count=True)
    
        for conf in tqdm(confs,total=total_matched):
            if conf.get("ee",[]):
                scraped_dict=scrape_icml(conf['ee'][0])
                if scraped_dict:
                    scraped_dict["id"]=conf["_id"]
                    update_collection_one(scraped_dict)

    """

    """
    #--------------------------------------------------------------------------
    # ACL
    #--------------------------------------------------------------------------
    for i in range(2010,2020)"
        confs, total_matched = query_by_conference("conf/acl/{}".format(i), return_count=True)
    
        for conf in tqdm(confs, total=total_matched):
            if conf.get("ee", []):
                for link in conf.get("ee"):
                    if "aclweb.org" in link.lower():
                        scraped_dict = scrape_ACLWeb(link)
                        if scraped_dict:
                            scraped_dict["id"] = conf["_id"]
                            print(scraped_dict)
                            #update_collection_one(scraped_dict)
    
    """

    """"
    # --------------------------------------------------------------------------
    # AAAI
    # --------------------------------------------------------------------------

    for i in range(2010,2020):
        confs, total_matched = query_by_conference("conf/aaai/{}".format(i), return_count=True)
    
        for conf in tqdm(confs, total=total_matched):
            if conf.get("ee", []):
                for link in conf.get("ee"):
                    if "doi.org" in link.lower() or "aaai.org" in link.lower():
                        scraped_dict = scrape_AAAI(link)
                        if scraped_dict:
                            scraped_dict["id"] = conf["_id"]
                            print(scraped_dict)
                        # update_collection_one(scraped_dict)

    """



    """

    #--------------------------------------------------------------------------
    # EMNLP
    #--------------------------------------------------------------------------
    for i in range(2010,2021):
        confs, total_matched = query_by_conference("conf/emnlp/{}".format(i), return_count=True)
        for conf in tqdm(confs, total=total_matched):
            if conf.get("ee", []):
                for link in conf.get("ee"):
                    if "aclweb.org" in link.lower() or "doi.org" in link.lower():
                        scraped_dict = scrape_ACLWeb(link)
                        if scraped_dict:
                            scraped_dict["id"] = conf["_id"]
                            print(scraped_dict)
                            #update_collection_one(scraped_dict)


    """

    """

    #--------------------------------------------------------------------------
    # KDD
    #--------------------------------------------------------------------------
    for i in range(2010,2020):
        confs, total_matched = query_by_conference("conf/kdd/{}".format(i), return_count=True)

        for conf in tqdm(confs, total=total_matched):
            if conf.get("ee", []):
                for link in conf.get("ee"):
                    if "acm.org" in link.lower() or "doi.org" in link.lower():
                        scraped_dict = scrape_ACM(link)
                        if scraped_dict:
                            scraped_dict["id"] = conf["_id"]
                            # print(scraped_dict)
                            update_collection_one(scraped_dict)
                    elif "proceedings.mlr.press" in link.lower():
                        scraped_dict=scrape_icml(link)
                        if scraped_dict:
                            scraped_dict["id"] = conf["_id"]
                            update_collection_one(scraped_dict)


    """

    """

    #--------------------------------------------------------------------------
    # CHI
    #--------------------------------------------------------------------------
    for i in range(2010,2021):
        confs, total_matched = query_by_conference("conf/chi/{}".format(i), return_count=True)

        for conf in tqdm(confs, total=total_matched):
           if conf.get("ee", []):
               for link in conf.get("ee"):
                   if "acm.org" in link or "doi.org" in link:
                       scraped_dict = scrape_ACM(link)
                       if scraped_dict:
                           scraped_dict["id"] = conf["_id"]
                           print(scraped_dict)
                           #update_collection_one(scraped_dict)

    """



    #-------------------------------------------------------------------------------------------------------
    # For documents that did not match the PDF scraping by conferences, check if the ee contains a pdf link
    #--------------------------------------------------------------------------------------------------------

    # get the documents where the PDF does not exist
    docs,total_matched=query_by_field_exists('PDF',False,True)

    for doc in tqdm(docs,total=total_matched):
        if doc.get("ee",[]):
            for link in doc.get("ee"):

                if link[-3:].lower()=="pdf":
                    update_dict={}
                    update_dict["id"]=doc["_id"]
                    update_dict["PDF"]=[link]
                    update_collection_one(update_dict)