import base64
import http.client
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from ratelimiter import RateLimiter
from tqdm import tqdm
import dateutil.parser
import semanticscholar as sch
from database_operations import query_by_field_exists, query_by_conference, update_collection_one
from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch
from serpapi import GoogleScholarSearch
import sys

scopus_client=ElsClient("API KEY")

@RateLimiter(max_calls=4,period=3)
def scrape_google_scholar(title,year):
    """
    SCRAPE CITATION DETAILS FROM GOOGLE SCHOLAR
    :param title:Title of the paper
    :type title: String
    :param year: Year at which the paper is published
    :type year: String
    :return:
    """

    # parameters to feed to the scraper api
    params = {
        "engine": "google_scholar",
        "q": f"{title}",
        "api_key": "API_KEY",
        "as_ylo": str(int(year)-1),
        "as_yhi": str(int(year)+1)
    }

    #initiate the client and get the results as a dictionary
    client = GoogleScholarSearch(params)
    try:
        results=client.get_dict()
    except:
        print("results not found")
        results=[]

    # result contains a lot of redundant data so filtering it
    filter_keys=["title","result_id","link","publication_info","inline_links"]



    results_list=[]

    for result in results.get('organic_results',[]):
        #go through the keys we want to filter and add them to this new dict if they're available
        result_dict = {}

        for key in filter_keys:
            if result.get(key,[]):
                result_dict[key]=result[key]
        results_list.append(result_dict)

    return results_list



@RateLimiter(max_calls=9,period=1)
def scrape_scopus(title,year):
    """
    RATE LIMITS OF 20,000/ 7 days
    SCRAPE CITATION DETAILS FROM SCOPUS SEARCH. SEARCH CRITERIA REQUIRES MATCH WITH TITLE+YEAR
    :param title: Title of the paper
    :type title: String
    :param year: Year of publication
    :type year: String
    :return: Dictionary of releveant scopus details
    """

    #list of matches that matched our criteria
    results=[]

    # generate a search object
    search=ElsSearch(f"TITLE({title})","scopus")

    #do the search
    search.execute(scopus_client)

    #acceptable dates within one year
    date_list=[int(year)-1,int(year),int(year)+1]

    # go through the results and check if date matches
    for result in search.results:
        # if disaplydate exists


        #boolean that represents any discrepencies in date between the scopus paper and our paper
        Date_checker=True

        if result.get('prism:coverDisplayDate',[]) or result.get("prism:coverDate",[]):
            if result.get('prism:coverDisplayDate',[]):
                try:
                    dt=dateutil.parser.parse(result.get('prism:coverDisplayDate'))
                except:
                    dt=0
                # use AND for boolean checks, so that if any of the checks fail, our match is invalid
                Date_checker= Date_checker & (dt.year in date_list)
            if result.get("prism:coverDate",[]):
                try:
                    dt=dateutil.parser.parse(result.get('prism:coverDate'))
                except:
                    dt=0
                Date_checker= Date_checker & ( dt.year in date_list)

        else:
            # if none of the date field exists, we cannot compare by date so we skip.
            Date_checker=False

        # if date condition matches
        if Date_checker:
            results.append(result)


    return results

def scrape_semanticscholar(title,year,url):
    """
       SCRAPE CITATION DETAILS FROM SEMANTIC SCHOLAR SEARCH. SEARCH CRITERIA REQUIRES MATCH WITH TITLE+YEAR
       :param title: Title of the paper
       :type title: String
       :param year: Year of publication
       :type year: String
       :param url: URL from which the pdf was extracted, ee value of DBLP to get doi id
       :type url: list
       :return: Dictionary of releveant semantic scholar details
       """
    doi=""
    # go through links, since there's only going to be one doi link
    for link in url:
        doi_link = re.search(r"doi.org/(.*)", link)

        #if doi url is found
        if doi_link:
            doi=doi_link.group(1)

    #Semantic scholar api only valid for links, so only return if any valid link was found
    if doi:
        return semantic_api(doi,year)

@RateLimiter(max_calls=1, period=3)
def semantic_api(doi,year):
    """
    MAKE CALLS TO THE SEMANTIC SCHOLAR API AND THROTTLE THE API CALLS TO 100 PER 5 MINUTES
    :param doi: DOI id of the paper
    :type doi: string
    :param year: Year of publication
    :type year: string
    :return: semantic api return  call
    """
    #acceptable dates within one year
    date_list=[int(year)-1,int(year),int(year)+1]

    try:
        paper=sch.paper(doi)

    except:
        return []
    #check if paper matches our date criteria (THIS IS MORE OF A SANITY CHECK THAN A REQUIREMENT SINCE WE'RE NOT DOING ANY SEARCHING, HENCE THE RAISED EXCEPTION)
    if paper:
        # if paper object has a year has a value
        if paper.get("year",0):
            # years matched, can return the dict
            if int(paper.get("year")) in date_list:
                return paper
            else:
                return []


@RateLimiter(max_calls=1,period=2)
def scrape_microsoft(title,year):
    """
    SCRAPE CITAITON COUNT FROM MICROSOFT ACADEMIC
    :param title: title of the paper
    :type title: string
    :param year: Year in which it was published
    :type year: string
    :return:
    """
    # acceptable dates within one year
    date_list = [int(year) - 1, int(year), int(year) + 1]

    #Authentication
    headers = {
        # Request headers
        'Ocp-Apim-Subscription-Key': 'API_KEY',
    }

    params = urllib.parse.urlencode({
        #Must encode it to work with the ti value from academic
        'expr':f"Ti='{encode_title_microsoft(title)}'",
        'model': 'latest',
        'attributes': 'CC,Y,DOI,Id',
        'count': '10',
        'offset': '0',
    })

    try:
        #establish connection
        conn = http.client.HTTPSConnection('api.labs.cognitive.microsoft.com')
        conn.request("GET", "/academic/v1.0/evaluate?%s" % params, "{body}", headers)
        response = conn.getresponse()
        #making a json file of the returned bytes
        data = json.loads(response.read())
        if data.get('error'):
            print(data)
            raise OSError("403 error")
        conn.close()
        #go through the 10 results returned by the API

        # list of entities that match the condition required (we save every one and can filter later on)
        return_entities=[]
        for entity in data['entities']:
            # check if dates match, and include all entries for which it does
            if int(entity.get('Y')) in date_list:
                return_entities.append(entity)

        return return_entities

    except Exception as e:
        if isinstance(e,OSError):
            if e.strerror=="403 error":
                raise
            print("[Errno {0}] {1}".format(e.errno, e.strerror))


def encode_title_microsoft(title):

    """
    MICROSOFT ACADEMIC TITLES ARE LOWERCASE AND DON'T HAVE A FULL STOP AT THE END. THIS FUNCTION ENCODES THE TITLE IN SUCH A WAY
    :param title: title of the paper
    :type title: string
    :return: properly formatted version of the title
    """


    title=title.lower()
    #removes aprostrophe cuz microsoft academic will crash with this.
    title=title.replace("'","")
    if title[-1] == ".":
        title=title[:-1]
    return str(title)



if __name__=="__main__":


    c,count=query_by_field_exists("PDF",return_count=True)

    if sys.argv[1]=="2":

        print("Microsoft")
        microsoft_list=list(c)
        for i, doc in tqdm(enumerate(microsoft_list),total=count):
            if doc.get("MSAcademic_cites",9)==9:
                if doc.get("title"):
                    update_dict={}
                    update_dict["id"] = doc["_id"]
                    res=scrape_microsoft(doc.get('title')[0],doc.get("year")[0])
                    update_dict['MSAcademic_cites']=res
                    update_collection_one(update_dict)

    """
    print("Scopus")
    for i, doc in tqdm(enumerate(c),total=count):
        if doc.get("title"):
            update_dict={}
            update_dict["id"] = doc["_id"]
            res=scrape_scopus(doc.get('title')[0],doc.get("year")[0])
            update_dict['Scopus_cites']=res
            update_collection_one(update_dict)
    """

    if sys.argv[1]=="3":
        semantic_list=list(c)
        print("Semantic")
        for i, doc in tqdm(enumerate(semantic_list),total=count):
            if doc.get("SemanticScholar_cites",9)==9:
                if doc.get("ee"):
                    update_dict = {}
                    update_dict["id"] = doc["_id"]
                    res = scrape_semanticscholar("title", doc.get("year")[0],doc.get('ee'))
                    update_dict['SemanticScholar_cites'] = res
                    update_collection_one(update_dict)

    if sys.argv[1]=="1":
        print("google")
        for i, doc in enumerate(c):
            if doc.get("Scholar_cites",9)==9:
                if doc.get("title"):
                    update_dict = {}
                    update_dict["id"] = doc["_id"]
                    res = scrape_google_scholar(doc.get('title')[0], doc.get("year")[0])
                    update_dict['Scholar_cites'] = res
                    update_collection_one(update_dict)
                if i>29000:
                    break