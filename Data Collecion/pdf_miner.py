from database_operations import query_by_field_exists, update_collection_one, update_collection_many,query_by_conference, get_collection
from tqdm import tqdm, trange
from pdfminer.high_level import extract_text
import requests
import re
from requests.exceptions import ConnectionError,HTTPError
from opnieuw import RetryException, retry
from  concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from functools import partial
from miniutils import parallel_progbar


@retry(retry_on_exceptions=(RetryException,ConnectionError,HTTPError),max_calls_total=5,retry_window_after_first_call_in_seconds=30)
def get_pdf(url,filename="file.pdf"):
    """
    SAVE PDF FILE FROM THE URL WITH REQUESTS AND SAVE IT AS
    :param url: URL of the pdf.
    :type url: String
    :param filename: Name of the file being saved
    :type filename: String
    :return: path of the saved file in string
    """

    # load the file from requests
    response=requests.get(url)

    # save it by reading the content of the pdf file as a binary
    with open(filename,"wb") as f:
        f.write(response.content)


    return filename


def read_pdf(document,pattern,collection):
    """
    READ A PDF FILE SEARCH FOR LINKS USING REGEX, UPDATE MONGODB
    :param document: The Document we are updating
    :type document: Dictionary
    :param pattern: Compiled regex pattern object for string searches
    :type pattern: re.Pattern object
    :param collection: MongoDB collection object
    :type collection: MongoDB.Collection
    """
    #print(document)
    pdf_list=document.get("PDF", [])

    if not type(pdf_list)==list:
        pdf_list=[pdf_list]


    # Can be multiple PDF links, if so go through all of them
    for link in pdf_list:
        # get the pdf file and extract text form the file using pdfminer
        pdf_filename = get_pdf(link,filename="PDFs/{}.pdf".format(str(document['_id'])))
        # pdfminer to get the pdf in string form
        text = extract_text(pdf_filename)

        # get all link matches by using regex. Prevent duplication by converting to set
        matches_list = list(set(re.findall(pattern, text)))

        # Create an update dict
        update_dict = {"PDF_SourceCode": matches_list}
        update_dict["id"] = document["_id"]

        # update the file
        #update_collection_one(update_dict, collection=collection)
        print(update_dict)

    # for updating progress bar
    return 1



def extract_sourcecode(process_documents):
    """
    MINE GITHUB/GITLAB/BITBUCKET/SOURCEFORGE LINKS FROM PDF FILES. GOES THROUGH DOCUMENTS THAT HAVE A PDF ATTRIBUTE AND IT CONTAINS AN ELEMENT
    :param process_documents: documents to update with this function
    :type process_documents: Tuple of form (proces_n,list of documents)
    """

    documents=process_documents[1]

    #connection update to make update faster
    connection=get_collection()

    # regex pattern to check for open source links.
    pattern = re.compile(r"((?:(?:http|https):\/\/)?(?:www.)?(?:github|gitlab|bitbucket|sourceforge)\.[a-z]{2,6}\b(?:[-a-zA-Z0-9@:%_\+~#?&\/\\=]*))")

    #since pattern and connection are the same for all threads
    func=partial(read_pdf,pattern=pattern,collection=connection)

    with tqdm(total=len(documents),position=process_documents[0]) as pbar:
        #create threads and map them with the available documents
        with ThreadPoolExecutor(max_workers=8) as exectuor:
            results={exectuor.submit(func,doc): doc for doc in documents}
            ''''
            for future in as_completed(results):
                pbar.update(1)
                '''
    return 1

def create_chunk(list,n):
    """
    CREATE A Iterator iterating n-sized chunk from the list

    :param list: List to chunk
    :param n: Size of each chunk
    :type n: Integer
    """

    for i in range(0, len(list), n):
        yield (i//n,list[i:i + n])

if __name__=="__main__":

    docs=[]
    #c=query_by_field_exists("PDF")
    c=query_by_conference("conf/chi")
    for doc in c:
        # check if this document has been scraped before,  if so the dictionary has a PDF_SourceCode element
        if doc.get("PDF_SourceCode",1)==1:
            if doc.get("PDF"):
                document={}
                document["_id"]=doc["_id"]
                document["PDF"]=doc["PDF"]
                docs.append(document)


    print(len(docs))

    with ProcessPoolExecutor(8) as executor:
        results=executor.map(extract_sourcecode,list(create_chunk(docs,1000)))

    """
    
    print(len(docs))
    conn=get_collection()
    pattern = re.compile(
        r"((?:(?:http|https):\/\/)?(?:www.)?(?:github|gitlab|bitbucket|sourceforge)\.[a-z]{2,6}\b(?:[-a-zA-Z0-9@:%_\+~#?&\/\\=]*))")

    extract_sourcecode(docs)
    """