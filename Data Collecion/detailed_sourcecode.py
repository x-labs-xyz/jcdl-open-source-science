"""Scraping week-by-week commit data from Github repositories including commit stats and countributor stats. Potentiallly add stars/forks/subscribers"""
from github import Github
from database_operations import  check_repo_exists, get_collection, insert_document,query_by_field_exists,update_collection_one
import time
from tqdm import tqdm
from opnieuw import retry
from requests.exceptions import  ReadTimeout
github=Github("API_KEY")

@retry(retry_on_exceptions=ReadTimeout,max_calls_total=10,retry_window_after_first_call_in_seconds=60)
def get_commit(comm):
    """ GET DETAILS ABOUT AN INDIVIDUAL COMMIT
    :param commit_object: Pygithub commit object
    :return : Dict containing commit details
    """

    commit_dict = {}
    commit_dict["sha"] = comm.sha
    commit_dict["Date"] = comm.commit.author.date
    commit_dict["Total"] = comm.stats.total

    # In case the commit is done by an author without a github login, we will just use their display name,email combination
    try:
        commit_dict["Author"] = comm.author.login
    except AttributeError:
        commit_dict["Author"] = comm.commit.author.name + " " + comm.commit.author.email

    return commit_dict


def scraper(repo_key):
    """GET A LIST OF COMMITS FOR A PARTICULAR REPOSITORY
    :param repo_key : Github key of the repository
    :type repo_key: String
    """

    # get the repostiory id used by the github package from the database
    splits=repo_key.split("/")
    key=splits[1]+"/"+splits[2]


    repo=github.get_repo(key)
    commits=repo.get_commits()

    return_list=[]
    # start scraping only if commits is less than 10000 (for the initial commit phase)
    if commits.totalCount<10000:
        # loop through all the commits
        for comm in tqdm(commits,total=commits.totalCount,position=1):
            commit_dict=get_commit(comm)

            return_list.append(commit_dict)

            # check rate limits and sleep untill this resets
            while github.get_rate_limit().core.remaining<2:
                time.sleep(20)

    return return_list

def stargazer_to_dict(stargazer):
    """
    CONVERT A STARGAZER OBJECT
    :param stargazer: pygithub stargazer object
    :return: Dict representing a starring event
    """

    #Dictionary to return
    star_dict={}

    star_dict["user"]=stargazer.user
    star_dict["time"]=stargazer.starred_at

    return star_dict

def stars_scraper(repo_key):
    """
    GET A LIST OF STARGAZE EVENTS FOR A PARTICULAR REPOSITORY
    :param repo_key: Github key for the repository
    :type repo_key: String
    :return:
    """
    # get the repostiory id used by the github package from the database
    splits=repo_key.split("/")
    key=splits[1]+"/"+splits[2]

    repo=github.get_repo(key)
    stars=[]

    if repo.stargazers_count<100000:
        stars_list=repo.get_stargazers_with_dates()
        for star_event in stars_list:
            star_dict=stargazer_to_dict(star_event)
            stars.append(star_dict)

            while github.get_rate_limit().core.remaining<10:
                time.sleep(60)

    return stars

if __name__=="__main__":
    repo_db=get_collection(collection_name="repository")
    cursor=list(repo_db.find())

    """
    for doc in tqdm(cursor,position=0):
        if doc.get("Detailed_commits",9)==9:
            update_dict={}
            if doc['key'].split("/")[0]=="github":
                commits=scraper(doc['key'])
            else:
                commits=[]

            update_dict["Detailed_commits"]=commits
            update_dict["id"]=doc.get("_id")
            update_collection_one(update_dict,collection=repo_db)
    """

    for doc in tqdm(cursor,position=0):
        if doc.get('Stars_events',9)==9:
            update_dict={}
            if doc['key'].split("/")[0]=="github":
                stars=stars_scraper(doc['key'])
            else:
                stars=[]

            update_dict["Stars_events"]=stars
            update_dict["id"]=doc.get("_id")
            update_collection_one(update_dict,collection=repo_db)