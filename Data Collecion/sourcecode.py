from github import Github
import re
import gitlab
from database_operations import  check_repo_exists, get_collection, insert_document,query_by_field_exists,update_collection_one
from tqdm import tqdm
import time

sourcecode_pattern=re.compile(r"(?:(?:http|https):\/\/)?(?:www.)?(github|gitlab)\.[a-z]{2,6}(?:\/([-a-zA-Z0-9@:%_\+~#?&\/\\=]*))")

github_pattern=re.compile(r"(?:(?:http|https):\/\/)?(?:www.)?(?:github)\.[a-z]{2,6}(?:\/([-a-zA-Z0-9@:%_\+~#?&\/\\=]*))")
gitlab_pattern=re.compile(r"(?:(?:http|https):\/\/)?(?:www.)?(?:gitlab)\.[a-z]{2,6}(?:\/([-a-zA-Z0-9@:%_\+~#?&\/\\=]*))")
bitbucket_pattern=re.compile(r"(?:(?:http|https):\/\/)?(?:www.)?(?:bitbucket)\.[a-z]{2,6}(?:\/([-a-zA-Z0-9@:%_\+~#?&\/\\=]*))")

##regex code for extracting the id in case the link contains branch details
id_pattern=re.compile(r"^([-a-zA-Z0-9@:%_\+~#?&=]*\/[-a-zA-Z0-9@:%_\+~#?&=]*)")


github=Github("API_KEY")
gl=gitlab.Gitlab("https://gitlab.com/", private_token="API_KEY")


def github_scraper(id):
    """
    Check if this repo is in the repository document, if not conduct the scraping and add it
    :param id: Key of the repo in the form of author_id/repo_id
    :type id: String
    :return: Objectid of the record for this repo in the repository document
    """

    #check rate limits and sleep untill this resets
    while github.rate_limiting[0]<1:
        time.sleep(20)

    #check if this repo has been scraped before by getting objectid from repo collection
    object_id=check_repo_exists(f"github/{id}")

    if object_id:
        return object_id
    else:
        repo_dict={}
        #initialize the repo for scraping
        repo = github.get_repo(id)

        #adding elementary stats to the return dict
        repo_dict["key"]=f"github/{id}"
        repo_dict["subscribers"]=repo.subscribers_count
        repo_dict["stars"]=repo.stargazers_count
        repo_dict["forks"]=repo.forks_count
        repo_dict["languages"]=repo.get_languages()
        repo_dict["size"]=repo.size

        # if there are issues
        if repo.has_issues:
            repo_dict["OpenIssues"]=repo.open_issues_count
        else:
            repo_dict["OpenIssues"]=0

        ## Scraping commits (new additions/deletions) on a week by week basis
        commits=repo.get_stats_code_frequency()
        commits_list=[]

        for commit in commits:
            week={}
            week["week"]=commit.week
            week["additions"]=commit.additions
            week["deletions"]=commit.deletions

            # for space efficiency, we will not add a week if there are no deletions or additions; i.e. a week not existing implies no commit activity happened in the repo
            if week["additions"] or week["deletions"]:
                commits_list.append(week)

        repo_dict["commits"]=commits_list

        ##getting contribution by users (github caps this to 100 users)
        contributors=repo.get_stats_contributors()
        contributors_list=[]

        for contrib in contributors:
            authors={}
            authors["id"]=contrib.author.login
            authors["contributions"]=contrib.total
            contributors_list.append(authors)

        repo_dict["contributors"]=contributors_list
        repo_dict["count"]=1

        # get collection and insert the document into the repositories collection, and return the id of the inserted entry
        coll=get_collection(collection_name="repository")

        return insert_document(repo_dict,coll)


def gitlab_scraper(id):
    """
    Check if the gitlab repo in this repository exists as a document, if not do the scraping and add it
    :param id: Key of the repo in the form of author_id/repo_id
    :type id: String
    :return: Objectid of the record for this repo in the repository document
    """

    #check if this repo has been scraped before by getting objectid from repo collection
    object_id=check_repo_exists(f"gitlab/{id}")

    if object_id:
        return object_id
    else:
        repo_dict={}

        repo=gl.projects.get(id)

        repo_dict["key"]=f"gitlab/{id}"
        repo_dict["forks"]=repo.attributes.get("forks_count",0)
        repo_dict["stars"]=repo.attributes.get("star_count",0)
        repo_dict["created"]=repo.attributes.get("created_at")
        repo_dict["lastactive"]=repo.attributes.get("last_activity_at")
        repo_dict["OpenIssues"]=repo.attributes.get("open_issues_count")

        try:
            issues = repo.issuesstatistics.get()
            repo_dict["ClosedIssues"]=issues.attributes['statistics']['counts']['closed']
        except KeyError:
            pass

        repo_dict["languages"]=repo.languages()

        repo_dict["contributors"]=repo.repository_contributors(all=True)

        #list of commits
        cmt_list=[]

        commits=repo.commits.list()
        for commit in commits:
            cmt_dict={}
            #getting more details including line additions and deleltions
            cmt=repo.commits.get(commit.id)
            cmt_dict["created_at"]=cmt.attributes.get("created_at")
            cmt_dict["stats"]=cmt.attributes.get("stats")
            cmt_list.append(cmt_dict)

        repo_dict["commits"]=cmt_list
        repo_dict["count"] = 1

    # get collection and insert the document into the repositories collection, and return the id of the inserted entry
    coll = get_collection(collection_name="repository")

    return insert_document(repo_dict, coll)




def scraper_sourcecode(urls):
    """
    RECOGNIZES THE SOURCE OF THE REPOSITORY (GITHUB/GITLAB) AND THEN CALLS THE APPROPRIATE SCRAPER.
    :param urls: url of the sourcecode
    :type urls: list of sourcecode links can be any of the three types
    :return: list of OjbectIds of repos in the Repository table associated with this paper.
    """

    return_list=[]

    for url in urls:
        # checking if the link is a github link
        match=re.search(sourcecode_pattern,url)

        # we have one of the three sites that we have an scraper api for
        if match and match.group(1)=="github":
            # get the id of the repo in the form author_id/repo_id, scrape if not scraped before and add an id of the document in repository collection to the entry in the papers collection.
            try:
                id=re.search(id_pattern,match.group(2))
                scraped_repo_id=github_scraper(id.group(1))
                return_list.append({"key":id.group(1),"id":scraped_repo_id})
            # a problem in getting the repo, or invalid id simply return the empty list with no scraped data
            except:
                return_list.append({"link":url})

        elif match and match.group(1)=="gitlab":
            try:
                id=re.search(id_pattern,match.group(2))
                scraped_repo_id=gitlab_scraper(id.group(1))
                return_list.append({"key":id.group(1),"id":scraped_repo_id})
            # a problem in getting the repo, or invalid id simply return the empty list with no scraped data
            except:
                return_list.append({"link":url})

        else:
            # not any links list add an empty dict
            return_list.append({})

    return return_list

if __name__=="__main__":

    """ INITIAL SCRAPING// COUNTS WERE NOT COMPLETED
    c,count=query_by_field_exists("PDF_SourceCode",return_count=True)
    for doc in tqdm(c,total=count):
        if doc.get("PDF_SourceCode") and doc.get("Repository",9)==9:
            update_dict={}
            update_dict["id"]=doc.get("_id")
            update_dict["Repository"]=scraper_sourcecode(doc.get("PDF_SourceCode"))
            update_collection_one(update_dict)
            """

    """GETTING THE COUNTS+CLEANUP OF THE DATASET"""

    c,count=query_by_field_exists("Repository",return_count=True)
    for doc in tqdm(c,total=count):
        if not doc.get('Finished'):
            update_dict={}
            update_dict["id"]=doc.get("_id")
            update_dict["Finished"]=True
            repo_list=[]
            for repo in doc["Repository"]:
                if repo:
                    if repo.get("key"):
                        if not check_repo_exists(repo_key=None, repo_id=repo.get("id")):
                            print(f"No-repo found for key {repo.get('key')}")
                        repo_list.append(repo)


                    elif repo.get("link"):
                        match = re.search(sourcecode_pattern, repo.get("link"))
                        if match and match.group(1) == "github":
                            try:
                                id = re.search(id_pattern, match.group(2))
                                scraped_repo_id = check_repo_exists(f"github/{id.group(1)}")
                                if scraped_repo_id:
                                    repo_list.append({"key":id.group(1),"id":scraped_repo_id})
                                else:
                                    repo_list.append(repo)
                            except:
                                repo_list.append(repo)

                        elif match and match.group(1) == "gitlab":
                            try:
                                id = re.search(id_pattern, match.group(2))
                                scraped_repo_id = gitlab_scraper(f"gitlab/{id.group(1)}")
                                if scraped_repo_id:
                                    repo_list.append({"key": id.group(1), "id": scraped_repo_id})
                                else:
                                    repo_list.append(repo)
                            except:
                                repo_list.append(repo)

            update_dict['Repository']=repo_list
            update_collection_one(update_dict)





