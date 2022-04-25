"""THIS MODULE INCLUDES THE CODE TO PARSE THROUGH THE RAW DATABASE AND EXPORT THE EXPORT THE DATA ENTRIES AS A ASCII (CSV/JSON) FILE, OR AS A PYTHON GENERATOR"""
from database_operations import  get_collection,query_by_field_exists,query_repo, query_by_conference, update_collection_one, repo_query_unique
import csv
from tqdm import tqdm
import numpy as np
import json
import datetime
from copy import deepcopy


def parse_contibutors(repository):
    """
    PARSE THE CONTRIBUTORS (AUTHORS) OF A REPOSITORY AND RETURN SIMPLIFIED FEATURES. THE RETUNRED FEATURES ARE HOW MANY CONTRIBUTORS ARE THERE AND THE VARIANCE OF THE PERCENTAGE OF CONTRIBUTIONS AMONGST THE CONTRIBUTORS
    :param repository: repository document from the database
    """


    contribs=repository.get("contributors")

    contributor_commits=[]
    for contributor in contribs:
        #Gitlab. Commits can be zero, so we cannot use a simple get boolean comparison
        if not contributor.get("commits",9)==9:
            contributor_commits.append(contributor.get("commits"))
        elif not contributor.get("contributions",9)==9:
            contributor_commits.append(contributor.get("contributions"))


    if contributor_commits:
        n_contributors=len(contributor_commits)
        var=np.std(np.array(contributor_commits))

        return (n_contributors,var)

    else:
        return (None,None)

def parse_languages(repository):
    """
    PARSES THROUGH THE AVAILABLE LANGUAGES PRESENT IN A REPOSITORY.
    :param repository: Repository Object
    :return: Tuple in the form of (Number of languages, String representaion of the languages, Name of the largest repository language, standard deviation of the percentages of languages used)
    """
    languages_dict=repository.get("languages",[])

    languages_len=len(languages_dict)

    #String representation of the languages included in repo
    languages=""

    # Value of the largest repository language
    language_max=0

    # Name of the largest repository language
    best_lang=""

    #List of language values
    language_values=[]


    for language,value in languages_dict.items():
        languages+=language+","
        if language_max < value:
            best_lang=language
            language_max=value

        language_values.append(value)

    language_std=np.std(np.array(language_values))

    return (languages_len,languages, best_lang,language_std)





def parse_repo_commit_activity(repository,conf_date=None):
    commits_list=repository.get("commits")

    before_commits=[]
    after_commits=[]
    weekly_commits={}
    try:
        #Github repo
        if commits_list[0].get("week"):
            start_commit=commits_list[0]['week']
            last_commit=commits_list[-1]['week']
            activity=[]
            for commit in commits_list:
                if conf_date:
                    week_index=(commit["week"]-conf_date).days //7

                    weekly_commits_lst=weekly_commits.get(week_index,[])
                    weekly_commits_lst.append(commit['additions']-commit['deletions'])
                    weekly_commits[week_index] = weekly_commits_lst

                    if commit['week']<conf_date:
                        before_commits.append(commit['additions']-commit['deletions'])
                    else:
                        after_commits.append(commit['additions']-commit['deletions'])

                activity.append(commit['additions']-commit['deletions'])

        #Gitlab To fix by binning by week here
        elif commits_list[0].get("stats"):
            start_commit=commits_list[0]['created_at']
            last_commit=commits_list[-1]['created_at']
            activity=[]
            for commit in commits_list:

                if conf_date:
                    week_index = (commit["created_at"] - conf_date).days // 7

                    weekly_commit_lst= weekly_commits.get(week_index, [])
                    weekly_commit_lst.append(commit['stats']['total'])
                    weekly_commits[week_index]=weekly_commit_lst

                    if commit['created_at'] < conf_date:
                        before_commits.append(commit['stats']['total'])
                    else:
                        after_commits.append(commit['stats']['total'])
                activity.append(commit['stats']['total'])
    except TypeError:
        if conf_date:
            return (None,None,None,None,None)
        else:
            return (None,None)
    try:
        active_date=last_commit-start_commit
        commit_week=np.sum(np.array(activity))/len(activity)
        if conf_date:
            weekly_commit_summed=[]
            for k,v in weekly_commits.items():
                weekly_commit_summed.append((k,np.sum(np.array(v))))
            before_commit_sum=np.sum(np.array(before_commits))
            after_commit_sum=np.sum(np.array(after_commits))
    except IndexError:
        if conf_date:
            return (None,None,None,None,None)
        else:
            return (None,None)


    if not conf_date:
        return (active_date,commit_week)
    else:
        return (active_date,commit_week,before_commit_sum,after_commit_sum,weekly_commit_summed)



def parse_detailed_commits(repository,time_scale,mean_date,variable="count", cum=False):
    """
    PARSE THROUGH THE DETAILED COMMIT LIST AND GENERATE A TIME SERIES
    :param repository: Repository Object
    :type repository: Dict
    :param time_scale: Number of days used as a single time scale unit for generating time series
    :type time_scale: Int
    :param mean_date: Date at which the time series will be 0, most commonly the date at which the publication is done
    :type mean_date: Datetime.datetime object
    :param variable: Name of variable in the repository dict to look for in a specific time interval. The variable type must be summable (int/float)
    :type variable: String. The default count is for simply counting if a commit was done in a particular interval.
    :param cum: Boolean indicator of whether or not the series should be cumulative
    :type cum: Boolean
    :return: List of tuples repersenting the time series. If cum=True, returns tuples of two series the first being the non-cumulative and the second the cumulative.
    """


    commits = repository.get("Detailed_commits",[])

    comm_dict={}
    for commit in commits:
        days_diff=(commit['Date']-mean_date).days//time_scale

        val=comm_dict.get(days_diff,0)
        if variable=="count":
            val+=1
        else:
            val+=commit[variable]

        comm_dict[days_diff]=val


    comm_list=sorted([(k, v) for k,v in comm_dict.items()],key=lambda x:x[0])


    if cum:
        cum_list = []
        total=0
        for t_scale in comm_list:
            total+=t_scale[1]
            cum_list.append((t_scale[0],total))

        return comm_list,cum_list

    return comm_list

def parse_detailed_stars(repository,time_scale,mean_date,variable="count", cum=False):
    """
    PARSE THROUGH THE DETAILED STAR LIST AND GENERATE A TIME SERIES
    :param repository: Repository Object
    :type repository: Dict
    :param time_scale: Number of days used as a single time scale unit for generating time series
    :type time_scale: Int
    :param mean_date: Date at which the time series will be 0, most commonly the date at which the publication is done
    :type mean_date: Datetime.datetime object
    :param variable: Name of variable in the repository dict to look for in a specific time interval. The variable type must be summable (int/float)
    :type variable: String. The default count is for simply counting if a commit was done in a particular interval.
    :param cum: Boolean indicator of whether or not the series should be cumulative
    :type cum: Boolean
    :return: List of tuples repersenting the time series. If cum=True, returns tuples of two series the first being the non-cumulative and the second the cumulative.
    """


    commits = repository.get("Stars_events",[])

    comm_dict={}
    for commit in commits:
        try:
            days_diff=(commit['time']-mean_date).days//time_scale
        except:
            print(commit)
            raise
        val=comm_dict.get(days_diff,0)
        if variable=="count":
            val+=1
        else:
            val+=commit[variable]

        comm_dict[days_diff]=val


    comm_list=sorted([(k, v) for k,v in comm_dict.items()],key=lambda x:x[0])


    if cum:
        cum_list = []
        total=0
        for t_scale in comm_list:
            total+=t_scale[1]
            cum_list.append((t_scale[0],total))

        return comm_list,cum_list

    return comm_list


def data_generator(paper_rows,repository_rows):

    repo_collection=get_collection(collection_name="repository")

    c,count=query_by_field_exists("Scholar_cites",return_count=True,empty=False)

    for paper in c:
        return_dict={}
        for row in paper_rows:
            if row=="author_len":
                return_dict["author_len"]=len(paper.get("author",[]))
            elif row=="authors":
                return_dict["authors"]="/".join(paper.get("author",[]))
            else:
                try:
                    return_dict[row]=paper.get(row)[0]
                except TypeError:
                    return_dict[row]=paper.get(row)


        return_dict["scholar_cites"]=paper["Scholar_cites"][0].get("inline_links",{}).get("cited_by",{}).get("total")


        #Boolean indicating whether the paper has returned at least one repository
        repo_returned=False
        for i,row in enumerate(paper.get("Repository",[])):
            #Check if this is a valid repository with an id (exclude cases on unscraped github repos and sourceforgenet links)
            if row.get("id"):
                repo=repo_query_unique(row['id'],paper["_id"],collection=repo_collection)

                if repo:
                    repo_dict = deepcopy(return_dict)
                    for repo_row in repository_rows:
                        #exclude adding cases where the repository document does not have the rows we need into the return dict

                        if repo_row=="commit_stats":
                            if "conf_date" in paper_rows and type(return_dict['conf_date'])!=list:
                                activity_range,commit_rate,before_publication,after_publication,weekly_commit=parse_repo_commit_activity(repo,return_dict['conf_date'])
                                repo_dict["commmit_before_publication"]=before_publication
                                repo_dict["commit_after_publication"]=after_publication
                                repo_dict["commits_weekly"]=weekly_commit
                            else:
                                activity_range,commit_rate=parse_repo_commit_activity(repo)

                            repo_dict[f"activity_range"]=activity_range
                            repo_dict[f"commit_rate"]=commit_rate

                        elif repo_row=="contributor_stats":
                            c_count,c_var=parse_contibutors(repo)
                            repo_dict["contributors_count"]=c_count
                            repo_dict["contributors_variance"]=c_var

                        elif repo_row=="language_stats":
                            languages_len, languages, best_lang, language_std=parse_languages(repo)
                            repo_dict["languages_length"]=languages_len
                            repo_dict["languages"]=languages
                            repo_dict["top_lang"]=best_lang
                            repo_dict["language_variance"]=language_std

                        elif repo_row=="detailed_commits":

                            detailed_list_day=parse_detailed_commits(repo,1,return_dict['conf_date'])
                            detailed_list_week=parse_detailed_commits(repo,7,return_dict['conf_date'])
                            detailed_list_month=parse_detailed_commits(repo,30,return_dict["conf_date"])

                            detailed_totals_day=parse_detailed_commits(repo,1,return_dict['conf_date'],"Total")
                            detailed_totals_week = parse_detailed_commits(repo, 7, return_dict['conf_date'], "Total")
                            detailed_totals_month = parse_detailed_commits(repo, 30, return_dict['conf_date'], "Total")

                            repo_dict["detailed_commit_day"]=detailed_list_day
                            repo_dict["detailed_commit_week"]=detailed_list_week
                            repo_dict["detailed_commit_month"]=detailed_list_month


                            repo_dict["detailed_total_day"]=detailed_totals_day
                            repo_dict["detailed_total_month"] = detailed_totals_month
                            repo_dict["detailed_total_week"] = detailed_totals_week

                        elif repo_row=="detailed_stars":
                            detailed_list_day=parse_detailed_stars(repo,1,return_dict['conf_date'])
                            detailed_list_week=parse_detailed_stars(repo,7,return_dict['conf_date'])
                            detailed_list_month=parse_detailed_stars(repo,30,return_dict["conf_date"])

                            repo_dict["detailed_stars_day"]=detailed_list_day
                            repo_dict["detailed_stars_week"]=detailed_list_week
                            repo_dict["detailed_stars_month"]=detailed_list_month

                        else:
                            #if the repo_row does not exist, then do not add. If it has a zero or a null value then add.
                            if not repo.get(repo_row,9)==9:
                                repo_dict[f"{repo_row}"]=repo.get(repo_row)

                    repo_returned=True
                    yield  repo_dict

        if not repo_returned:
            yield  return_dict


def to_csv(paper_rows,repository_rows,outfile="export.csv"):
    with open(outfile,"w",newline="",encoding="utf-8") as file:
        writer=csv.writer(file)
        repo_headers=deepcopy(repository_rows)
        if "commit_stats" in repository_rows:
            repo_headers.remove("commit_stats")
            repo_headers.append("activity_range")
            repo_headers.append("commit_rate")

            if "conf_date" in paper_rows:
                repo_headers.append("commmit_before_publication")
                repo_headers.append("commit_after_publication")
                repo_headers.append("commits_weekly")

        if "detailed_commits" in repository_rows:
            repo_headers.remove("detailed_commits")

            repo_headers.append("detailed_commit_day")
            repo_headers.append("detailed_commit_week")
            repo_headers.append("detailed_commit_month")

            repo_headers.append("detailed_total_day")
            repo_headers.append("detailed_total_week")
            repo_headers.append("detailed_total_month")

        if "detailed_stars" in repository_rows:
            repo_headers.remove("detailed_stars")

            repo_headers.append("detailed_stars_day")
            repo_headers.append("detailed_stars_week")
            repo_headers.append("detailed_stars_month")

        if "contributor_stats" in repository_rows:
            repo_headers.remove("contributor_stats")
            repo_headers.append("contributors_count")
            repo_headers.append("contributors_variance")

        if "language_stats" in repository_rows:
            repo_headers.remove("language_stats")
            repo_headers.append("languages_length")
            repo_headers.append("languages")
            repo_headers.append("top_lang")
            repo_headers.append("language_variance")


        writer.writerow(paper_rows+["citations"]+repo_headers)

        for row_dict in tqdm(data_generator(paper_rows,repository_rows)):
            # To preserve order we have to add the values in the same way, if the value does not exist, it becomes null
            write_row=[]
            for row in paper_rows:
                write_row.append(row_dict.get(row))
            write_row.append(row_dict.get("scholar_cites"))
            for row in repo_headers:
                write_row.append(row_dict.get(row))

            writer.writerow(write_row)


#JSON cannot be streamed into a file
def to_json(paper_rows,repository_rows,outfile="export.json"):
    with open(outfile,'w',encoding="utf-8"):
        rows=[]
        for row in tqdm(data_generator(paper_rows,repository_rows)):
            rows.append(row)
        json.dump(row,outfile)




def add_date_paper(conf,date):

    """
    ADD DATES TO PAPER DEPENDING UPOIN THE DATE AT WHICH THE CONFERENCE TOOK PLACE
    :param conf: conference id corresponding to the crossref field in the database
    :param date: Date of the conference
    """


    coll=get_collection()

    """
      s,count=query_by_conference(conf,return_count=True)
    for item in tqdm(s,total=count):
        return_dic={}
        return_dic["id"]=item.get("_id")
        return_dic["conf_date"]=date
        update_collection_one(return_dic, collection=coll)
    """

    coll.update_many({"crossref":{"$regex":conf}},
                     {"$set":{"conf_date":date}})


def link_repo_to_paper():
    """
    ADD A NEW FIELD IN THE REPOSITORY TABLE CONTAINING THE PAPERS THAT ARE LINKED WITH A REPOSITORY AND MAINTAIN ONE TO ONE ASSOCIATION BY CONSIDERING ONLY THE PAPER WITH THE EARLIEST PUBLICATION DATE
    """

    s,count=query_by_field_exists("Repository",empty=False,return_count=True)
    repo_collection=get_collection(collection_name="repository")

    documents=list(s)

    for doc in tqdm(documents,total=count):
        id=doc.get("_id")
        conf_date=doc.get("conf_date")[0]
        for row in doc.get("Repository",[]):
            if row.get("id"):
                repo = query_repo(row['id'], collection=repo_collection)
                if repo:
                    update_dict={}
                    update_dict["id"]=repo["_id"]
                    repo_list=repo.get("referencing_papers",[])
                    repo_list.append([id,conf_date])
                    update_dict["referencing_papers"]=repo_list
                    if repo.get("earliest_paper",[1,datetime.datetime(2021,1,1)])[1]>conf_date:
                        update_dict["earliest_paper"]=[id,conf_date]
                    update_collection_one(update_dict,collection=repo_collection)



if __name__=="__main__":

    to_csv(["title","year","authors","author_len","H5Index","ImpactScore","conf_date"],["key","subscribers","stars","forks","size","OpenIssues","commit_stats","contributor_stats","language_stats","detailed_commits","detailed_stars"],outfile="detailed_commits_all.csv")

    """
    add_date_paper("conf/nips/2010",[datetime.datetime(2010,12,6),datetime.datetime(2010,12,11)])
    add_date_paper("conf/nips/2011",[datetime.datetime(2011,12,12),datetime.datetime(2011,12,17)])
    add_date_paper("conf/nips/2013",[datetime.datetime(2013,12,5),datetime.datetime(2013,12,10)])
    
    
    date_list=[[datetime.datetime(2014, 12, 8), datetime.datetime(2014, 12, 13)],[datetime.datetime(2015,12,7),datetime.datetime(2015,12,12)],[datetime.datetime(2016,12,5),datetime.datetime(2016,12,10)],[datetime.datetime(2017,12,4),datetime.datetime(2017,12,9)],[datetime.datetime(2018,12,2),datetime.datetime(2018,12,8)],[datetime.datetime(2019,12,8),datetime.datetime(2019,12,14)]]
    for i in range(2014,2020):
        add_date_paper(f"conf/nips/{i}",date_list[i-2014])

    
    date_list=[[datetime.datetime(2018,6,19),datetime.datetime(2018,6,21)],[datetime.datetime(2019,6,16),datetime.datetime(2019,6,20)]]

    for i in range(2018,2020):
        add_date_paper(f"conf/cvpr/{i}",date_list[i-2018])

    date_list=[[datetime.datetime(2010,6,21),datetime.datetime(2010,6,24)],
               [datetime.datetime(2011,6,28),datetime.datetime(2011,7,2)],
               [datetime.datetime(2012,6,25),datetime.datetime(2012,7,1)],
               [datetime.datetime(2013,6,17),datetime.datetime(2013,6,19)],
               [datetime.datetime(2014,6,22),datetime.datetime(2014,6,24)],
               [datetime.datetime(2015,7,7),datetime.datetime(2015,7,9)],
               [datetime.datetime(2016,6,20),datetime.datetime(2016,6,24)],
               [datetime.datetime(2017,8,7),datetime.datetime(2017,8,9)],
               [datetime.datetime(2018,7,10),datetime.datetime(2018,7,15)],
               [datetime.datetime(2019,6,9),datetime.datetime(2019,6,15)]]

    for i in range(2010,2020):
        add_date_paper(f"conf/icml/{i}",date_list[i-2010])


    date_list=[[datetime.datetime(2010,7,11),datetime.datetime(2010,7,16)],
               [datetime.datetime(2011,7,19),datetime.datetime(2011,7,24)],
               [datetime.datetime(2012,7,8),datetime.datetime(2012,7,14)],
               [datetime.datetime(2013,8,4),datetime.datetime(2013,8,9)],
               [datetime.datetime(2014,7,22),datetime.datetime(2014,7,27)],
               [datetime.datetime(2015,7,26),datetime.datetime(2015,7,31)],
               [datetime.datetime(2016,8,7),datetime.datetime(2016,8,12)],
               [datetime.datetime(2017,7,30),datetime.datetime(2017,8,4)],
               [datetime.datetime(2018,7,15),datetime.datetime(2018,7,20)],
               [datetime.datetime(2019,7,28),datetime.datetime(2019,8,2)]]

    for i in range(2010,2020):
        add_date_paper(f"conf/acl/{i}",date_list[i-2010])


    add_date_paper("conf/aaai/2020",[datetime.datetime(2020,2,7),datetime.datetime(2020,2,12)])
    add_date_paper("conf/aaai/2019",[datetime.datetime(2019,1,27),datetime.datetime(2019,2,1)])
    
    

    date_list=[[datetime.datetime(2010,10,9),datetime.datetime(2010,10,11)],
               [datetime.datetime(2011,7,27),datetime.datetime(2011,7,31)],
               [datetime.datetime(2012,7,12),datetime.datetime(2012,7,14)],
               [datetime.datetime(2013,10,18),datetime.datetime(2013,10,21)],
               [datetime.datetime(2014,10,25),datetime.datetime(2014,10,29)],
               [datetime.datetime(2015,9,17),datetime.datetime(2015,9,21)],
               [datetime.datetime(2016,11,1),datetime.datetime(2016,11,5)],
               [datetime.datetime(2017,9,7),datetime.datetime(2017,9,11)],
               [datetime.datetime(2018,10,31),datetime.datetime(2018,11,4)],
               [datetime.datetime(2019,11,3),datetime.datetime(2019,11,7)]]

    for i in range(2010,2020):
        add_date_paper(f"conf/emnlp/{i}",date_list[i-2010])


    date_list_2=[[datetime.datetime(2010,4,10),datetime.datetime(2010,4,15)],
                 [datetime.datetime(2011,5,7),datetime.datetime(2011,5,12)],
                 [datetime.datetime(2012,5,5),datetime.datetime(2012,5,10)],
                 [datetime.datetime(2013,4,27),datetime.datetime(2013,5,2)],
                 [datetime.datetime(2014,4,26),datetime.datetime(2014,5,1)],
                 [datetime.datetime(2015,4,18),datetime.datetime(2015,4,23)],
                 [datetime.datetime(2016,5,7),datetime.datetime(2016,5,12)],
                 [datetime.datetime(2017,5,6),datetime.datetime(2017,5,11)],
                 [datetime.datetime(2018,4,21),datetime.datetime(2018,4,26)],
                 [datetime.datetime(2019,5,4),datetime.datetime(2019,5,9)],
                 [datetime.datetime(2020,4,25),datetime.datetime(2020,4,30)]]


    for i in range(2010,2021):
        add_date_paper(f"conf/chi/{i}",date_list_2[i-2010])
        
    

    date_list=[[datetime.datetime(2010,7,25),datetime.datetime(2010,7,28)],
               [datetime.datetime(2011,8,21),datetime.datetime(2011,8,2)],
               [datetime.datetime(2012,8,12),datetime.datetime(2012,8,16)],
               [datetime.datetime(2013,8,11),datetime.datetime(2013,8,14)],
               [datetime.datetime(2014,8,24),datetime.datetime(2014,8,27)],
               [datetime.datetime(2015,8,10),datetime.datetime(2015,8,13)],
               [datetime.datetime(2016,8,13),datetime.datetime(2016,8,17)],
               [datetime.datetime(2017,8,13),datetime.datetime(2017,8,17)],
               [datetime.datetime(2018,8,19),datetime.datetime(2018,8,23)],
               [datetime.datetime(2019,8,4),datetime.datetime(2019,8,8)]]
    for i in range(2010,2020):
        add_date_paper(f"conf/kdd/{i}",date_list[i-2010])
    add_date_paper("conf/kdd/2020",[datetime.datetime(2020,8,23),datetime.datetime(2020,8,27)])\
    link_repo_to_paper()
    """
