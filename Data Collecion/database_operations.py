from pymongo import MongoClient
import json
from bson.objectid import ObjectId
from pymongo.errors import  WriteConcernError,WriteError

def get_collection(connection_url="MONGODB DATABSE URL",database_name="papers",collection_name="papers"):
    """
    GET THE CONNECTION WE WANT
    :param connection_url: MongoDB URI formatted connection url for the database
    :type connection_url: String

    :return:pyMongo.MongoClient Object
    """

    client= MongoClient("localhost",27018)
    return client[database_name][collection_name]

def import_json(input_file):
    """
    INSERT A JSON FILE INTO A MONGODB COLLECTION
    :param input_file: json file path
    :type input_file: string

    :return: Inserted ids of the documents in the MongoDB collection
    """
    #load the json file to insert into the database
    imported=json.load(open(input_file))

    # get the collection object from mongodb
    collection=get_collection()

    # insert into the database
    results=collection.insert_many(imported)

    return results.inserted_ids


def query_by_conference(conference_key,strict_matching=False,return_count=False):
    """
    GET ALL THE DOCUMENTS REPRESENTING A PAPER FROM THE DATABASE

    :param conference_key: DBLP key for the conference we want to query
    :type conference_key: string
    :param strict_matching:Indicates whether the conference id matching with the dblp key is strict. /conf/nips/2019-1 matches with /conf/nips/2019 with strict_matching=False. Used to include the workshops or if the publication is broken into multiple volumes.
    :type strict_matching: Boolean
    :param return_count: Return the total number of matches with the cursor or not
    :type return_count: Boolean

    :return: MonogDB cursor or list
    """

    collection=get_collection()

    #list to return
    ret=[]

    if strict_matching:
        #simple query, does not include regex matching
        ret.append( collection.find({"crossref": conference_key}))
        ret.append(collection.count_documents({"crossref": conference_key}))
    else:
        #regex match to ensure that as long as the conference key is included in the crossref field, the match is accpeted
        ret.append( collection.find({"crossref":{"$regex":conference_key}}))
        ret.append(collection.count_documents({"crossref":{"$regex":conference_key}}))


    # return the list if return_count is enabled else just the cursor
    if return_count:
        return ret
    else:
        return ret[0]



def update_collection_many(update_dicts,collection=None):
    """
    :param update_dicts: List of dictonaries with ids and fields to be updated to the mognodb database
    :type: list
    :return:
    """
    # if collection is not given then get one
    if not collection:
        collection=get_collection()

    # loop through the content of the dict and use Mongodb's update_one method to add new fields
    for record in update_dicts:
        #use the id key from the update_dict and remove it from the dict
        id=record.  pop("id")
        collection.update_one(
            {"id":id},
            {"$set":record}
        )

def update_collection_one(update_dict,object_id=None,collection=None):
    """
    Update collection to add one or multiple field,only one collection at a time.

    :param object_id: MongoDB id of the collection. By default this should be contained within the "id" attribute of the update dict, if this is not the desired behavior use this parameter
    :type object_id: string for bson.objectid object
    :param update_dict: Dictionary containg the new fields to add to the dict
    :type update_dict: dict
    :param collection: MongoDB collection object, to reduce repeatedly getting connections
    :type collection: MongoClient.Collection object
    """

    # if custom object_id desired
    if object_id:
        if type(object_id)==str:
            object_id=ObjectId(object_id)

    else:
        # use the id key from the update_dict and remove it from the dict
        object_id=update_dict.pop("id")

    # if collection is not given then get one
    if not collection:
        collection=get_collection()

    try:

        collection.update_one(

            {"_id":object_id},
            {"$set":update_dict}
        )
    except (WriteError,WriteConcernError) as e:
        print("Writerror")
        pass



def query_by_field_exists(field_name,exist_bool=True,return_count=False,no_timeout=False,empty=True):
    """
    GET ALL THE DOCUMENTS WHERE THE FIELD WE WANT HAS AN EXISTENT VALUE (CAN BE NULL)
    :param field_name: Name of field we are checking
    :type field_name: String
    :param exist_bool: Boolean indicating if we're checking if the field exist or doesn't exist
    :type exist_bool: Boolean
    :param return_count: Return the total number of matches with the cursor or not
    :type return_count: Boolean
    :param empty: Indicates whether to exclude documents where the field exists, but is empty or null
    :type empty: Boolean
    :return:(MonogDB cursor, count of matches)
    """

    return_list=[]

    # get collection
    collection=get_collection()

    #add cursor to the list
    if not empty:
        return_list.append(collection.find({field_name:{"$exists":exist_bool,"$ne":[]}},no_cursor_timeout=no_timeout))
    else:
        return_list.append(collection.find({field_name: {"$exists": exist_bool}}, no_cursor_timeout=no_timeout))
    if return_count:
        # add count of documents matching the filter\
        if not empty:
            return_list.append(collection.count_documents({field_name:{"$exists":exist_bool,"$ne":[]}}))
        else:
            return_list.append(collection.count_documents({field_name:{"$exists":exist_bool}}))
        return return_list
    else:
        return return_list[0]


def check_repo_exists(repo_key,repo_id=None):
    """
    CHECK IF A PARTICULAR REPO WITH A KEY EXISTS IN THE REPOSITORY COLLECTION. IF SO RETURN THE OBJECTID, IF NOT RETURN 0. UPDATES THE COUNT OF THE REPOSITORY.
    THIS FUNCTION IS ONLY USED FOR THE INITIAL POPULATION OF THE REPOSITORY DATASET
    :param repo_key: key of the repo in the format {SITE}/author_id/repo_id
    :type repo_key: String
    :return: OBJECTID OR 0
    """

    object_id=0
    # get the collection
    collection=get_collection(collection_name="repository")

    repo_id=ObjectId(repo_id)

    if repo_id:
        c=collection.find({"_id":ObjectId(repo_id)})
    else:
        c=collection.find({"key":repo_key})

    # if the key exists this loop will not happen so we just return zero. If not, we return the objectid
    for document in c:
        object_id=document["_id"]
        #update the counter for this dict
        update_dict={}
        c=document.get("count",0)
        c+=1
        update_dict["id"]=object_id
        update_dict["count"]=c
        update_collection_one(update_dict,collection=collection)

    return  object_id


def query_repo(repo_id,count=100,collection=None):
    """GET AN MONGODB CURSOR FOR A REPOSITORY DOCUMENT. ONLY RETURNS REPOSTIORIES THAT HAVE COUNT LESS THAN A PRE-DEFNIED AMOUNT.
    :param repo_id: ObjectId of the Repository
    :type repo_id: bson.ObjectID or String
    :param count: The upper limit for count attribute of a document  that is to be returned
    :type count: Integer
    :param collection: MongoDB collection object. Avoids repeateadbly establishing connection to the MongoDB server
    :type collection: MongoClient.Collection Object
    """

    if not collection:
        collection=get_collection(collection_name="repository")

    repo_id=ObjectId(repo_id)

    c = collection.find({"_id": ObjectId(repo_id)})

    for doc in c:
        if doc.get("count")<count:
            return doc

    return {}

def repo_query_unique(repo_id,document_id,collection=None):
    """
    GET A MONGODB CURSOR FOR A REPOSITORY ID ONLY IF THE DOCUMENT ID IS THE EARLIEST OF ALL THE PAPERS THAT REFER TO THIS REPOSITORY
    :param repo_id: ObjectId of the Repository
    :type repo_id: bson.ObjectId or String
    :param document_id: ObjectId of the Paper
    :type document_id: bson.ObjectId or String
    :param collection: Collection object to avoid repeatedly avoid connection attempts
    :type collection: MongoClient.Collection Object
    """

    repo=query_repo(repo_id,collection=collection)

    id=repo.get("earliest_paper")[0]

    if id==document_id:
        return repo
    else:
        return {}


def insert_document(document,collection=None):
    """
I   INSERT A DOCUMENT THAT DOES NOT EXIST INTO A COLLECTION
    :param document: the document that is being inserted
    :type document: Dict
    :param collection: MongoDB collection object, to reduce repeatedly getting connections
    :type collection: MongoClient.Collection object
    :return: Id of the inserted document in string format
    """

    if not collection:
        collection=get_collection()

    id=collection.insert_one(document)

    return str(id.inserted_id)


if __name__=="__main__":
    import_json("dblp.json")
    '''
    for c in query_by_conference("conf/nips/2018",True):
        print (c)
        break
    '''
