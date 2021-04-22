import pprint as pp
import pandas as pd
import numpy as np

list_query = lambda cursor: [doc for doc in cursor]

def get_unique_users(coll):
    pipeline = [
        { "$group" : { "_id" : "$created.uid" } },
        { "$count" : "unique_users" }
    ]
    return coll.aggregate(pipeline)


def get_counts(coll):
    zip_ct = len(list_query(coll.find( { "addr.postcode" : { "$exists" : 1 } } ) ) )
    st_ct = len(list_query(coll.find( { "addr.state" : { "$exists" : 1 } } ) ) )
    addr_ct = len(list_query(coll.find( { "addr" : { "$exists" : 1 } } ) ) )
    return zip_ct, st_ct, addr_ct


def update_states(coll):
    print("Updating states:")
    results_df = pd.DataFrame({"Zip": np.zeros(4), "State": np.zeros(4),
                               "Address": np.zeros(4)},
                               index= ["Pre_update", "Matched", "Modified", "Post_update"])
    results_df.columns.name = "Address Key"
    results_df.index.name = "Count"
    zip_ct, st_ct, addr_ct = get_counts(coll=coll)
    results_df.loc["Pre_update", "Zip"] = zip_ct
    results_df.loc["Pre_update", "State"] = st_ct
    results_df.loc["Pre_update", "Address"] = addr_ct
    result = coll.update_many({ "addr.postcode" : { "$exists" : True } },
                              { "$set" : { "addr.state" : "WA" } },
                              upsert=True)
    results_df.loc["Matched", "Zip"] = result.matched_count
    results_df.loc["Modified", "State"] = result.modified_count
    zip_ct, st_ct, addr_ct = get_counts(coll=coll)
    results_df.loc["Post_update", "Zip"] = zip_ct
    results_df.loc["Post_update", "State"] = st_ct
    results_df.loc["Post_update", "Address"] = addr_ct
        
    return results_df


def count_docs_by(coll, doc_type, count_k, group_k):
    '''Find and count documents grouped by given key.
    
    Parameters:
        coll: (MongoDB collection) Collection to query.
        doc_type: (str) Document type to select.
        count_k: (str) Key to count ("_id").
        group_k: (str) Key to group documents by for the count.
    Returns:
        result: (dict) Query result. { "_id" : group_k, "count" : <n> }
    '''
    group_k = "$" + group_k
    result = coll.aggregate([
        { "$match": { "doc_type" : doc_type, count_k : { "$exists" : 1 } } },
        { "$group" : { "_id" : group_k, "count" : { "$sum" : 1 }}}
    ])
    return result


def check_doc_counts_by(coll, doc_type_lst, count_k, group_k):
    '''Find and count documents grouped by given key.
    
    Parameters:
        coll: (MongoDB collection) Collection to query.
        doc_type_lst: (list(str)) Document types to select.
        count_k: (str) Key to count ("_id").
        group_k: (str) Key to group documents by for the count.
    Returns:
        doc_count_lst: (list(dict)) Listed query results. [{ "_id" : group_k, "count" : <n> }, ...]
    '''
    doc_count_lst = []
    for dt in doc_type_lst:
        doc_count = list_query(count_docs_by(coll=coll, doc_type=dt, count_k=count_k, group_k=group_k))
        doc_count_lst.extend(doc_count)

    return doc_count_lst


def get_bike_services(coll):
    print("Documents with bicycle services, shops, and/or bike repair stations:\n")

    query = { "$or" : [ { "service.bicycle" : { "$exists" : 1 } },
                        { "shop" : "bicycle" },
                        { "amenity" : "bicycle_repair_station" }] }
    projection = { "_id" : 1, "name" : 1, "note" : 1, "description" : 1,
                   "service" : 1, "shop" : 1, "amenity" : 1, "opening_hours" : 1,
                  "fee" : 1 }
    pp.pprint(list_query(coll.find(query, projection)))
    return
    
    
def audit_ref_types(coll, coll_str):
    '''Audit reference types. Node_refs expected to only point to nodes, but okay if not.
    Member.type should match doc_type of document reference by member.ref. Print findings.
    
    Parameters:
        coll: (MongoDB collection) Collection to check.
        coll_str: (str) Collection name.
    Returns:
        None
    '''
    
    print("Ways point to the following types:")
    pipeline = [
        { "$match" : { "doc_type" : "way" } },
        { "$unwind" : "$node_refs" },
        {
            "$lookup" : {
                "from" : coll_str,
                "localField" : "node_refs",
                "foreignField" : "_id",
                "as" : "refs"
            }
        },
        { "$match" : { "refs" : { "$ne" : [] } } },
        { "$group" : { "_id" : "$refs.doc_type" } },
        { "$project" : { "_id" : 0, "type" : "$_id" } }
    ]
    print(list_query(coll.aggregate(pipeline)))

    print("Relations point to the following types, and refer to them as:")
    pipeline = [
        { "$match" : { "doc_type" : "relation" } },
        { "$unwind" : "$members" },
        {
            "$lookup" : {
                "from" : coll_str,
                "localField" : "members.ref",
                "foreignField" : "_id",
                "as" : "refs"
            }
        },
        { "$match" : { "refs" : { "$ne" : [] } } },
        { "$group" : { "_id" : "$refs.doc_type",
                       "member_type_set" : { "$addToSet" : "$members.type" } } },
        { "$project" : { "_id" : 0, "type" : "$_id",
                         "referred_as" : "$member_type_set"}}
    ]
    pp.pprint(list_query(coll.aggregate(pipeline)))
    return


def get_doctype_mismatches(coll, coll_str):
    '''Find ways with node_refs that don't match to elements that are nodes.
    Find relations with member types that don't match referenced types.
    Print findings.
    
    Parameters:
        coll: (MongoDB collection) Collection to check.
        coll_str: (str) Collection name.
    Returns:
        mismatched_members_lst: (list(dict)) Relations with mismatched referenced document types.
            e.g. [{'_id': '2317217', 'members': {'ref': '37125674', 'role': 'forward', 'type': 'node'},
                   'refs': {'_id': '37125674', 'doc_type': 'node'}}, ... ]
    '''
    
    # Find ways with node_refs that don't match to nodes.
    pipeline = [
        { "$match" : { "doc_type" : "way" } },
        { "$unwind" : "$node_refs" },
        { # Left join.
            "$lookup" :
            {
                "from" : coll_str,
                "localField" : "node_refs",
                "foreignField" : "_id",
                "as" : "refs"
            }
        }, # Default unwind removes nulls, so makes inner join.
        { "$unwind" : "$refs" },
        {
            "$project" :
            {
                "doc_type" : 1,
                "comp" : { "$cmp" : [ "$refs.doc_type", "node" ] },
                "refs" : 1
            }
        },
        { "$match" : { "comp" : { "$ne" : 0 } } }
    ]
    print("Ways pointing to non-nodes:")
    for doc in coll.aggregate(pipeline):
        pp.pprint(doc)
    print("\n")

    # Find relations with member types that don't match referenced types.
    pipeline = [
        { "$match" : { "doc_type" : "relation" } },
        { "$unwind" : "$members" },
        {
            "$lookup" :
            {
                "from" : coll_str,
                "localField" : "members.ref",
                "foreignField" : "_id",
                "as" : "refs"
            }
        },
        { "$unwind" : "$refs" },
        {
            "$project" :
            {
                "comp" : { "$cmp" : [ "$members.type", "$refs.doc_type" ] },
                "members" :
                {
                    "$cond" : [
                        { "$ne" : [ "$comp", 0 ] },
                        {
                            "type" : "$refs.doc_type",
                            "ref" : "$members.ref",
                            "role" : "$members.role"
                        },
                        "$members"
                    ]
                },
                "refs._id" : 1, "refs.doc_type" : 1
            }
        },
        { "$match" : { "comp" : { "$ne" : 0 } } },
        { "$project" : { "members" : 1, "refs._id" : 1, "refs.doc_type" : 1 } }
    ]
    
    mismatched_members_lst = list_query(coll.aggregate(pipeline))
    print("Relations with mismatched referenced document types:")
    pp.pprint(mismatched_members_lst)
    
    return mismatched_members_lst
    
    
def fix_mismatched_refs(coll, mismatched_members_lst):
    '''Fix mismatched references (where relations point to elements that don't match their reference type).
    
    Parameters:
        coll: (MongoDB collection) Collection to be updated.
        mismatched_members_lst: (list(dict)) Relations with mismatched referenced document types.
            e.g. [{'_id': '2317217', 'members': {'ref': '37125674', 'role': 'forward', 'type': 'node'},
                   'refs': {'_id': '37125674', 'doc_type': 'node'}}, ... ]
    Return:
        None
    '''
    
    for doc in mismatched_members_lst:
        fltr = { "_id" : doc["_id"], "members.ref" : doc["members"]["ref"] }
        update = {
            "$set" :
            {
                "members.$" : doc["members"]
            }
        }
        projection = { "members.$" : 1 }
        up_doc = coll.find_one_and_update(fltr, update, projection)
        
        print("Member to update:")
        print(up_doc)
        
        print("Updated member:")
        fltr = { "_id" : up_doc["_id"], "members.ref" : doc["members"]["ref"] }
        print(coll.find_one(fltr, projection))
        
        print("Referenced document:")
        print( coll.find_one( { "_id" : doc["members"]["ref"] },
                                  { "doc_type" : 1 } ), "\n" )
    return


def write_ref_docs(db, coll):
    '''Create a reference collection for node references. Overwrites existing reference collection "ref_docs".
    
    Parameters:
        db: (MongoDB) Database to add the reference collection "ref_docs".
        coll: (MondoDB collection) Collection to create the reference collection for.
        
    Returns:
        ref_docs_coll: (MongoDB collection) Handle of "ref_docs" collection.
    '''
    pipeline = [
        {
            "$unwind" :
            {
                "path" : "$node_refs",
                "preserveNullAndEmptyArrays" : True
            }
        },
        {
            "$unwind" :
            {
                "path" : "$members",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$match" :
            {
                "$or" : [
                    { "node_refs" : { "$exists" : 1 } },
                    { "members" : { "$exists" : 1 } }
                ]
            }
        },
        {
            "$project" :
            {
                "refs" : [ "$node_refs", "$members.ref" ]
            }
        },
        { "$unwind" : "$refs" },
        { "$match" : { "refs" : { "$ne" : None } } },
        {
            "$group" :
            {
                "_id" : "$refs",
                "refers" :
                {
                    "$push" : "$_id"
                },
            }
        }
    ]

    db.drop_collection("ref_docs")
    ref_docs_col = db.create_collection("ref_docs")
    ref_docs_col.insert_many(list_query(coll.aggregate(pipeline)))
    
    return ref_docs_col
    
    
def get_by_field(coll, field):
    
    pipeline = [
        { "$match" : { field : { "$exists" : 1 } } },
        { "$project" : { field : "$"+field } }
    ]
    
    return coll.aggregate(pipeline)
    
def get_most_refd(coll, field, limit):
# Which service documents are referenced most, and who contributed them?
    pipeline = [
        { "$match" : { field : { "$exists" : 1 } } },
        {
            "$lookup" :
            {
                "from" : "ref_docs",
                "localField" : "_id",
                "foreignField" : "_id",
                "as" : "ref_doc"
            }
        },
        { "$unwind" : "$ref_doc" },
        {
            "$project" :
            {
                "refer_count" : { "$size" : "$ref_doc.refers" }
            }
        },
        { "$sort" : { "refer_count" : -1 } },
        { "$limit" : limit},
        {
            "$lookup" :
            {
                "from" : "bham",
                "localField" : "_id",
                "foreignField" : "_id",
                "as" : "full_ref_doc"
            }
        },
        {
            "$project" :
            {
                "refer_count" : 1,
                "contributor" : "$full_ref_doc.created.user",
                "contributer_uid" : "$full_ref_doc.created.uid"
            }
        }
    ]
    return coll.aggregate(pipeline)