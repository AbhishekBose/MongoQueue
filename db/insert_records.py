from db.mongoqueue import insertData


def insertRecords(ds, *args):
    query = {
        "user_id": int(ds),
        "num_of_videos": int(ds) *100,
        "process_state": "Not Processed"
    }
    coll = 'fetch_list'
    msg = insertData(coll, query)
    return msg
