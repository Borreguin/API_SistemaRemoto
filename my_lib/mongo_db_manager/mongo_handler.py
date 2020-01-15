import pymongo as pm
config_collection = "settings"


def read_config():
    try:
        client = pm.MongoClient()
        db = client["settings"]
        collection = db[config_collection]
        result = collection.find_one({'id': "general_settings"}, {'_id': False})
        if u'_id' in result.keys() or '_id' in result.keys():
            result[u'_id'] = "general_settings"
        return True, result
    except Exception as e:
        return False, str(e)


def save_settings(key, value):
    try:
        client = pm.MongoClient()
        db = client["settings"]
        collection = db[config_collection]
        collection.update(
            {'id': "general_settings"},
            {"$set": {key: value}},
            upsert=True
        )
        return True, "Successful change in Settings: " + str(key) + ":" + str(value)

    except Exception as e:
        print(e)
        return False, str(e)

