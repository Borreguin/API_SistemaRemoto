import re
from enum import Enum

from mongoengine import Document, NotUniqueError


class MongoDBErrorEnum(str, Enum):
    DUPLICATED = "Duplicated key"


def tryExcept(func):
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
            return True
        except Exception as e:
            print(f">>>>>\tNot able to execute due to: \n{e}")
            return False

    return wrapper


def save_mongo_document_safely(document: Document):
    try:
        document.save()
        return True, 'Document saved successfully'
    except NotUniqueError as e:
        dup_key, collection = find_collection_and_dup_key(f'{e}')
        return False, f"No unique key for: {dup_key} in {collection}"
    except Exception as e:
        return False, f"No able to save: {e}"


def find_collection_and_dup_key(exception: str):
    regex_dup_key = re.compile('dup key:(\\s*\\{[\\w|\\s|\\.|\\:|\\"]*\\})')
    regex_collection = re.compile('(collection:\\s*[\\w|\\.]+)')
    dup_key = regex_dup_key.findall(exception)
    collection = regex_collection.findall(exception)
    return dup_key[0] if len(dup_key) > 0 else '', collection[0] if len(collection) > 0 else ''
