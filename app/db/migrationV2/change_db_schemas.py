from app.db.v2.entities.v2_sRNode import V2SRNode
from app.main import db_connection

# This file is used to change the database schema to be compatible with the new version of the API

def migration_process_v2():
    db_connection()
    # Delete index for V2SREntity
    print("Deleting index for V2SREntity: ")
    print(V2SRNode._get_collection().index_information())
    entidad_index = 'entidades.id_entidad_1'
    V2SRNode._get_collection().drop_index(entidad_index)

    print("migration_process_v2 done")
    print(V2SRNode._get_collection().index_information())


if __name__ == '__main__':
    migration_process_v2()