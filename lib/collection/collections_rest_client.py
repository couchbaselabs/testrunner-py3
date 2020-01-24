from membase.api.rest_client import RestConnection

import re
import logger
import json

class Collections_Rest(object):
    def __init__(self, node):
        self.log = logger.Logger.get_logger()
        self.rest = RestConnection(node)

    def create_collection(self, bucket="default", scope="scope0", collection="mycollection0", params=None):
        self.rest.create_collection(bucket, scope, collection, params)

    def create_scope(self, bucket="default", scope="scope0", params=None):
        self.rest.create_scope(bucket, scope, params)

    def delete_collection(self, bucket="default", scope='_default', collection='_default'):
        self.rest.delete_collection(bucket, scope, collection)

    def delete_scope(self, scope, bucket="default"):
        self.rest.delete_scope(bucket, scope)

    def create_scope_collection(self, bucket, scope, collection, params=None):
        self.create_scope(bucket, scope)
        self.create_collection(bucket, scope, collection, params)

    def delete_scope_collection(self, bucket, scope, collection):
        self.rest.delete_collection(bucket, scope, collection)
        self.delete_scope(bucket, scope)

    def get_bucket_scopes(self, bucket):
        return self.rest.get_bucket_scopes(bucket)

    def get_bucket_collections(self, bucket):
        return self.rest.get_bucket_collections(bucket)

    def get_scope_collections(self, bucket, scope):
        return self.rest.get_scope_collections(bucket, scope)

    def delete_scope(self, scope, bucket="default"):  # scope should be passed as default scope can not be deleted
        return self.rest.delete_collection(bucket, scope)

    def create_scope_collection(self, scope_num, collection_num, bucket="default"):
        collection_name = {}
        collection_name[bucket] = []
        for i in range(scope_num):
            if i == 0:
                scope_name = "_default"
            else:
                scope_name = bucket + str(i)
                self.create_scope(bucket, scope=scope_name)
            try:
                if i == 0:
                    num = int(collection_num[i] - 1)
                else:
                    num = int(collection_num[i])
            except:
                num = 2
            for n in range(num):
                collection = 'collection' + str(n)
                self.create_collection(bucket=bucket, scope=scope_name, collection=collection)
        collection_name[bucket] = self.get_collection(bucket)

        self.log.info("created collections for the bucket {} are {}".format(bucket, collection_name[bucket]))
        return collection_name[bucket]

    def get_collection(self, bucket="default"):
        import json
        collections = []
        status, content = self.rest.get_collection(bucket)
        content = json.loads(content.decode())

        num_scopes = content["scopes"].__len__()
        collections = []
        if num_scopes > 0:
            for scope in range(num_scopes):
                scope_name = content["scopes"][scope]["name"]
                col_list = content["scopes"][scope]["collections"]
                col_num = col_list.__len__()
                if col_num > 0:
                    for col in range(col_num):
                        name = scope_name + "." + col_list[col]["name"]
                        collections.append(name)
        return collections
