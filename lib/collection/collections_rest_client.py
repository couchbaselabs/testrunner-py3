from membase.api.rest_client import RestConnection
import re
import logger

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