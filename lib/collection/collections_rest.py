from membase.api.rest_client import RestConnection
import re

class Collections_Rest(object):
    def __init__(self):
        pass

    def create_collection(self, node, bucket="default", scope="scope0", collection="mycollection0", result=True):
        status = RestConnection(node).create_collection(bucket, scope, collection)
        if result:
            self.assertEqual(True, status)
            self.log.info("Collection creation passed, name={}".format(collection))
        else:
            self.assertEqual(False, status)
            self.log.info("Collection creation failed, name={}".format(collection))

    def create_scope(self, node, bucket="default", scope="scope0", result=True):
        status = RestConnection(node).create_scope(bucket, scope)
        if result:
            self.assertEqual(True, status)
            self.log.info("Scope creation passed, name={}".format(scope))
        else:
            self.assertEqual(False, status)
            self.log.info("Scope creation failed, name={}".format(scope))


    def delete_collection(self, node, bucket="default", scope='_default', collection='_default'):
        status = RestConnection(node).delete_collection(bucket, scope, collection)
        self.assertEqual(True, status)
        self.log.info("{} collections deleted".format(collection))
        try:
            if "_default._default" in self.collection_name[bucket]:
                self.collection_name[bucket].remove("_default._default")
        except KeyError:
            pass

    def delete_scope(self, node, scope, bucket="default"):  # scope should be passed as default scope can not be deleted
        status = RestConnection(node).delete_collection(bucket, scope)
        self.assertEqual(True, status)
        self.log.info("{} scope deleted".format(scope))
        rex = re.compile(scope)
        try:
            # remove all the collections with the deleted scope
            self.collection_name[bucket] = [x for x in self.collection_name[bucket] if
                                            not rex.match(self.collection_name[bucket])]
        except KeyError:
            pass

    def create_scope_collection(self, node, collection_name, scope_num, collection_num, bucket="default"):
        collection_name[bucket] = ["_default._default"]
        for i in range(scope_num):
            if i == 0:
                scope_name = "_default"
            else:
                scope_name = bucket + str(i)
                self.create_scope(node, bucket, scope=scope_name)
            try:
                if i == 0:
                    num = int(collection_num[i] - 1)
                else:
                    num = int(collection_num[i])
            except:
                num = 2
            for n in range(num):
                collection = 'collection' + str(n)
                self.create_collection(node, bucket=bucket, scope=scope_name, collection=collection)
                collection_name[bucket].append(scope_name + '.' + collection)

        self.log.info("created collections for the bucket {} are {}".format(bucket, collection_name[bucket]))