from basetestcase import BaseTestCase

from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError, MemcachedClient
from collection.collections_rest_client import Collections_Rest
from couchbase_cli import CouchbaseCLI
from lib.couchbase_helper.tuq_generators import JsonGenerator
from TestInput import TestInputSingleton, TestInputServer
from membase.api.rest_client import RestConnection
import logger

class basic_collections(BaseTestCase):

    def setUp(self):
        super(basic_collections, self).setUp()
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.default_bucket_name = self.input.param("default_bucket_name", "default")
        self.servers = self.input.servers
        self.master = self.servers[0]
        self.use_rest = self.input.param("use_rest", True)
        self.use_cli = self.input.param("use_cli", False)
        self.rest = Collections_Rest(self.master)
        self.cli = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password)
        self.cli.enable_dp()


    def tearDown(self):
        RestConnection(self.master).delete_all_buckets()

    def generate_docs_bigdata(self, docs_per_day, start=0, document_size=1024000):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=docs_per_day, start=start, value_size=document_size)


    def test_load_collection(self):
        #epengine.basic_collections.basic_collections.test_load_collection,value_size=200,num_items=100,collection=True
        self.value_size = 200
        self.enable_bloom_filter = False
        self.buckets = RestConnection(self.master).get_buckets()
        self.active_resident_threshold = float(self.input.param("active_resident_threshold", 100))


        gen_create = BlobGenerator('eviction', 'eviction-', self.value_size, end=self.num_items)

        self._load_all_buckets(self.master, gen_create, "create", 0)

        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])

        self._verify_all_buckets(self.master)
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])

        # Add a node, rebalance and verify data
        # self._load_all_buckets(self.master, gen_create, "delete", 0)
        # self._verify_stats_all_buckets(self.servers[:self.nodes_init])
        #
        # self._load_all_buckets(self.master, gen_create, "create", 0)
        # # Add a node, rebalance and verify data
        # self._load_all_buckets(self.master, gen_create, "read", 0)
        # self._verify_stats_all_buckets(self.servers[:self.nodes_init])
        #
        # self._load_all_buckets(self.master, gen_create, "update", 0)
        # self._verify_stats_all_buckets(self.servers[:self.nodes_init])
        # self._verify_all_buckets(self.master)
