import uuid
import copy
from tuqquery.tuq import QueryTests
from couchbase_helper.documentgenerator import DocumentGenerator
import time

JOIN_INNER = "INNER"
JOIN_LEFT = "LEFT"
JOIN_RIGHT = "RIGHT"

class JoinTests(QueryTests):
    def setUp(self):
        try:
            super(JoinTests, self).setUp()
            self.gens_tasks = self.generate_docs_tasks()
            self.type_join = self.input.param("type_join", JOIN_INNER)
        except Exception, ex:
            self.log.error("ERROR SETUP FAILED: %s" % str(ex))
            raise ex

    def suite_setUp(self):
        super(JoinTests, self).suite_setUp()
        self.load(self.gens_tasks, start_items=self.num_items)

    def tearDown(self):
        super(JoinTests, self).tearDown()

    def suite_tearDown(self):
        super(JoinTests, self).suite_tearDown()

    def test_simple_join_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project " +\
            "FROM %s as employee %s JOIN default.project as new_project " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids"
            time.sleep(30)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            full_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                     "new_project" : doc['project']}
                                    for doc in full_list if doc and 'project' in doc])
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
                                    for doc in full_list if doc and not 'project' in doc])
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_prepared_simple_join_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project " +\
            "FROM %s as employee %s JOIN default.project as new_project " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids"
            self.prepared_common_body()

    def test_join_several_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_task.project, new_task.task_name " +\
            "FROM %s as employee %s JOIN default as new_task " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            full_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                     "project" : doc['project'], "task_name" : doc['task_name']}
                                    for doc in full_list if doc and 'project' in doc])
            #expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
            #                        for doc in full_list if doc and not 'project' in doc])
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)


    def test_where_join_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE new_project_full.project == 'IT'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'project' in doc and\
                               doc['project'] == 'IT']
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_bidirectional_join(self):
            self.query = "create index idxbidirec on %s(tasks_ids)" %self.buckets[1].name ;
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = "explain SELECT employee.name, employee.tasks_ids " +\
            "FROM %s as employee %s JOIN %s as new_project " % (self.buckets[0].name, self.type_join,self.buckets[1].name) +\
            "ON KEY new_project.tasks_ids FOR employee where new_project.tasks_ids is not null"
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.test_explain_particular_index("idxbidirec")
            self.query = "SELECT employee.name, employee.tasks_ids " +\
            "FROM %s as employee %s JOIN %s as new_project " % (self.buckets[0].name, self.type_join,self.buckets[1].name)  +\
            "ON KEY new_project.tasks_ids FOR employee where new_project.tasks_ids is not null"
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['metrics']['resultCount'] == 0, 'Query was not run successfully')
            self.query = "drop index %s.idxbidirec" %self.buckets[1].name;
            actual_result = self.run_cbq_query()


    def test_where_join_keys_covering(self):
        created_indexes = []
        ind_list = ["one"]
        index_name="one"
        for bucket in self.buckets:
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind =="one":
                    self.query = "CREATE INDEX %s ON %s(name, tasks_ids,job_title)  USING %s" % (index_name, bucket.name,self.index_type)
                    if self.gsi_type:
                        self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for bucket in self.buckets:
            self.query = "EXPLAIN SELECT employee.name, employee.tasks_ids, employee.job_title new_project " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.name == 'employee-9'"
            if self.covering_index:
                self.test_explain_covering_index(index_name[0])
            self.query = "SELECT employee.name, employee.tasks_ids " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.name == 'employee-9'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']
                                }
            for doc in expected_result if doc and 'name' in doc and\
                                          doc['name'] == 'employee-9']
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
            for index_name in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                self.run_cbq_query()

    def test_where_join_keys_not_equal(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day != 2"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] != 2]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_between(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day between 1 and 2"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] <= 2]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_not_equal_more_less(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day <> 2"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] != 2]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_equal_less(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.project new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] <= 2]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_equal_more(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.job_title new_project " +\
            "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
            "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['job_title']}
                               for doc in expected_result if doc and 'join_day' in doc and\
                               doc['join_day'] <= 2]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys_equal_more_covering(self):
        created_indexes = []
        ind_list = ["one"]
        index_name="one"
        for bucket in self.buckets:
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind =="one":
                    self.query = "CREATE INDEX %s ON %s(join_day, tasks_ids, job_title)  USING %s" % (index_name, bucket.name,self.index_type)
                    if self.gsi_type:
                        self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for bucket in self.buckets:
            self.query = "EXPLAIN SELECT employee.join_day, employee.tasks_ids, new_project_full.job_title new_project " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2 order by employee.join_day limit 10"
            if self.covering_index:
                self.test_explain_covering_index(index_name[0])
            self.query = "SELECT employee.join_day, employee.tasks_ids, new_project_full.job_title new_project " +\
                         "FROM %s as employee %s JOIN default as new_project_full " % (bucket.name, self.type_join) +\
                         "ON KEYS employee.tasks_ids WHERE employee.join_day <= 2  order by employee.join_day limit 10"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"join_day" : doc['join_day'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['job_title']}
            for doc in expected_result if doc and 'join_day' in doc and\
                                          doc['join_day'] <= 2]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_day']))[0:10]
            self._verify_results(actual_result['results'], expected_result)
            for index_name in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                self.run_cbq_query()

    def test_join_unnest_alias(self):
        for bucket in self.buckets:
            self.query = "SELECT task2 FROM %s emp1 JOIN %s" % (bucket.name, bucket.name) +\
            " task ON KEYS emp1.tasks_ids UNNEST emp1.tasks_ids as task2"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc:(
                                                               doc['task2']))
            expected_result = self._generate_full_joined_docs_list()
            expected_result = [{"task2" : task} for doc in expected_result
                               for task in doc['tasks_ids']]
            expected_result = sorted(expected_result, key=lambda doc:(
                                                          doc['task2']))
            self._verify_results(actual_result, expected_result)

    def test_unnest(self):
        for bucket in self.buckets:
            self.query = "SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task" % (bucket.name,self.type_join)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self.generate_full_docs_list(self.gens_load)
            expected_result = [{"task" : task, "name" : doc["name"]}
                               for doc in expected_result for task in doc['tasks_ids']]
            if self.type_join.upper() == JOIN_LEFT:
                expected_result.extend([{}] * self.gens_tasks[-1].end)
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_unnest_covering(self):
        created_indexes = []
        ind_list = ["one"]
        index_name="one"
        for bucket in self.buckets:
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind =="one":
                    self.query = "CREATE INDEX %s ON %s(name, task, tasks_ids)  USING %s" % (index_name, bucket.name,self.index_type)
                    if self.gsi_type:
                        self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for bucket in self.buckets:
            self.query = "EXPLAIN SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task where emp.name is not null" % (bucket.name,self.type_join)
            if self.covering_index:
                self.test_explain_covering_index(index_name[0])
            self.query = "SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task where emp.name is not null" % (bucket.name,self.type_join)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self.generate_full_docs_list(self.gens_load)
            expected_result = [{"task" : task, "name" : doc["name"]}
            for doc in expected_result for task in doc['tasks_ids']]
            if self.type_join.upper() == JOIN_LEFT:
                expected_result.extend([{}] * self.gens_tasks[-1].end)
            expected_result = sorted(expected_result)
            #self._verify_results(actual_result, expected_result)
        for index_name in created_indexes:
            self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
            self.run_cbq_query()

    def test_prepared_unnest(self):
        for bucket in self.buckets:
            self.query = "SELECT emp.name, task FROM %s emp %s UNNEST emp.tasks_ids task" % (bucket.name,self.type_join)
            self.prepared_common_body()

##############################################################################################
#
#   SUBQUERY
##############################################################################################

    def test_subquery_count(self):
        for bucket in self.buckets:
            self.query = "select name, ARRAY_LENGTH((select task_name  from %s d use keys %s)) as cn from %s" % (bucket.name, str(['test_task-%s' % i for i in xrange(0, 29)]),
                                                                                                                    bucket.name)
            self.run_cbq_query()
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            expected_result= [{'name': doc['name'],
                                     'cn' : 29}
                                    for doc in all_docs_list]
            expected_result.extend([{'cn' : 29}] * 29)
            self._verify_results(actual_result, expected_result)

    def test_subquery_select(self):
        for bucket in self.buckets:
            self.query = "select task_name, (select count(task_name) cn from %s d use keys %s) as names from %s" % (bucket.name, str(['test_task-%s' % i for i in xrange(0, 29)]),
                                                                                                                    bucket.name)
            self.run_cbq_query()
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result_subquery = {"cn" : 29}
            expected_result = [{'names' : [expected_result_subquery]}] * len(self.generate_full_docs_list(self.gens_load))
            expected_result.extend([{'task_name': doc['task_name'],
                                     'names' : [expected_result_subquery]}
                                    for doc in self.generate_full_docs_list(self.gens_tasks)])
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_prepared_subquery_select(self):
        for bucket in self.buckets:
            self.query = "select task_name, (select count(task_name) cn from %s d use keys %s) as names from %s" % (bucket.name, str(['test_task-%s' % i for i in xrange(0, 29)]),
                                                                                                                    bucket.name)
            self.prepared_common_body()

    def test_subquery_where_aggr(self):
        for bucket in self.buckets:
            self.query = "select name, join_day from %s where join_day =" % (bucket.name) +\
            " (select AVG(join_day) as average from %s d use keys %s)[0].average" % (bucket.name,
                                                                               str(['query-test-Sales-2010-1-1-%s' % i
                                                                                    for i in xrange(0, self.docs_per_day)]))
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name'],
                                'join_day' : doc['join_day']}
                               for doc in all_docs_list
                               if  doc['join_day'] == 1]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_subquery_where_in(self):
        for bucket in self.buckets:
            self.query = "select name, join_day from %s where join_day IN " % (bucket.name) +\
            " (select ARRAY_AGG(join_day) as average from %s d use keys %s)[0].average" % (bucket.name,
                                                                               str(['query-test-Sales-2010-1-1-%s' % i
                                                                                    for i in xrange(0, self.docs_per_day)]))
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name'],
                                'join_day' : doc['join_day']}
                               for doc in all_docs_list
                               if doc['join_day'] == 1]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids from %s where tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project='MB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name'],
                                'tasks_ids' : doc['tasks_ids']}
                               for doc in all_docs_list
                               if doc['tasks_ids'] in ['test_task-1', 'test_task-2']]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_not_equal(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids from %s where tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project!='AB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name'],
                                'tasks_ids' : doc['tasks_ids']}
                               for doc in all_docs_list
                               if ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids'])]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_equal_more(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids,join_day from %s where join_day>=2 and tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project!='AB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name'],
                                'tasks_ids' : doc['tasks_ids'],
                                'join_day': doc['join_day']}
                               for doc in all_docs_list
                               if ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids']) and doc['join_day'] >=2]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_equal_less(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids,join_day from %s where join_day<=2 and tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project!='AB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name'],
                                'tasks_ids' : doc['tasks_ids'],
                                'join_day': doc['join_day']}
                               for doc in all_docs_list
                               if ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids']) and doc['join_day'] <=2]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_in_subquery_between(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_ids, join_day from %s where (join_day between 1 and 12) and tasks_ids[0] IN" % bucket.name +\
            " (select ARRAY_AGG(DISTINCT task_name) as names from %s d " % bucket.name +\
            "use keys %s where project!='AB')[0].names" % ('["test_task-1", "test_task-2"]')
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name'],
                                'tasks_ids' : doc['tasks_ids'],
                                'join_day': doc['join_day']}
                               for doc in all_docs_list
                               if ('test_task-1' in doc['tasks_ids'] or 'test_task-2' in doc['tasks_ids']) and
                                  doc['join_day'] <=12]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_subquery_exists(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s d1 WHERE " % bucket.name +\
            "EXISTS (SELECT * FROM %s d  use keys toarray(d1.tasks_ids[0]))" % bucket.name
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            tasks_ids = [doc["task_name"] for doc in self.generate_full_docs_list(self.gens_tasks)]
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name']}
                               for doc in all_docs_list
                               if doc['tasks_ids'][0] in tasks_ids]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_subquery_exists_where(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s d1 WHERE " % bucket.name +\
            "EXISTS (SELECT * FROM %s d use keys toarray(d1.tasks_ids[0]) where d.project='MB')" % bucket.name
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            tasks_ids = [doc["task_name"] for doc in self.generate_full_docs_list(self.gens_tasks) if doc['project'] == 'MB']
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name']}
                               for doc in all_docs_list
                               if doc['tasks_ids'][0] in tasks_ids]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_subquery_exists_and(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s d1 WHERE " % bucket.name +\
            "EXISTS (SELECT * FROM %s d  use keys toarray(d1.tasks_ids[0])) and join_mo>5" % bucket.name
            all_docs_list = self.generate_full_docs_list(self.gens_load)
            tasks_ids = [doc["task_name"] for doc in self.generate_full_docs_list(self.gens_tasks)]
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'name' : doc['name']}
                               for doc in all_docs_list
                               if doc['tasks_ids'][0] in tasks_ids and doc['join_mo']>5]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_subquery_from(self):
        for bucket in self.buckets:
            self.query = "SELECT TASKS.task_name FROM (SELECT task_name, project FROM %s WHERE project = 'CB') as TASKS" % bucket.name
            all_docs_list = self.generate_full_docs_list(self.gens_tasks)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'task_name' : doc['task_name']}
                               for doc in all_docs_list
                               if doc['project'] == 'CB']
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_subquery_from_join(self):
        for bucket in self.buckets:
            self.query = "SELECT EMP.name Name, TASK.project proj FROM (SELECT tasks_ids, name FROM "+\
            "%s WHERE join_mo>10) as EMP %s JOIN %s TASK ON KEYS EMP.tasks_ids" % (bucket.name, self.type_join, bucket.name)
            all_docs_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = [{'Name' : doc['name'], 'proj' : doc['project']}
                               for doc in all_docs_list
                               if doc['join_mo'] > 10]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
##############################################################################################
#
#   KEY
##############################################################################################

    def test_keys(self):
        for bucket in self.buckets:
            keys_select = []
            generator = copy.deepcopy(self.gens_tasks[0])
            for i in xrange(5):
                key, _ = generator.next()
                keys_select.append(key)
            self.query = 'select task_name FROM %s USE KEYS %s' % (bucket.name, keys_select)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['task_name']))
            full_list = self.generate_full_docs_list(self.gens_tasks, keys=keys_select)
            expected_result = [{"task_name" : doc['task_name']} for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task_name']))
            self._verify_results(actual_result, expected_result)

            keys_select.extend(["wrong"])
            self.query = 'select task_name FROM %s USE KEYS %s' % (bucket.name, keys_select)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            self._verify_results(actual_result, expected_result)

            self.query = 'select task_name FROM %s USE KEYS ["wrong_one","wrong_second"]' % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertFalse(actual_result['results'], "Having a wrong key query returned some result")

    def test_key_array(self):
        for bucket in self.buckets:
            gen_select = copy.deepcopy(self.gens_tasks[0])
            key_select, value_select = gen_select.next()
            self.query = 'SELECT * FROM %s USE KEYS ARRAY emp._id FOR emp IN [%s] END' % (bucket.name, value_select)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self.generate_full_docs_list(self.gens_tasks, keys=[key_select])
            expected_result = [{bucket.name : doc} for doc in expected_result]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

            key2_select, value2_select = gen_select.next()
            self.query = 'SELECT * FROM %s USE KEYS ARRAY emp._id FOR emp IN [%s,%s] END' % (bucket.name,
                                                                                      value_select,
                                                                                      value2_select)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])
            expected_result = self.generate_full_docs_list(self.gens_tasks, keys=[key_select, key2_select])
            expected_result = [{bucket.name : doc} for doc in expected_result]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   NEST
##############################################################################################


    def test_simple_nest_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT * FROM %s emp %s NEST %s tasks " % (
                                                bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            self._delete_ids(actual_result)
            actual_result = self.sort_nested_list(actual_result, key='task_name')
            actual_result = sorted(actual_result, key=lambda doc:
                                   self._get_for_sort(doc))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"emp" : doc['item'], "tasks" : doc['items_nested']}
                               for doc in full_list if doc and 'items_nested' in doc]
            expected_result.extend([{"emp" : doc['item']}
                                    for doc in full_list if not 'items_nested' in doc])
            self._delete_ids(expected_result)
            expected_result = self.sort_nested_list(expected_result, key='task_name')
            expected_result = sorted(expected_result, key=lambda doc:
                                   self._get_for_sort(doc))
            self._verify_results(actual_result, expected_result)

    def test_simple_nest_key(self):
        for bucket in self.buckets:
            self.query = "SELECT * FROM %s emp %s NEST %s tasks " % (
                                                bucket.name, self.type_join, bucket.name) +\
                         "KEY emp.tasks_ids[0]"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc:
                                                            self._get_for_sort(doc))
            self._delete_ids(actual_result)
            full_list = self._generate_full_nested_docs_list(particular_key=0,
                                                             join_type=self.type_join)
            expected_result = [{"emp" : doc['item'], "tasks" : doc['items_nested']}
                               for doc in full_list if doc and 'items_nested' in doc]
            expected_result.extend([{"emp" : doc['item']}
                                    for doc in full_list if not 'items_nested' in doc])
            expected_result = sorted(expected_result, key=lambda doc:
                                                            self._get_for_sort(doc))
            self._delete_ids(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_with_array(self):
        for bucket in self.buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result)
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list if doc and 'items_nested' in doc]
            expected_result.extend([{} for doc in full_list if not 'items_nested' in doc])
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_prepared_nest_keys_with_array(self):
        for bucket in self.buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids"
            self.prepared_common_body()

    def test_nest_keys_where(self):
        for bucket in self.buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids where ANY item IN items SATISFIES item.project == 'CB' end"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result, key=lambda doc: (doc['name'], doc['projects']))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list if doc and 'items_nested' in doc and\
                               len([nested_doc for nested_doc in doc['items_nested']
                                    if nested_doc['project'] == 'CB']) > 0]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['projects']))
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_where_not_equal(self):
        for bucket in self.buckets:
            self.query = "select emp.name, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids where ANY item IN items SATISFIES item.project != 'CB' end"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result, key=lambda doc: (doc['name'], doc['projects']))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list if doc and 'items_nested' in doc and\
                               len([nested_doc for nested_doc in doc['items_nested']
                                    if nested_doc['project'] != 'CB']) > 0]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['projects']))
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_where_between(self):
        for bucket in self.buckets:
            self.query = "select emp.name, emp.join_day, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids where emp.join_day between 2 and 4"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result, key=lambda doc: (doc['name'], doc['projects']))
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'], "join_day" : doc['item']['join_day'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list
                               if doc and 'join_day' in doc['item'] and\
                               doc['item']['join_day'] >= 2 and doc['item']['join_day'] <=4]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['projects']))
            self._verify_results(actual_result, expected_result)

    def test_nest_keys_where_less_more_equal(self):
        for bucket in self.buckets:
            self.query = "select emp.name, emp.join_day, emp.join_yr, ARRAY item.project FOR item in items end projects " +\
                         "FROM %s emp %s NEST %s items " % (
                                                    bucket.name, self.type_join, bucket.name) +\
                         "ON KEYS emp.tasks_ids where emp.join_day <= 4 and emp.join_yr>=2010"
            actual_result = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_result['results'], key='projects')
            actual_result = sorted(actual_result)
            full_list = self._generate_full_nested_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['item']['name'], "join_day" : doc['item']['join_day'],
                                'join_yr': doc['item']['join_yr'],
                                "projects" : [nested_doc['project'] for nested_doc in doc['items_nested']]}
                               for doc in full_list
                               if doc and 'join_day' in doc['item'] and\
                               doc['item']['join_day'] <=4 and doc['item']['join_yr']>=2010]
            expected_result = self.sort_nested_list(expected_result, key='projects')
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def _get_for_sort(self, doc):
        if not 'emp' in doc:
            return ''
        if 'name' in doc['emp']:
            return doc['emp']['name'], doc['emp']['join_yr'],\
                   doc['emp']['join_mo'], doc['emp']['job_title']
        else:
            return doc['emp']['task_name']

    def _delete_ids(self, result):
        for item in result:
            if 'emp' in item:
                del item['emp']['_id']
            if 'tasks' in item:
                for task in item['tasks']:
                    if task and '_id' in task:
                        del task['_id']

    def generate_docs(self, docs_per_day, start=0):
        generators = []
        types = ['Engineer', 'Sales', 'Support']
        join_yr = [2010, 2011]
        join_mo = xrange(1, 12 + 1)
        join_day = xrange(1, 28 + 1)
        template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
        template += ' "job_title":"{4}", "tasks_ids":{5}}}'
        for info in types:
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        name = ["employee-%s" % (str(day))]
                        tasks_ids = ["test_task-%s" % day, "test_task-%s" % (day + 1)]
                        generators.append(DocumentGenerator("query-test-%s-%s-%s-%s" % (info, year, month, day),
                                               template,
                                               name, [year], [month], [day],
                                               [info], [tasks_ids],
                                               start=start, end=docs_per_day))
        return generators

    def generate_docs_tasks(self):
        generators = []
        start, end = 0, (28 + 1)
        template = '{{ "task_name":"{0}", "project": "{1}"}}'
        generators.append(DocumentGenerator("test_task", template,
                                            ["test_task-%s" % i for i in xrange(0,10)],
                                            ["CB"],
                                            start=start, end=10))
        generators.append(DocumentGenerator("test_task", template,
                                            ["test_task-%s" % i for i in xrange(10,20)],
                                            ["MB"],
                                            start=10, end=20))
        generators.append(DocumentGenerator("test_task", template,
                                            ["test_task-%s" % i for i in xrange(20,end)],
                                            ["IT"],
                                            start=20, end=end))
        return generators

    def _generate_full_joined_docs_list(self, join_type=JOIN_INNER,
                                        particular_key=None):
        joined_list = []
        all_docs_list = self.generate_full_docs_list(self.gens_load)
        if join_type.upper() == JOIN_INNER:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key is not None:
                    keys=[item["tasks_ids"][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                for tasks_item in tasks_items:
                    item_to_add = copy.deepcopy(item)
                    item_to_add.update(tasks_item)
                    joined_list.append(item_to_add)
        elif join_type.upper() == JOIN_LEFT:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key is not None:
                    keys=[item["tasks_ids"][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                for key in keys:
                    item_to_add = copy.deepcopy(item)
                    if key in [doc["_id"] for doc in tasks_items]:
                        item_to_add.update([doc for doc in tasks_items if key == doc['_id']][0])
                        joined_list.append(item_to_add)
            joined_list.extend([{}] * self.gens_tasks[-1].end)
        elif join_type.upper() == JOIN_RIGHT:
            raise Exception("RIGHT JOIN doen't exists in corrunt implementation")
        else:
            raise Exception("Unknown type of join")
        return joined_list

    def _generate_full_nested_docs_list(self, join_type=JOIN_INNER,
                                        particular_key=None):
        nested_list = []
        all_docs_list = self.generate_full_docs_list(self.gens_load)
        if join_type.upper() == JOIN_INNER:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key is not None:
                    keys=[item["tasks_ids"][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                if tasks_items:
                    nested_list.append({"items_nested" : tasks_items,
                                        "item" : item})
        elif join_type.upper() == JOIN_LEFT:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key is not None:
                    keys=[item["tasks_ids"][particular_key]]
                tasks_items = self.generate_full_docs_list(self.gens_tasks, keys=keys)
                if tasks_items:
                    nested_list.append({"items_nested" : tasks_items,
                                        "item" : item})
            tasks_doc_list = self.generate_full_docs_list(self.gens_tasks)
            for item in tasks_doc_list:
                nested_list.append({"item" : item})
        elif join_type.upper() == JOIN_RIGHT:
            raise Exception("RIGHT JOIN doen't exists in corrunt implementation")
        else:
            raise Exception("Unknown type of join")
        return nested_list
