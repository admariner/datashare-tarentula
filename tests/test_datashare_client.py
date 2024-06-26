import requests
import uuid

from .test_abstract import TestAbstract


class TestDatashareClient(TestAbstract):

    def test_default_index_creation(self):
        self.assertEqual(requests.get('%s/%s' % (self.elasticsearch_url, self.datashare_project)).status_code,
                         requests.codes.ok)
        self.assertNotEqual(requests.get('%s/%s' % (self.elasticsearch_url, 'unknown-index')).status_code,
                            requests.codes.ok)

    def test_temporary_project_creation(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            result = requests.get('%s/%s' % (self.elasticsearch_url, project))
            self.assertEqual(result.status_code, requests.codes.ok)

    def test_temporary_project_deletion(self):
        project = self.datashare_client.temporary_project(self.datashare_project)
        self.datashare_client.refresh()
        result = requests.get('%s/%s' % (self.elasticsearch_url, project))
        self.assertNotEqual(result.status_code, requests.codes.ok)

    def test_document_creation(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            id = self.datashare_client.index(index=project, document={'foo': 'bar'}, id=str(uuid.uuid4()))
            result = requests.get('%s/%s/_doc/%s' % (self.elasticsearch_url, project, id))
            self.assertEqual(result.status_code, requests.codes.ok)
            self.assertEqual(result.json().get('_source', {}).get('foo'), 'bar')

    def test_document_deletion(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'foo': 'bar'}, id='to-delete')
            result = requests.get('%s/%s/_doc/%s' % (self.elasticsearch_url, project, 'to-delete'))
            self.assertTrue(result.json().get('found'))
            self.datashare_client.delete(index=project, id='to-delete')
            result = requests.get('%s/%s/_doc/%s' % (self.elasticsearch_url, project, 'to-delete'))
            self.assertFalse(result.json().get('found'))

    def test_index_deletion(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            result = requests.get('%s/%s' % (self.elasticsearch_url, project))
            self.assertEqual(result.status_code, requests.codes.ok)
            self.datashare_client.delete_index(project)
            result = requests.get('%s/%s' % (self.elasticsearch_url, project))
            self.assertNotEqual(result.status_code, requests.codes.ok)

    def test_query_with_two_docs(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'name': 'Atypidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Migidae'}, id=str(uuid.uuid4()))
            total = self.datashare_client.query(index=project).get('hits', {}).get('total', {}).get('value', None)
            self.assertEqual(total, 2)

    def test_query_with_three_docs(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'name': 'Atypidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Euctenizidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Migidae'}, id=str(uuid.uuid4()))
            total = self.datashare_client.query(index=project).get('hits', {}).get('total', {}).get('value', None)
            self.assertEqual(total, 3)

    def test_query_has_limited_size(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'name': 'Atypidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Euctenizidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Migidae'}, id=str(uuid.uuid4()))
            hits = self.datashare_client.query(index=project, size=1).get('hits', {})
            self.assertEqual(hits['total']['value'], 3)
            self.assertEqual(len(hits['hits']), 1)

    def test_query_is_applied_and_get_all_documents(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'name': 'Atypidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Euctenizidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Migidae'}, id=str(uuid.uuid4()))
            total = self.datashare_client.query(index=project, q='name:*ae').get('hits', {}).get('total', {}).get('value', None)
            self.assertEqual(total, 3)

    def test_query_is_applied_and_get_one_document(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'name': 'Atypidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Euctenizidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Migidae'}, id=str(uuid.uuid4()))
            total = self.datashare_client.query(index=project, q='name:Migi*').get('hits', {}).get('total', {}).get('value', None)
            self.assertEqual(total, 1)

    def test_get_document(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'name': 'Atypidae'}, id='atypidae')
            document = self.datashare_client.document(index=project, id='atypidae')
            self.assertEqual(document.get('_source', {}).get('name'), 'Atypidae')

    def test_query_is_made_over_all_documents(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'name': 'Actinopodidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Antrodiaetidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Atracidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Atypidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Barychelidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Barychelidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Ctenizidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Cyrtaucheniidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Dipluridae'}, id=str(uuid.uuid4()))

            count = 0
            names = [ 'Actinopodidae', 'Antrodiaetidae', 'Atracidae', 'Atypidae', 'Barychelidae', 'Barychelidae', 'Ctenizidae', 'Cyrtaucheniidae', 'Dipluridae', ]
            for document in self.datashare_client.query_all(index=project, q='name:*', size=2, limit=9):
                count = count + 1
                self.assertIn(document['_source']['name'], names)
            self.assertEqual(count, 9)


    def test_query_sorting_by(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'name': 'Actinopodidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Antrodiaetidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Atracidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Atypidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Barychelidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Barychelidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Ctenizidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Cyrtaucheniidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Dipluridae'}, id=str(uuid.uuid4()))

            # desc
            sort = [
                { "_score" : "desc" },
            ]
            results = self.datashare_client.query_all(index=project, q='name:*', 
                                                        sort=sort,
                                                        size=8)
            prev_value = None
            for document in results:
                if prev_value:
                    self.assertTrue(prev_value <= document['_score'])
                prev_value = document['_score']

            # asc
            sort = [
                { "_id" : "asc" },
            ]
            results = self.datashare_client.query_all(index=project, q='name:*', 
                                                        sort=sort,
                                                        size=8)
            prev_value = None
            for document in results:
                if prev_value:
                    self.assertTrue(prev_value >= document['_id'])
                prev_value = document['_score']

    def test_scan_is_made_over_all_documents(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            self.datashare_client.index(index=project, document={'name': 'Actinopodidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Antrodiaetidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Atracidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Atypidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Barychelidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Barychelidae'}, id=str(uuid.uuid4()))
            self.datashare_client.index(index=project, document={'name': 'Ctenizidae'}, id=str(uuid.uuid4()))

            count = 0
            for document in self.datashare_client.scan_all(index=project, q='name:*', size=2):
                count = count + 1
            self.assertEqual(count, 7)

    def test_count_with_multiple_fields_query(self):
        with self.datashare_client.temporary_project(self.datashare_project) as project:
            query = {'query': {'bool': {'must': [{'term': {'contentType': 'application/pdf'}}]}}, 'runtime_mappings': {}}
            try:
                self.datashare_client.count(index=project, query=query)
            except Exception as e:
                msg = f"Error when counting with a complex query: {e}: {e.__traceback__}"
                self.fail(msg)
