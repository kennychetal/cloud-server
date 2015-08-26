import unittest
import requests
import time

class TestAddToQueueMethods(unittest.TestCase):

  def test_empty_queue_backend(self):
      response = requests.get(url="http://localhost:8081/")
      self.assertEqual("Queue is empty", str(response.json()))

  def test_queue_dummy_1(self):
      time.sleep(1) # Wait 1 second
      response = requests.get(url="http://localhost:8080/retrieve?name=dummy")
      self.assertEqual('{u\'data\': u\'"command accepted"\'}', str(response.json()))

  def test_queue_dummy_2(self):
      time.sleep(5) # Wait 5 seconds
      response = requests.get(url="http://localhost:8080/retrieve?name=dummy")
      self.assertEqual('{u\'data\': u\'"command accepted"\'}', str(response.json()))

#  def test_queue_dummy_3(self):
#      time.sleep(5) # Wait 5 seconds
#      response = requests.get(url="http://localhost:8080/retrieve?name=dummy")
#      self.assertEqual('{u\'data\': u\'"command accepted"\'}', str(response.json()))

class TestRetrieveFromQueueMethods(unittest.TestCase):

  def test_retrieve_1(self):
      time.sleep(5) # Wait 5 seconds
      response = requests.get(url="http://localhost:8081/")
      self.assertEqual("{u'status': 200, u'json': {u'data': {u'activities': [u'activity one'], u'type': u'person', u'id': u'3', u'name': u'dummy'}}}", str(response.json()))

  def test_retrieve_4_queue_now_empty2(self):
      time.sleep(7) # Wait 7 seconds
      response4 = requests.get(url="http://localhost:8081/")
      self.assertEqual("Queue is empty", str(response4.json()))

#  def test_retrieve_2(self):
#      time.sleep(7) # Wait 7 seconds
#      response = requests.get(url="http://localhost:8081/")
#      self.assertEqual("{u'status': 200, u'json': {u'data': {u'activities': [u'activity one'], u'type': u'person', u'id': u'3', u'name': u'dummy'}}}", str(response.json()))

  def test_retrieve_3(self):
      time.sleep(7) # Wait 7 seconds
      response1 = requests.get(url="http://localhost:8081/")
      self.assertEqual("{u'status': 200, u'json': {u'data': {u'activities': [u'activity one'], u'type': u'person', u'id': u'3', u'name': u'dummy'}}}", str(response1.json()))

  def test_retrieve_4_queue_now_empty(self):
      time.sleep(7) # Wait 7 seconds
      response4 = requests.get(url="http://localhost:8081/")
      self.assertEqual("Queue is empty", str(response4.json()))

if __name__ == '__main__':
    unittest.main()