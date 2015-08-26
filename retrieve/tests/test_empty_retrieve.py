import unittest
import requests

class TestEmptyRetrieveMethods(unittest.TestCase):

  def test_retrieve_id1(self):
      response = requests.get(url="http://localhost:8080/retrieve?id=1")
      self.assertEqual('{u\'data\': u\'"command accepted"\'}', str(response.json()))

  def test_retrieve_id333(self):
      response = requests.get(url="http://localhost:8080/retrieve?id=333")
      self.assertEqual('{u\'data\': u\'"command accepted"\'}', str(response.json()))
  
  def test_both_id_and_name(self):
      response = requests.get(url="http://localhost:8080/retrieve?name=name_one&id=9999")
      self.assertEqual('{u\'data\': u\'"command accepted"\'}', str(response.json()))

if __name__ == '__main__':
    unittest.main()