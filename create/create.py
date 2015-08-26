import boto.dynamodb2
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER

def do_create(uid,uname,uactivities, boto):
 	try:
 		demo_user=boto.get_item(id=uid)
 		if demo_user['name']==uname and demo_user['activities']==uactivities:
 			ujson = {"data": {"type": "person", "id": uid, "links": {"self": "http://localhost:8080/retrieve?id=" + uid}}}
 			status=201
 			#print "**************user with same info exist**************"
 		else:
 			status=400
 			ujson = {"errors": [{"id_exists": {"status": status, "title": "id already exists", "detail": { "name": uname, "activities": uactivities}}}]}
 			#print "**************user with same uid but different info**************"
 	except:
 		status=201
 		ujson = {"data": {"type": "person", "id": uid, "links": {"self": "http://localhost:8080/retrieve?id=" + uid}}}
 		boto.put_item(data={
 			'id': uid,
 			'type': 'person',
			'name': uname,
			'activities': uactivities,
 		})
 		#print "**************user created**************"

	create_response = {'status': status, 'json': ujson}
	return create_response