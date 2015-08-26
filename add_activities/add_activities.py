def do_add_activities(uid, uactivities, boto):
	status=200
	ujson = {'type': "person", 'id': uid, 'added': uactivities}

	try:
		if((str(uid)!="")):
			item = boto.get_item(id=uid)

			added_list = [] + uactivities.split(",")
			acti_list = item['activities']

			for i in added_list:
				acti_list.append(i)

			ujson['type'] = item['type']
			ujson['id'] = item['id']
			ujson['added'] = uactivities.split(",")

			try:
				boto.put_item(data={
				'id': uid,
				'type': item['type'],
				'name': item['name'],
				'activities': acti_list,
				}, overwrite=True)

			except:
				print "Error in put_item"

			status = 200
	except:
		errors = {}
		errors['error'] = 'Item not found.'
		errors['id'] = uid
		ujson = errors
		status = 404

	add_activities_response = {'status': status, 'json': ujson}
	return add_activities_response
