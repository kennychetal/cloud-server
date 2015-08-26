def do_retrieve(rq_id, rq_name, rq_activities,boto):
	#State 0, nothing
	#State 1, only id is passed in,
	#State 2, only name is passed in
	#State 3, error state, both id, and name passed in
	state = set_state(rq_name, rq_id)
	status = 200
	r_json = {'id': rq_id, 'name': rq_name, 'activities': rq_activities, 'type' : 'person'}
	#Following JSON API SPEC
	json_api_spec = {}
	try:
		if(state==2):
			results = boto.query_2(name__eq=rq_name,index='EverythingIndex')
			#allowed to assume only one name.
			for rs_item in results:
				r_json['name'] = rs_item['name']
				r_json['id'] = rs_item['id']
				r_json['activities'] = rs_item['activities']
				status = 200
				json_api_spec['data'] = r_json
			#
			if(len(results._results)==0):
				status = 404
				errors = {}
				errors['not_found'] = {}
				errors['not_found']['name'] = rq_name
				errors['not_found']['status'] = status
				r_json = errors
				json_api_spec['errors'] = r_json

	except:
		print "Error in retrieve function"

	try:
		if(state==1):
			item = boto.get_item(id=rq_id)
			r_json['name'] = item['name']
			r_json['id'] = item['id']
			r_json['activities'] = item['activities']
			status = 200
			json_api_spec['data'] = r_json
	except:
		status = 404
		errors = {}
		errors['not_found'] = {}
		errors['not_found']['id'] = rq_id
		errors['not_found']['status'] = status
		r_json = errors
		json_api_spec['errors'] = r_json

	if(state==3):
		status = 404
		errors = {}
		errors['not_supported'] = {}
		errors['not_supported']['id'] = rq_id
		errors['not_supported']['name'] = rq_name
		errors['not_supported']['status'] = status
		r_json = errors
		json_api_spec['errors'] = r_json

	r_response = {'status': status, 'json': json_api_spec}

	return r_response

def set_state(rq_name,rq_id):
	state = 0
	if(str(rq_name)!="" and str(rq_id)!=""):
		state = 3
	elif(str(rq_id)!=""):
		state = 1
	elif(str(rq_name)!=""):
		state = 2
	return state