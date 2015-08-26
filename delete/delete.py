def do_delete(rq_id, rq_name, rq_activities):
	status = 200
	d_json = {'id': rq_id, 'name': rq_name, 'activities': rq_activities, 'type' : 'person'}
	d_response = {'status': status, 'json': d_json}

	return d_response