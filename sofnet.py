import utils

# To maintain track of current usage with total capacity of a resource
UZI = {}

IS_VERBOSE = True


def fetch_resource_available_slot(resource_logs, resource_id, verbose=IS_VERBOSE):
    '''
    To return the time at which the given resource becomes available again 
    '''
    available_from = resource_logs[resource_id][-1]['end_time'] if resource_logs[resource_id] else 0
    if verbose: print(f'{resource_id} is available from {available_from}')
    return available_from


def schedule_on_fog(architecture, job, runtime_values, verbose=IS_VERBOSE):
    F = architecture['F']
    fdc_id = runtime_values['resource_id']
    resource_logs = architecture['resource_logs']

    # Schedule the job on the resource
    resource_logs[fdc_id].append({'job_id': job['id'],
                                'start_time': runtime_values['start_time'],
                                'end_time': runtime_values['end_time']})
    
    job_size = 64 * job['instructions']
    F[fdc_id]['used_capacity'] += job_size

    architecture['executed_jobs'][job['id']] = {'resource': fdc_id, 'end_time': runtime_values['end_time']}
    architecture['end_at'] = max(architecture['end_at'], runtime_values['end_time'])

    if verbose: print(f"{job['id']} scheduled on {fdc_id} from {runtime_values['start_time']} to {runtime_values['end_time']}")
    return architecture


def schedule_on_cloud(architecture, job, runtime_values, verbose=IS_VERBOSE):
    C = architecture['C']
    cdc_id = runtime_values['resource_id']
    resource_logs = architecture['resource_logs']

    # Schedule the job on the resource
    resource_logs[cdc_id].append({'job_id': job['id'],
                                'start_time': runtime_values['start_time'],
                                'end_time': runtime_values['end_time']})
    
    job_size = 64 * job['instructions']
    C[cdc_id]['used_capacity'] += job_size

    architecture['executed_jobs'][job['id']] = {'resource': cdc_id, 'end_time': runtime_values['end_time']}
    architecture['end_at'] = max(architecture['end_at'], runtime_values['end_time'])

    if verbose: print(f"{job['id']} scheduled on {cdc_id} from {runtime_values['start_time']} to {runtime_values['end_time']}")
    return architecture


def check_fdc_deadline_constraint(architecture, job, is_native=True, verbose=IS_VERBOSE):
    F = architecture['F']
    EU = architecture['EU']
    resource_logs = architecture['resource_logs']

    # Extract the native/public fdc id
    fdc_id = utils.fetch_native_fog_for_job(EU, job) if is_native else utils.fetch_public_fog_for_job(EU, job)
    resource = F[fdc_id]

    if verbose: print(f"Checking deadline constraint for {job['id']} on {fdc_id}...")

    # Check for resource availability
    start_time = max(job['arrival_time'], fetch_resource_available_slot(resource_logs, fdc_id))

    # Calculate the job execution time and the communication delay
    latency_delay = utils.fetch_communication_delay(job, resource, EU[job['eu']], F[fdc_id])
    runtime = utils.calculate_runtime(resource, job)
    end_time = start_time + runtime + latency_delay

    runtime_values = {'job_id': job['id'], 'resource_id': fdc_id, 'start_time': start_time, 'end_time': end_time}
    if verbose: print('runtime_values', runtime_values)

    return end_time <= job['deadline'], runtime_values


def check_fdc_space_constraint(architecture, job, is_native=True, verbose=IS_VERBOSE):
    job_size = 64 * job['instructions']
    F = architecture['F']
    EU = architecture['EU']
    fdc_id = utils.fetch_native_fog_for_job(EU, job) if is_native else utils.fetch_public_fog_for_job(EU, job)
    available_resource_capacity = F[fdc_id]['total_capacity'] - F[fdc_id]['used_capacity']
    if verbose: print(f'{job_size} | Required: {job_size} | Available @{fdc_id}: {available_resource_capacity}')
    return job_size <= available_resource_capacity


def check_cdc_deadline_constraint(architecture, job, is_native=True, verbose=IS_VERBOSE):
    C = architecture['C']
    F = architecture['F']
    EU = architecture['EU']
    resource_logs = architecture['resource_logs']

    # Extract the native/public cdc id
    cdc_id = utils.fetch_native_cloud_for_job(F, EU, job) if is_native else utils.fetch_public_cloud_for_job(F, EU, job)
    resource = C[cdc_id]

    if verbose: print(f"Checking deadline constraint for {job['id']} on {cdc_id}...")

    # Check for resource availability
    start_time = max(job['arrival_time'], fetch_resource_available_slot(resource_logs, cdc_id))

    latency_delay = utils.fetch_communication_delay(job, resource, EU[job['eu']], C[cdc_id])
    runtime = utils.calculate_runtime(resource, job)
    end_time = start_time + runtime + latency_delay

    runtime_values = {'job_id': job['id'], 'resource_id': cdc_id, 'start_time': start_time, 'end_time': end_time}
    if verbose: print('runtime_values', runtime_values)

    return end_time <= job['deadline'], runtime_values


def check_cdc_space_constraint(architecture, job, is_native=True, verbose=IS_VERBOSE):
    job_size = 64 * job['instructions']
    C = architecture['C']
    F = architecture['F']
    EU = architecture['EU']
    cdc_id = utils.fetch_native_cloud_for_job(F, EU, job) if is_native else utils.fetch_public_cloud_for_job(F, EU, job)
    available_resource_capacity = C[cdc_id]['total_capacity'] - C[cdc_id]['used_capacity']
    if verbose: print(f'Required: {job_size} | Available @{cdc_id}: {available_resource_capacity}')
    return job_size <= available_resource_capacity


def calculate_utilization(resource, verbose=IS_VERBOSE):
    UZI = resource['used_capacity'] / resource['total_capacity']
    if verbose: print(f"@{resource['id']} UZI: {UZI}")
    return UZI


# Procedure 1 Initial allocation of classified jobs
# 1: procedure CLASSIFIED()
# 2: for selected job j having tag = tc do
def allocate_classified_jobs(architecture, job):
    z_score = job['z_score']
    z_score_threshold = architecture['z_score_threshold']

    is_native_fog_deadline_constraint_satisfied, fn_runtime_values = check_fdc_deadline_constraint(architecture, job, True)
    is_native_fog_space_constraint_satisfied = check_fdc_space_constraint(architecture, job, True)

    # 3: if (D1 & S1 holds on fn) && (z-score < 0.5) then
    if (is_native_fog_deadline_constraint_satisfied and is_native_fog_space_constraint_satisfied) and z_score <= z_score_threshold:

        # 4: schedule j on fn
        architecture = schedule_on_fog(architecture, job, fn_runtime_values)
        return architecture

    is_native_cloud_deadline_constraint_satisfied, cn_runtime_values = check_cdc_deadline_constraint(architecture, job)
    is_native_cloud_space_constraint_satisfied = check_cdc_space_constraint(architecture, job)

    # 5: else if (D2 & S2 holds on cn) && (z-score > 0.5) then
    if (is_native_cloud_deadline_constraint_satisfied and is_native_cloud_space_constraint_satisfied) and z_score > z_score_threshold:
        
        # 6: schedule j on cn
        architecture = schedule_on_cloud(architecture, job, cn_runtime_values)

		# 7: else if (D1 or S1 does not holds on fn) then
    elif not is_native_fog_deadline_constraint_satisfied or not is_native_fog_space_constraint_satisfied:
			
        # 8: MIGRATION()
        architecture = migration(architecture, job, fn_runtime_values=fn_runtime_values, cn_runtime_values=cn_runtime_values)

		# 9: else
    else:
        # 10: try scheduling j later
        pass
    # 11: end if

    return architecture
	# 12: end for
# 13: end procedure


# Procedure 2 Initial allocation of restricted jobs
# 1: procedure RESTRICTED()
# 2: for selected job j having tag = tr do
def allocate_restricted_jobs(architecture, job):
    C = architecture['C']
    F = architecture['F']
    EU = architecture['EU']
    cdc_id = utils.fetch_native_cloud_for_job(F, EU, job)

    # Calculate the quantities
    SF = architecture['SF']
    z_score_threshold = architecture['z_score_threshold']
    z_score = job['z_score']
    cdc_UZI = calculate_utilization(C[cdc_id])

    # Evaluate the constraints
    is_public_fog_deadline_constraint_satisfied, fp_runtime_values = check_fdc_deadline_constraint(architecture, job, is_native=False)
    is_public_fog_space_constraint_satisfied = check_fdc_space_constraint(architecture, job, is_native=False)

    # 3: if (D1 & S1 holds on fp) && (z-score < 0.5) then
    if (is_public_fog_deadline_constraint_satisfied and is_public_fog_space_constraint_satisfied) and z_score <= z_score_threshold:

        # 4: schedule j on fp
        architecture = schedule_on_fog(architecture, job, fp_runtime_values)
        return architecture
		
    is_native_cloud_deadline_constraint_satisfied, cn_runtime_values = check_cdc_deadline_constraint(architecture, job)
    is_native_cloud_space_constraint_satisfied = check_cdc_space_constraint(architecture, job)

    # 5: else if (D2 & S2 holds on cn) && (z-score > 0.5) && (UZI < SF) then
    if (is_native_cloud_deadline_constraint_satisfied and is_native_cloud_space_constraint_satisfied) and z_score > z_score_threshold and cdc_UZI <= SF:

        # 6: schedule j on cn
        architecture = schedule_on_cloud(architecture, job, cn_runtime_values)

    # 7: else if (D2 & S2 holds on cn) && (z-score > 0.5) && (UZI > SF) then
    elif (is_native_cloud_deadline_constraint_satisfied and is_native_cloud_space_constraint_satisfied) and z_score > z_score_threshold and cdc_UZI > SF:

        # 8: try scheduling j later
        pass
	
    # 9: else if (D1 or S1 does not holds on fp) then
    elif not is_public_fog_deadline_constraint_satisfied or not is_public_fog_space_constraint_satisfied:

        # 10: MIGRATION()
        architecture = migration(architecture, job, fp_runtime_values=fp_runtime_values, cn_runtime_values=cn_runtime_values)

    # 11: else
    else:
     
        # 12: try scheduling j later
        pass
    
    # 13: end if
    
    return architecture
	# 14: end for
# 15: end procedure


# Procedure 3 Initial allocation of public jobs
# 1: procedure PUBLIC()
# 2: for selected job j having tag = tp do
def allocate_public_jobs(architecture, job):
    F = architecture['F']
    EU = architecture['EU']

    # Calculate the quantities
    z_score = job['z_score']
    SF = architecture['SF']
    z_score_threshold = architecture['z_score_threshold']
    public_fdc_id = utils.fetch_public_fog_for_job(EU, job)
    public_fdc_UZI = calculate_utilization(F[public_fdc_id])

    # Evaluate the constraints
    is_public_fog_deadline_constraint_satisfied, fp_runtime_values = check_fdc_deadline_constraint(architecture, job, is_native=False)
    is_public_fog_space_constraint_satisfied = check_fdc_space_constraint(architecture, job, is_native=False)

    # 3: if (D1 & S1 holds on fp) && (z-score < 0.5) && (UZI < SF) then
    if (is_public_fog_deadline_constraint_satisfied and is_public_fog_space_constraint_satisfied) and z_score <= z_score_threshold and public_fdc_UZI <= SF:

        # 4: schedule j on fp
        architecture = schedule_on_fog(architecture, job, fp_runtime_values)
        return architecture
    
    is_public_cloud_deadline_constraint_satisfied, cp_runtime_values = check_cdc_deadline_constraint(architecture, job, is_native=False)
    is_public_cloud_space_constraint_satisfied = check_cdc_space_constraint(architecture, job, is_native=False)

    # is_native_cloud_deadline_constraint_satisfied, cn_runtime_values = check_cdc_deadline_constraint(architecture, job, is_native=True)
    # is_native_cloud_space_constraint_satisfied = check_cdc_space_constraint(architecture, job, is_native=True)


    # 5: else if (D1 & S1 holds on fp) && (z-score < 0.5) && (UZI > SF) then
    if (is_public_fog_deadline_constraint_satisfied and is_public_fog_space_constraint_satisfied) and z_score <= z_score_threshold and public_fdc_UZI > SF:

        # 6: schedule j on cp
        architecture = schedule_on_cloud(architecture, job, cp_runtime_values)

    # 7: else if (D2 & S2 holds on cn) && (z-score > 0.5) then
    elif (is_public_cloud_deadline_constraint_satisfied and is_public_cloud_space_constraint_satisfied) and z_score > z_score_threshold:

        # 8: schedule j on cp
        architecture = schedule_on_cloud(architecture, job, cp_runtime_values)

    # 9: else if (D1 or S1 does not holds on fp) then
    elif not is_public_fog_deadline_constraint_satisfied or not is_public_fog_space_constraint_satisfied:

        # 10: MIGRATION()
        architecture = migration(architecture, job, fp_runtime_values=fp_runtime_values, cp_runtime_values=cp_runtime_values)

    # 11: end if

    return architecture
    # 12: end for
# 13: end procedure
	

def fetch_waiting_bit(r1, r2):
    '''
    To decide whether the job should migrate to another resource or wait at initially allocated resource  
    '''
    return 1 if r1['end_time'] <= r2['end_time'] else 0


# Procedure 4 Job Migration
# 1: procedure MIGRATION()
def migration(architecture, job, fn_runtime_values=None, fp_runtime_values=None, cn_runtime_values=None, cp_runtime_values=None):
    C = architecture['C']
    F = architecture['F']
    EU = architecture['EU']
    SF = architecture['SF']

    # 2: if WB = 1 then
        # 3: do not migrate
    # 4: end if
    
    # 5: for job j has tag = tc && WB = 0 do
    if job['category'] == "tc":
        # Calculate the waiting bit
        WB = fetch_waiting_bit(fn_runtime_values, cn_runtime_values)
        
        # 6: migrate j from fn to cn
        if WB:
            architecture = schedule_on_fog(architecture, job, fn_runtime_values)
        else:
            architecture = schedule_on_cloud(architecture, job, cn_runtime_values)

    # 7: end for

    # 8: for job j has tag = tc do
    elif job['category'] == "tr":
        fn_id = utils.fetch_native_fog_for_job(EU, job)
        fn_UZI = calculate_utilization(F[fn_id])
        _, runtime_values = check_fdc_deadline_constraint(architecture, job, is_native=True)
        WB_1 = fetch_waiting_bit(fp_runtime_values, runtime_values)
        WB_2 = fetch_waiting_bit(fp_runtime_values, cn_runtime_values)

        # 9: if fn has UZI < SF && WB = 0 then
        if fn_UZI <= SF and not WB_1:

            # 10: migrate j from fp to fn
            architecture = schedule_on_fog(architecture, job, runtime_values)

        # 11: else if WB = 0 then
        elif not WB_2:

            # 12: migrate j from fp to cn
                architecture = schedule_on_cloud(architecture, job, cn_runtime_values)

        else:
            architecture = schedule_on_fog(architecture, job, fp_runtime_values)

        # 13: end if

    # 14: end for

    # 15: for job j has tag = tp do
    if job['category'] == "tp":
        fn_id = utils.fetch_native_fog_for_job(EU, 
                                               job)
        fn_UZI = calculate_utilization(F[fn_id])
        cn_id = utils.fetch_native_cloud_for_job(F, EU, job)
        cn_UZI = calculate_utilization(C[cn_id])

        _, runtime_values_1 = check_fdc_deadline_constraint(architecture, job, is_native=True)
        _, runtime_values_2 = check_cdc_deadline_constraint(architecture, job, is_native=True)

        WB_1 = fetch_waiting_bit(fp_runtime_values, runtime_values_1)
        WB_2 = fetch_waiting_bit(fp_runtime_values, runtime_values_2)
        WB_3 = fetch_waiting_bit(fp_runtime_values, cp_runtime_values)

        # 16: if fn has UZI < SF && WB = 0 then
        if fn_UZI <= SF and not WB_1:
            
            # 17: migrate j from fp to fn
            architecture = schedule_on_fog(architecture, job, runtime_values_1)

        # 18: else if cn has UZI < SF && WB = 0 then
        elif cn_UZI <= SF and not WB_2:
            
            # 19: migrate j from fp to cn
            architecture = schedule_on_cloud(architecture, job, runtime_values_2)

        # 20: else if WB = 0 then
        elif not WB_3:

            # 21: migrate j from fp to cp
            architecture = schedule_on_cloud(architecture, job, cp_runtime_values)
        
        else:
            architecture = schedule_on_fog(architecture, job, fp_runtime_values)

        # 22: end if
    # 23: end for
    
    return architecture
# 24: end procedure


# Algorithm 1 SOFNET: Load Balancing & Scheduling
# Input: IoT data in job queue sorted in increasing order of their deadlines.
# Output: Data scheduled on fog or cloud node.
# 1: procedure ALGORITHM()
def algorithm(architecture, jobs):
    total_jobs = len(jobs)
    
    # 2: Compute quantities (mention here specifically).
    # 3: Specify SF threshold values for fog and cloud nodes.
    job_deadlines = [job['deadline'] for job in jobs]

    # Check for empty job queue
    if not job_deadlines:
        return architecture

    d_min = min(job_deadlines)
    d_max = max(job_deadlines)

    # For each job in the job queue
    for i in range(total_jobs):
        print('-'*100)

        if d_max > d_min: jobs[i]['z_score'] = (jobs[i]['deadline'] - d_min) / (d_max - d_min)
        else: jobs[i]['z_score'] = 0.0

        # 4: for selected job j having tag = tc do
        if jobs[i]['category'] == "tc":
        
            # 5: CLASSIFIED()
            architecture = allocate_classified_jobs(architecture, jobs[i])

        # 7: for selected job j having tag = tr do
        elif jobs[i]['category'] == "tr":
        
            # 8: RESTRICTED()
            architecture = allocate_restricted_jobs(architecture, jobs[i])

        # 10: for selected job j having tag = tp do
        elif jobs[i]['category'] == "tp":

            # 11: PUBLIC()
            architecture = allocate_public_jobs(architecture, jobs[i])

    # 6: end for
    # 9: end for
    # 12: end for

    return architecture
# 13: end procedure



def schedule_on_fdc_only(architecture, fdc_id, job, verbose=IS_VERBOSE):
    F = architecture['F']
    EU = architecture['EU']

    resource = F[fdc_id]
    resource_logs = architecture['resource_logs']

    # Check for resource availability
    start_time = max(job['arrival_time'], fetch_resource_available_slot(resource_logs, fdc_id))

    # Calculate the job execution time and the communication delay
    latency_delay = utils.fetch_communication_delay(job, resource, EU[job['eu']], F[fdc_id])
    runtime = utils.calculate_runtime(resource, job)
    end_time = start_time + runtime + latency_delay

    # Schedule the job on the resource
    resource_logs[fdc_id].append({'job_id': job['id'],
                                'start_time': start_time,
                                'end_time': end_time})
    
    job_size = 64 * job['instructions']
    F[fdc_id]['used_capacity'] += job_size

    architecture['executed_jobs'][job['id']] = {'resource': fdc_id, 'end_time': end_time}
    architecture['end_at'] = max(architecture['end_at'], end_time)

    if verbose: print(f"{job['id']} scheduled on {fdc_id} from {start_time} to {end_time}")
    return architecture



def fdc_algorithm(architecture, jobs):
    EU = architecture['EU']

    for job in jobs:
        fn_id = utils.fetch_native_fog_for_job(EU, job)
        architecture = schedule_on_fdc_only(architecture, fn_id, job)

    return architecture


def schedule_on_cdc_only(architecture, cdc_id, job, verbose=IS_VERBOSE):
# def schedule_on_fog(architecture, job, runtime_values, verbose=IS_VERBOSE):
    C = architecture['C']
    EU = architecture['EU']

    resource = C[cdc_id]
    resource_logs = architecture['resource_logs']

    # Check for resource availability
    start_time = max(job['arrival_time'], fetch_resource_available_slot(resource_logs, cdc_id))

    # Calculate the job execution time and the communication delay
    latency_delay = utils.fetch_communication_delay(job, resource, EU[job['eu']], C[cdc_id])
    runtime = utils.calculate_runtime(resource, job)
    end_time = start_time + runtime + latency_delay

    # Schedule the job on the resource
    resource_logs[cdc_id].append({'job_id': job['id'],
                                'start_time': start_time,
                                'end_time': end_time})
    
    job_size = 64 * job['instructions']
    C[cdc_id]['used_capacity'] += job_size

    architecture['executed_jobs'][job['id']] = {'resource': cdc_id, 'end_time': end_time}
    architecture['end_at'] = max(architecture['end_at'], end_time)

    if verbose: print(f"{job['id']} scheduled on {cdc_id} from {start_time} to {end_time}")
    return architecture



def cdc_algorithm(architecture, jobs):
    F = architecture['F']
    EU = architecture['EU']

    for job in jobs:
        cn_id = utils.fetch_native_cloud_for_job(F, EU, job)
        architecture = schedule_on_cdc_only(architecture, cn_id, job)

    return architecture


