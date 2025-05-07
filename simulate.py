import math
import time
import sofnet
import pandas as pd
import utils


def register_resources(resources):
    resources_dict = {}
    for resource in resources:
        resources_dict[resource['id']] = resource
    return resources_dict


def create_job_sequence(workload):
    job_sequence = []
    for i, job in workload.iterrows():
        job_sequence.append(job['id'])

    return job_sequence


def insert_jobs_in_queue(job_ids, workload):
    job_queue = []

    for job_id in job_ids:
        job_queue.append(workload[job_id])

    return job_queue


def compute_quantities(workload):
    quantities = {}
    quantities['d_min'] = min([job['deadline'] for job_id, job in workload.items()])
    quantities['d_max'] = max([job['deadline'] for job_id, job in workload.items()])
    quantities['SF'] = 0.5
    return quantities


def initialize_resource_logs(C, F):
    resource_logs = {}

    for cdc_id in C.keys():
        resource_logs[cdc_id] = []

    for fdc_id in F.keys():
        resource_logs[fdc_id] = []
    
    return resource_logs


def initialize_resource_utilization(C, F):
    resource_utilization = {}

    for cdc_id in C.keys():
        resource_utilization[cdc_id] = 0.0

    for fdc_id in F.keys():
        resource_utilization[fdc_id] = 0.0
    
    return resource_utilization


def update_resource_utilization(architecture):
    C, F, resource_utilization = architecture['C'], architecture['F'], architecture['resource_utilization']
    for resource_id in resource_utilization.keys():
        if 'fdc' in resource_id:
            resource_utilization[resource_id] += 100 * F[resource_id]['used_capacity'] / F[resource_id]['total_capacity']
        else:
            resource_utilization[resource_id] += 100 * C[resource_id]['used_capacity'] / C[resource_id]['total_capacity']

    return resource_utilization


def display_cdc_data(nodes):
    for id, node in nodes.items():
        print(f"{id} | {node['x_coordinate']} | {node['y_coordinate']}")


def display_native_and_public_cdc(F):
    for id, node in F.items():
        print(f"{id} | {node['native_cdc_id']} | {node['public_cdc_id']}")


def display_native_and_public_fdc(EU):
    for id, node in EU.items():
        print(f"{id} | {node['native_fdc_id']} | {node['public_fdc_id']}")


def display_resource_logs(resource_logs):
    for resource_id, jobs in resource_logs.items():
        print(f'{resource_id} has {len(jobs)} jobs')

        for job in jobs:
            print(f"{job['job_id']} ran from {job['start_time']} to {job['end_time']}")
    
        # print('-' * 100)


def setup_architecture(C, F, EU):
    # Map the native and public resources
    F = utils.map_fog_to_cloud(F, C)
    EU = utils.map_end_user_to_fog(EU, F)

    # Define the network architecture
    architecture = {'C': C,
                    'F': F,
                    'EU': EU,
                    'resource_logs': initialize_resource_logs(C, F),
                    'resource_utilization': initialize_resource_utilization(C, F),
                    'executed_jobs': {},
                    'completed_jobs': [],
                    'end_at': -1}
    
    return architecture


def setup_fdc_architecture(C, F, EU):
    # Map the native and public resources
    F = utils.map_fog_to_cloud(F, C)
    EU = utils.map_end_user_to_fog(EU, F)

    # Define the network architecture
    architecture = {'C': C,
                    'F': F,
                    'EU': EU,
                    'resource_logs': initialize_resource_logs(C, F),
                    'resource_utilization': initialize_resource_utilization(C, F),
                    'executed_jobs': {},
                    'completed_jobs': [],
                    'end_at': -1}
    
    return architecture


def setup_cdc_architecture(C, F, EU):
    # Map the native and public resources
    F = utils.map_fog_to_cloud(F, C)
    EU = utils.map_end_user_to_fog(EU, F)

    # Define the network architecture
    architecture = {'C': C,
                    'F': F,
                    'EU': EU,
                    'resource_logs': initialize_resource_logs(C, F),
                    'resource_utilization': initialize_resource_utilization(C, F),
                    'executed_jobs': {},
                    'completed_jobs': [],
                    'end_at': -1}
    
    return architecture


def display_final_job_resource_pair(executed_jobs):

    for i in range(len(executed_jobs)):
        job_id = f'job_{i+1}'
        print(f"{job_id}: {executed_jobs[job_id]['resource']}")
    return


def free_resource_post_job_completion(architecture, all_jobs, counter):
    '''
    To free resource storage post job completion
    '''
    # For each job in execution
    for job_id, job_info in architecture['executed_jobs'].items():

        # Check for completion
        if job_info['end_time'] == counter:

            # Free the resource
            job_resource_id = job_info['resource']
            job_size = 64 * all_jobs[job_id]['instructions']
            resource = F[job_resource_id] if 'f' in job_resource_id else C[job_resource_id]
            resource['used_capacity'] -= job_size

    return architecture


def run_simulation(workload, architecture):
    # Represents real-time clock
    counter = 0
    job_queue = []
    remaining_jobs = workload.copy()
    all_jobs = register_resources(workload.to_dict('records'))

    # Until all jobs are executed
    while True:
    
        # Until all new jobs are added to job queue
        while len(remaining_jobs):
            top_job = remaining_jobs.iloc[0]

            if top_job['arrival_time'] == counter: 
                job_queue.append(all_jobs[top_job['id']])
                remaining_jobs = remaining_jobs.iloc[1:, :]
            else: break

        # Schedule the jobs in the job queue
        architecture = sofnet.algorithm(architecture, job_queue)

        job_queue = [job for job in job_queue if job['id'] not in architecture['executed_jobs'].keys()]

        architecture['resource_utilization'] = update_resource_utilization(architecture)

        # Free resource storage post job completion
        architecture = free_resource_post_job_completion(architecture, all_jobs, counter)

        # if not counter % 1000:
        #     print('counter:', counter)
        #     print("architecture['end_at']:", architecture['end_at'])
        #     print('len(remaining_jobs):', len(remaining_jobs))

        # Check for end of simulation
        #0<=15
        if counter <= architecture['end_at'] or len(remaining_jobs): counter += 1
        else: break
    
    SR = utils.calculate_success_ratio(all_jobs, architecture['executed_jobs'])
    print(f'Success Ratio: {SR}')

    SC = utils.calculate_system_cost(architecture)
    print(f'System Cost: {SC}')

    RU = utils.calculate_resource_utilization(architecture)
    print(f'Resource Utilization: {RU}')

    # display_final_job_resource_pair(architecture['executed_jobs'])
    # display_resource_logs(architecture['resource_logs'])

    performance_ratio =  {'SR': SR,
                        'SC': SC,
                        'RU': RU}

    return performance_ratio


def run_fdc_simulation(workload, architecture):
    # Represents real-time clock
    counter = 0
    job_queue = []
    remaining_jobs = workload.copy()
    all_jobs = register_resources(workload.to_dict('records'))

    # Until all jobs are executed
    while True:
    
        # Until all new jobs are added to job queue
        while len(remaining_jobs):
            top_job = remaining_jobs.iloc[0]

            if top_job['arrival_time'] == counter: 
                job_queue.append(all_jobs[top_job['id']])
                remaining_jobs = remaining_jobs.iloc[1:, :]
            else: break

        # Schedule the jobs in the job queue
        architecture = sofnet.fdc_algorithm(architecture, job_queue)

        job_queue = [job for job in job_queue if job['id'] not in architecture['executed_jobs'].keys()]

        # Free resource storage post job completion
        architecture = free_resource_post_job_completion(architecture, all_jobs, counter)

        # if not counter % 1000:
        #     print('counter:', counter)
        #     print("architecture['end_at']:", architecture['end_at'])
        #     print('len(remaining_jobs):', len(remaining_jobs))

        # Check for end of simulation
        #0<=15
        if counter <= architecture['end_at'] or len(remaining_jobs): counter += 1
        else: break
    
    SR = utils.calculate_success_ratio(all_jobs, architecture['executed_jobs'])
    print(f'Success Ratio: {SR}')

    SC = utils.calculate_system_cost(architecture)
    print(f'System Cost: {SC}')

    RU = utils.calculate_resource_utilization(architecture)
    print(f'Resource Utilization: {RU}')

    # display_final_job_resource_pair(architecture['executed_jobs'])
    # display_resource_logs(architecture['resource_logs'])

    performance_ratio =  {'SR': SR,
                        'SC': SC,
                        'RU': RU}

    return performance_ratio


def run_cdc_simulation(workload, architecture):
    # Represents real-time clock
    counter = 0
    job_queue = []
    remaining_jobs = workload.copy()
    all_jobs = register_resources(workload.to_dict('records'))

    # Until all jobs are executed
    while True:
    
        # Until all new jobs are added to job queue
        while len(remaining_jobs):
            top_job = remaining_jobs.iloc[0]

            if top_job['arrival_time'] == counter: 
                job_queue.append(all_jobs[top_job['id']])
                remaining_jobs = remaining_jobs.iloc[1:, :]
            else: break

        # Schedule the jobs in the job queue
        architecture = sofnet.cdc_algorithm(architecture, job_queue)

        job_queue = [job for job in job_queue if job['id'] not in architecture['executed_jobs'].keys()]

        architecture['resource_utilization'] = update_resource_utilization(architecture)

        # Free resource storage post job completion
        architecture = free_resource_post_job_completion(architecture, all_jobs, counter)

        # if not counter % 1000:
        #     print('counter:', counter)
        #     print("architecture['end_at']:", architecture['end_at'])
        #     print('len(remaining_jobs):', len(remaining_jobs))

        # Check for end of simulation
        #0<=15
        if counter <= architecture['end_at'] or len(remaining_jobs): counter += 1
        else: break
    
    SR = utils.calculate_success_ratio(all_jobs, architecture['executed_jobs'])
    print(f'Success Ratio: {SR}')

    SC = utils.calculate_system_cost(architecture)
    print(f'System Cost: {SC}')

    RU = utils.calculate_resource_utilization(architecture)
    print(f'Resource Utilization: {RU}')

    # display_final_job_resource_pair(architecture['executed_jobs'])
    # display_resource_logs(architecture['resource_logs'])

    performance_ratio =  {'SR': SR,
                        'SC': SC,
                        'RU': RU}

    return performance_ratio


# def fetch_ledger():  
#     return


def end_test(C, F):
    for id, fdc in F.items():
        print(id, fdc['used_capacity'])

    for id, cdc in C.items():
        print(id, cdc['used_capacity'])
    
    return


# def trigger_simulations(workload, z_score_thresholds=None, SF_thresholds=None):
#     default_threshold = 0.5
#     architecture = setup_architecture(C, F, EU)
#     SF_thresholds = [0.1 * i for i in range(11)]

#     for sf_threshold in SF_thresholds:
#         architecture['SF'] = sf_threshold
#         architecture['z_score_threshold'] = default_threshold
#         performance_ratio = run_simulation(workload, architecture)
#         break
        
#     print(performance_ratio)

#     return performance_ratio


# Import the files
C = register_resources(pd.read_csv('files/CDCs.csv').to_dict('records'))
F = register_resources(pd.read_csv('files/FDCs.csv').to_dict('records'))
EU = register_resources(pd.read_csv('files/EUs.csv').to_dict('records'))

# Sort the workload based on deadline and arrival time
workload = pd.read_csv('files/workload.csv').sort_values(by=['deadline', 'arrival_time'])

z_score_thresholds = [0.1 * i for i in range(11)]
SF_thresholds = [0.1 * i for i in range(11)]

X = [round(i/10, 1) for i in range(11)]
SR_Y = []
SC_Y = []
RU_Y = []

for i in range(11):
    architecture = setup_architecture(C, F, EU)
    architecture['SF'] = 0.5
    architecture['z_score_threshold'] = round(i / 10, 1)
    performance_ratio = run_simulation(workload, architecture)
    SR_Y.append(performance_ratio['SR'])
    SC_Y.append(performance_ratio['SC'] / 1000)
    RU_Y.append(performance_ratio['RU'])

print(f'Z-Score Thresholds: {X}')
print(f'SR Values: {SR_Y}')
print(f'SC Values: {SC_Y}')
print(f'RU Values: {RU_Y}')

count = 0
utils.plot(X, SR_Y, 'Z-Score Threshold', 'Success Ratio (SR)', 'SR vs Z-Score Threshold', count)
count += 1
utils.plot(X, SC_Y, 'Z-Score Threshold', 'System Cost (SC) (in s)', 'SC vs Z-Score Threshold', count)
count += 1
utils.plot(X, RU_Y, 'Z-Score Threshold', 'Resource Utilization (in %)', 'RU vs Z-Score Threshold', count)
count += 1


print('For scheduling only on FDC...')

SR_Y = []
SC_Y = []
RU_Y = []
architecture = setup_fdc_architecture(C, F, EU)
architecture['SF'] = 0.5
architecture['z_score_threshold'] = 0.5
performance_ratio = run_fdc_simulation(workload, architecture)
for i in range(11):
    SR_Y.append(performance_ratio['SR'])
    SC_Y.append(performance_ratio['SC'] / 1000)
    RU_Y.append(performance_ratio['RU'])

print(f'Z-Score Thresholds: {X}')
print(f'SR Values: {SR_Y}')
print(f'SC Values: {SC_Y}')
print(f'RU Values: {RU_Y}')


print(time.sleep(20))

utils.plot(X, SR_Y, 'Z-Score Threshold', 'Success Ratio (SR)', 'SR vs Z-Score Threshold', count)
count += 1
utils.plot(X, SC_Y, 'Z-Score Threshold', 'System Cost (SC) (in s)', 'SC vs Z-Score Threshold', count)
count += 1
utils.plot(X, RU_Y, 'Z-Score Threshold', 'Resource Utilization (in %)', 'RU vs Z-Score Threshold', count)
count += 1

print('For scheduling only on CDC...')

SR_Y = []
SC_Y = []
RU_Y = []
architecture = setup_cdc_architecture(C, F, EU)
architecture['SF'] = 0.5
architecture['z_score_threshold'] = 0.5
performance_ratio = run_cdc_simulation(workload, architecture)
for i in range(11):
    SR_Y.append(performance_ratio['SR'])
    SC_Y.append(performance_ratio['SC'] / 1000)
    RU_Y.append(performance_ratio['RU'])

print(f'Z-Score Thresholds: {X}')
print(f'SR Values: {SR_Y}')
print(f'SC Values: {SC_Y}')
print(f'RU Values: {RU_Y}')

utils.plot(X, SR_Y, 'Z-Score Threshold', 'Success Ratio (SR)', 'SR vs Z-Score Threshold', count)
count += 1
utils.plot(X, SC_Y, 'Z-Score Threshold', 'System Cost (SC) (in s)', 'SC vs Z-Score Threshold', count)
count += 1
utils.plot(X, RU_Y, 'Z-Score Threshold', 'Resource Utilization (in %)', 'RU vs Z-Score Threshold', count)

# architecture['SF'] = 0.5
# run_simulation(workload, architecture)

# trigger_simulations(workload, SF_thresholds=SF_thresholds)

# display_resource_logs(architecture['resource_logs'])
# display_cdc_data(EU)
# display_cdc_data(F)
# display_cdc_data(C)
# display_native_and_public_fdc(EU)
# display_native_and_public_cdc(F)

# run_simulation(C, F, EU, workload, SF=0.5)

# end_test(C, F)