import math
import matplotlib.pyplot as plt


def fetch_native_fog_for_job(EU, job):
    end_user_id = job['eu']
    return EU[end_user_id]['native_fdc_id']


def fetch_public_fog_for_job(EU, job):
    end_user_id = job['eu']
    return EU[end_user_id]['public_fdc_id']


def fetch_native_cloud_for_job(F, EU, job):
    fdc_id = fetch_native_fog_for_job(EU, job)
    return F[fdc_id]['native_cdc_id']


def fetch_public_cloud_for_job(F, EU, job):
    fdc_id = fetch_native_fog_for_job(EU, job)
    return F[fdc_id]['public_cdc_id']


def calculate_distance(node_1, node_2):
    '''
    To find the Euclidean distance (in km) between two geographically separated nodes
    '''
    return math.ceil(((node_1['x_coordinate'] - node_2['x_coordinate'])**2 + (node_1['y_coordinate'] - node_2['y_coordinate'])**2)**0.5)


def evaluate_neighborhood(source, targets):
    '''
    To sort the targets based on the distance from the source
    '''
    neighborhood = []
    for id, target in targets.items():
        distance = calculate_distance(source, target)
        neighborhood.append((source['id'], target['id'], distance))
    
    neighborhood = sorted(neighborhood, key=lambda x: x[2])
    return neighborhood


def map_fog_to_cloud(F, C):
    '''
    To map each fdc with its native and public cdc
    '''
    # Evaluate the neighborhood of each fdc
    for id, fdc in F.items():
        neighborhood = evaluate_neighborhood(fdc, C)
        F[id]['native_cdc_id'] = neighborhood[0][1]
        F[id]['public_cdc_id'] = neighborhood[1][1]
            
    return F


def map_end_user_to_fog(EU, F):
    '''
    To map each end-user with its native and public fdc
    '''
    # Evaluate the neighborhood of each end-user
    for id, eu in EU.items():
        neighborhood = evaluate_neighborhood(eu, F)
        EU[id]['native_fdc_id'] = neighborhood[0][1]
        EU[id]['public_fdc_id'] = neighborhood[1][1]
            
    return EU


def calculate_resource_utilization():
    '''
    To calculate the ratio of current usage with total capacity of a resource
    '''
    return


def calculate_scope_factor():
    '''
    To calculate the ratio of current usage with total capacity of a resource
    '''
    return 0.5


def calculate_z_score_quantities(jobs):
    d_min = 10**9
    d_max = -1

    for job in jobs:
        if job['deadline'] < d_min: d_min = job['deadline']
        if job['deadline'] > d_max: d_max = job['deadline']

    return d_min, d_max


# def calculate_z_score(job, d_min, d_max):
#     '''
#     To analyse the size of the selected job amongst all the available jobs
#     '''
#     return (job['deadline'] - quantities['d_min']) / (quantities['d_max']- quantities['d_min'])


def fetch_communication_delay(job, resource, node_1, node_2):
    transmission_delay = calculate_transmission_delay(job, resource)
    propagation_delay = calculate_propagation_delay(node_1, node_2)
    latency_delay = transmission_delay + propagation_delay
    # print(f"{job['id']} @{resource['id']} | {transmission_delay} + {propagation_delay} = {latency_delay}")
    return latency_delay


def calculate_runtime(resource, job):
    '''
    Calculate the job runtime (in ms) on the given resource
    '''
    job_instructions = job['instructions']
    resource_Mips = resource['total_Mips']
    runtime = math.ceil(10**3 * job_instructions / resource_Mips)
    # print(f"{job['id']} @{resource['id']} | Runtime: {runtime}")
    return runtime


def calculate_transmission_delay(job, resource):
    '''
    To calculate the time taken (in ms) to transmit the data from user to transmission medium
    '''
    return math.ceil(1000 * 64 * job['instructions'] / resource['BW'])


def calculate_propagation_delay(node_1, node_2):
    '''
    To calculate the time taken for propagation of data through the transmission medium
    '''
    
    distance = 1000*calculate_distance(node_1, node_2) # distance in  km
    velocity = 2 * 10**8
    print("dist", math.ceil(1000 * distance / velocity))
    return math.ceil(1000 * distance / velocity) # converting to ms


def calculate_execution_time():
    return


#def is_job_executed_before_deadline(job, link):
 #   return True if job['arrival_time'] + calculate_execution_time() + calculate_communication_delay(job, link) <= job['deadline'] else False


def calculate_success_ratio(all_jobs, executed_jobs):
    total, N_dash = 0, 0

    for job_id, job_info in executed_jobs.items():
        total += 1
        if job_info['end_time'] <= all_jobs[job_id]['deadline']:
            N_dash += 1

    print(f'{N_dash} / {total}')
    return N_dash / total


def calculate_set_up_cost(C, F):
    '''
    To calculate the initial set-up cost
    '''
    return 0


def calculate_execution_cost(resource_logs):
    '''
    To calculate the aggregated execution cost of all jobs
    '''
    t_exec = 0

    # For each resource
    for resource_id, jobs in resource_logs.items():

        # For each executed job
        for job in jobs:
            t_exec += job['end_time'] - job['start_time']
    
    return t_exec


def calculate_system_cost(architecture):
    '''
    To measure the performance of an architecture by software and hardware interactions within the network
    '''
    C = architecture['C']
    F = architecture['F']
    resource_logs = architecture['resource_logs']
    t_exec = calculate_execution_cost(resource_logs)
    print('t_exec:', t_exec)
    return calculate_set_up_cost(C, F) + t_exec


def calculate_total_capacity(counter, F, C):
    '''
    To measure the total execution capacity of available resources
    '''
    return


def calculate_resource_utilization(architecture):
    '''
    To calculate the ratio of total execution time with the total capacity available in our network
    '''
    end_time = architecture['end_at']
    resource_utilization = architecture['resource_utilization']

    ru_list = []
    for resource_id in resource_utilization.keys():
        ru_list.append(resource_utilization[resource_id] / end_time)

    return round(sum(ru_list) / len(ru_list), 2)


def plot(X, Y, xlabel, ylabel, title, count):
    '''
    To visualize network performance for separate thresholds values
    '''
    plt.clf()
    plt.plot(X, Y)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    # plt.show()
    plt.savefig(f'figure{count}.jpg')
    return

