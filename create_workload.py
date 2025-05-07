import random
import numpy as np
import pandas as pd
import time

def set_up_cdcs(count):
    '''
    To set-up cloud data centres in the network architecture
    '''
    cdcs = []
    reach = 100 * count
    for i in range(1, count+1):
        cdc = {}
        cdc['id'] = f'cdc_{i}'
        cdc['x_coordinate'] = random.choice(range(-reach, reach + 1))
        cdc['y_coordinate'] = 100000
        cdc['total_capacity'] = (2 ** 10) * (2 ** 10) * (2 ** 10) * (2 ** random.choice(range(2)))
        cdc['total_Mips'] = 10000 * random.choice(range(1, 6))
        cdc['BW'] = (2 ** 10) * (2 ** 10) * (2 ** random.choice(range(6, 11)))
        cdc['used_capacity'] = 0
        cdc['busy_until'] = 0
        cdc['executing_job_id'] = None
        cdcs.append(cdc)
    return cdcs


def set_up_fdcs(count):
    '''
    To set-up fog data centres in the network architecture
    '''
    fdcs = []
    reach = 100 * count
    for i in range(1, count+1):
        fdc = {}
        fdc['id'] = f'fdc_{i}'
        fdc['x_coordinate'] = random.choice(range(-reach, reach + 1))
        fdc['y_coordinate'] = 1000
        fdc['total_capacity'] = (2 ** 10) * (2 ** 10) * (2 ** random.choice(range(7, 10)))
        fdc['total_Mips'] = 1000 * random.choice(range(1, 6))
        fdc['BW'] = (2 ** 10) * (2 ** 10) * (2 ** random.choice(range(1, 6)))
        fdc['used_capacity'] = 0
        fdc['busy_until'] = 0
        fdc['executing_job_id'] = None
        fdcs.append(fdc)
    return fdcs


def set_up_eus(count):
    '''
    To set-up end-users in the network architecture
    '''
    eus = []
    reach = 100 * count
    for i in range(1, count+1):
        eu = {}
        eu['id'] = f'eu_{i}'
        eu['x_coordinate'] = random.choice(range(-reach, reach + 1))
        eu['y_coordinate'] = 0
        eus.append(eu)
    return eus


def set_up_workload(EU, N):
    '''
    To configure jobs and return the workload
    '''
   
    workload = []
    for i in range(1, N+1):
        job = {}
        job['id'] = f'job_{i}'
        job['category'] = random.choice(['tc', 'tr', 'tp'])
        job['instructions'] = random.choice(range(1000, 10000))
        job['arrival_time'] = 0
        job['deadline'] = job['arrival_time'] + 1000 * random.choice(range(1, 121))
        job['eu'] = random.choice(EU)['id']
        workload.append(job)
    return workload
    
# No. of cdcs, no. of fdcs, no. of end-users, and no. of jobs
# g, h, e, N = 10, 25, 50, 1000
# g, h, e, N = 15, 50, 100, 300
# g, h, e, N = 10, 25, 100, 500
g, h, e, N = 50, 150, 300, 500

# Set-up cdcs, fdcs, end-users, and IoT jobs
C, F, EU = set_up_cdcs(g), set_up_fdcs(h), set_up_eus(e)
J = set_up_workload(EU, N)

print(C)
print(F)
print(EU)
for job in J:
    print(job)

# Export the CDCs
cdc_df = pd.DataFrame.from_dict(C)
cdc_df.to_csv('files/CDCs.csv', index=False)

# Export the FDCs
fdc_df = pd.DataFrame.from_dict(F)
fdc_df.to_csv('files/FDCs.csv', index=False)

# Export the end-users
eu_df = pd.DataFrame.from_dict(EU)
eu_df.to_csv('files/EUs.csv', index=False)

# Export workload
workload_df = pd.DataFrame.from_dict(J)
workload_df.to_csv('files/workload.csv', index=False)




