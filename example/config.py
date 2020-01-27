#!/usr/bin/env python3
# coding=utf-8
import random

import sfctss


def template_default_parameters(sites):
    from_config = dict()
    
    from_config['seed'] = 0
    
    rand = random.Random()
    rand.seed(from_config['seed'])
    
    from_config['latency_between_sites'] = 3 * 1000  # in µs
    from_config['latency_within_sites'] = 700
    
    # WARNING if you want to change the number if sites, you have to modify the script. This value is only intended
    # for evaluation purpose
    from_config['sites'] = sites
    
    from_config['server_capacity'] = 100
    from_config['number_of_total_sfis'] = 30
    
    from_config['number_of_servers_per_site'] = [rand.randint(6, 8) for _ in range(from_config['sites'])]
    from_config['number_of_sff_per_site'] = [1] * from_config['sites']
    
    from_config['scheduler'] = 'greedy'
    from_config['scheduler_incremental'] = True
    from_config['scheduler_oracle'] = False
    
    from_config['cpu_policy'] = 'one-at-a-time'
    # from_config['cpu_policy'] = 'dynamic'
    
    from_config['individual_class_per_egress'] = False
    
    from_config['admission_threshold_low'] = 0.1
    from_config['admission_threshold_high'] = 1
    
    from_config['statistics_polling'] = 50000
    
    wl_config = sfctss.workload.SyntheticWorkloadGenerator.get_default_config()
    from_config = {**from_config, **wl_config}
    
    return from_config
