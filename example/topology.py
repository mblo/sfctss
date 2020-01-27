#!/usr/bin/env python3
# coding=utf-8
import json
import random
from random import Random
from typing import Dict

import numpy as np

import sfctss
from sfctss.scheduler.examples import GreedyShortestDeadlineFirstScheduler, LoadUnawareRoundRobinScheduler


def run(config: Dict,
        show_progress: bool = False,
        show_ui: bool = False,
        show_ui_full: bool = False,
        statistics_filename: str = None,
        statistics_overview: bool = True,
        statistics_packets: bool = False,
        statistics_server: bool = False,
        statistics_polling: int = None,
        statistics_polling_sfi: bool = False,
        statistics_polling_sff: bool = False,
        statistics_polling_server: bool = False,
        statistics_polling_overview: bool = False,
        statistics_workload: bool = False,
        statistics_packet_cdfs: int = None,
        debug: bool = False,
        stop_simulation_after: int = -1,
        stop_simulation_when_workload_is_over: bool = False,
        run_interactive: bool = False,
        no_workload_reloading: bool = False,
        dry_run: bool = False):
    seed = config['seed']
    sim = sfctss.simulator.Sim(seed=seed)
    sim.DEBUG = debug
    sim.PACKET_ID_TO_DEBUG = None
    
    sim.props.sim_stats.FLUSH_ENTRIES = 500
    
    if stop_simulation_after > 0:
        config["workload_start_new_flows_till"] = stop_simulation_after
    
    print(f"run with configuration: {json.dumps(config, indent=1)}")
    sites = config['sites']
    
    cpu_policy = None
    if config['cpu_policy'] == 'one-at-a-time':
        cpu_policy = sfctss.model.ServerCpuPolicy.one_at_a_time
    elif config['cpu_policy'] == 'dynamic':
        cpu_policy = sfctss.model.ServerCpuPolicy.dynamic
    else:
        raise NameError(f"unknown cpu policy: {config['cpu_policy']}")
    
    rand: Random = random.Random()
    rand.seed(seed)
    np.random.seed(rand.randint(0, 1000000))
    
    sim.props.flow.individual_class_per_egress = config['individual_class_per_egress']
    sf_processing_rate = config['sf_processing_rate']
    
    number_of_servers_per_site = config['number_of_servers_per_site']
    number_of_sff_per_site = config['number_of_sff_per_site']
    server_capacity = config['server_capacity']
    number_of_sf_types = config['number_of_sf_types']
    number_of_total_sfis = config['number_of_total_sfis']
    
    if config['scheduler'] == 'greedy':
        scheduler_instance = lambda: GreedyShortestDeadlineFirstScheduler(sim=sim,
                                                                          incremental=config['scheduler_incremental'],
                                                                          oracle=config['scheduler_oracle'],
                                                                          admission_control_threshold_low=
                                                                          config['admission_threshold_low'],
                                                                          admission_control_threshold_high=
                                                                          config['admission_threshold_high'])
    elif config['scheduler'] == 'static':
        scheduler_instance = lambda: LoadUnawareRoundRobinScheduler(sim=sim,
                                                                    incremental=config['scheduler_incremental'],
                                                                    oracle=config['scheduler_oracle'])
    elif config['scheduler'] == 'reject':
        scheduler_instance = lambda: sfctss.scheduler.RejectScheduler(sim=sim,
                                                                      incremental=config['scheduler_incremental'],
                                                                      oracle=config['scheduler_oracle'])
    else:
        raise NameError(f'unsupported scheduler {config["scheduler"]}')
    
    assert len(number_of_servers_per_site) == len(number_of_sff_per_site) == sites
    assert len(sf_processing_rate) == number_of_sf_types
    
    sff_of_site = {s: [sfctss.model.SFF(sim=sim, scheduler=scheduler_instance())
                       for _ in range(number_of_sff_per_site[s])]
                   for s in range(sites)}
    
    # activate bandwidth limits on links
    sfctss.model.SFF.set_consider_bw_capacity(sim, True)
    
    # setup latency distributions for links
    expected_latency_within_sites = config['latency_within_sites']
    expected_latency_between_sites = config['latency_between_sites']
    
    intra_site_latency_id = 0
    inter_site_latency_id = 1
    
    # create link latency distributions
    sfctss.model.SFF.setup_latency_distribution(sim, intra_site_latency_id,
                                                np.random.poisson(expected_latency_within_sites, 5000)
                                                if expected_latency_within_sites > 0 else [0])
    
    # create link latency distributions
    sfctss.model.SFF.setup_latency_distribution(sim, inter_site_latency_id,
                                                np.random.poisson(expected_latency_between_sites, 5000)
                                                if expected_latency_between_sites > 0 else [0])
    
    # setup sff connection inside a site
    for site in sff_of_site:
        for i, sff in enumerate(sff_of_site[site]):
            for sff_other in sff_of_site[site][i + 1:]:
                sfctss.model.SFF.setup_connection(sim, source_id=sff.id, destination_id=sff_other.id,
                                                  bw_cap=100000, latency_provider=intra_site_latency_id, bidirectional=True)
    
    # fully connected mesh of sites
    for site in sff_of_site:
        for sff in sff_of_site[site]:
            for other_site in sff_of_site:
                if site != other_site:
                    for other_sff in sff_of_site[other_site]:
                        sfctss.model.SFF.setup_connection(sim, source_id=sff.id, destination_id=other_sff.id,
                                                          bw_cap=100000, latency_provider=inter_site_latency_id,
                                                          bidirectional=True)
    
    if len(sff_of_site) == 1:
        sfctss.model.SFF.init_data_structure(sim)
    
    sfctss.model.SFI.init_data_structure(sim, number_of_sf_types=number_of_sf_types, latency_provider_sff_sfi=intra_site_latency_id)
    
    # define the processing rate of
    for i in range(number_of_sf_types):
        sfctss.model.SFI.setup_sf_processing_rate_per_1s(
            sim, of_type=i, with_mu=sf_processing_rate[i])
    
    # set seed again for sfi and servers
    rand.seed(seed)
    
    # we randomize server capacity slightly based on the given value
    def get_randomized_server_capacity():
        return round(server_capacity * (rand.randint(80, 120) / 100))
    
    # create some servers
    server_of_site = {
        s: [(lambda: sfctss.model.Server(sim, processing_cap=get_randomized_server_capacity(), cpu_policy=cpu_policy))()
            for _ in
            range(number_of_servers_per_site[s])] for s in range(sites)}
    
    sfis_to_allocate = number_of_total_sfis
    
    # spread SFIs among all SFF and servers
    # apply a very simple random
    
    # sff_list = list(sim.props.sff.allSFFs.values())
    sf_types = list(range(number_of_sf_types))
    shuffled_sites = list(range(sites))
    while sfis_to_allocate > 0:
        failed_allocation = True
        rand.shuffle(shuffled_sites)
        # for each site
        for site in shuffled_sites:
            # add for each server a function
            server: sfctss.model.Server = None
            for server in server_of_site[site]:
                # choose any of the SFs which is not yet installed on this server
                rand.shuffle(sf_types)
                
                if sfis_to_allocate == 0:
                    break
                
                for sf in sf_types:
                    other_sfi: sfctss.model.SFI = None
                    sf_not_yet_on_server = True
                    for other_sfi in server.SFIs:
                        if other_sfi.of_type == sf:
                            sf_not_yet_on_server = False
                            break  # stop checking other sfis on this server
                    if sf_not_yet_on_server:
                        # take the existing SFF, if there is any, or any of the sffs of this site
                        connect_sff = rand.choice(sorted(server.SFF_ids)) if len(server.SFF_ids) > 0 else sff_of_site[site][0].id
                        server.add_sfi(of_type=sf, with_sff_id=connect_sff)
                        sfis_to_allocate -= 1
                        failed_allocation = False
                        break  # stop checking other sfs for this server
        if failed_allocation:
            raise NameError(f"this workload seems to be unbalanced - "
                            f"cannot find available server for remaining {sfis_to_allocate} SFIs")
    
    print("SFI spread done")
    
    sfctss.sanity.print_infrastructure(sim)
    
    wl_gen = sfctss.workload.SyntheticWorkloadGenerator(sim=sim,
                                                        workload_rand=rand,
                                                        config=config)
    wl_gen.prepare_before_simulation_starts()
    
    per_sf_demand = wl_gen.get_workload_statistics()
    
    print("workload has demand of (per site)")
    for sf in per_sf_demand:
        print(f"\tsf {sf} -> pack. to be process.:{round(per_sf_demand[sf][0] / sites, 1)} total, "
              f"{round(per_sf_demand[sf][1] / sites, 1)} /s, "
              f"{round(per_sf_demand[sf][2] / sites, 1)} cap. required")
    
    print(f"Register packet generator")
    sim.register_packet_generator(packet_generator=wl_gen,
                                  fetch_all=no_workload_reloading)
    
    if statistics_filename is not None:
        store_config = {k: str(v) for k, v in config.items()}
        
        sfctss.measurement.SimStats.activate(sim, statistics_filename, configuration=store_config)
        
        if statistics_packets:
            sfctss.measurement.SimStats.activate_packet_statistics(sim)
            sfctss.measurement.SimStats.activate_flow_statistics(sim)
        if statistics_server:
            sfctss.measurement.SimStats.activate_server_statistics(sim)
        if statistics_overview:
            sfctss.measurement.SimStats.activate_overview_statistics(sim, optional_time_snapshots=[c * 1000000 for c in range(100)])
        if statistics_workload:
            sfctss.measurement.SimStats.activate_workload_statistics(sim)
        
        if statistics_packet_cdfs is not None:
            sfctss.measurement.SimStats.activate_packet_cdf_statistics(sim, cdf_buckets=statistics_packet_cdfs, per_group=False)
        
        if statistics_polling is not None:
            sfctss.measurement.SimStats.activate_polling_statistics(sim, interval=int(statistics_polling))
            if statistics_polling_server:
                sfctss.measurement.SimStats.activate_polling_server_statistics(sim)
            if statistics_polling_sff:
                sfctss.measurement.SimStats.activate_polling_sff_statistics(sim)
            if statistics_polling_sfi:
                sfctss.measurement.SimStats.activate_polling_sfi_statistics(sim)
            if statistics_polling_overview:
                sfctss.measurement.SimStats.activate_polling_overview_statistics(sim)
    
    if dry_run:
        print("Dry run, simply test configuration, do not run simulation. Exit")
        return
    
    sim.ui_shows_full_state = show_ui_full
    sim.run_sim(show_progress=show_progress,
                interactive=run_interactive,
                max_sim_time=stop_simulation_after,
                ui=show_ui,
                stop_simulation_when_workload_is_over=stop_simulation_when_workload_is_over)
    
    sfctss.sanity.print_simple_stats(sim)
    print("*" * 30)
