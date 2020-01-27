#!/usr/bin/env python3
# coding=utf-8
import random
from collections import deque
from typing import Dict, Generator, Deque, List

import numpy as np

from .model.core import Flow, Packet
from .model.sfi import SFI
from .simulator import Sim


class WorkloadGenerator(object):
    
    def __init__(self, sim: Sim, workload_rand: random.Random, config: Dict):
        self.sim = sim
        self.workload_random = workload_rand
        self.config = config
    
    def get_traffic_classes(self) -> List[List[int]]:
        return self.config['tClasses']
    
    def get_expected_workload_time(self) -> int:
        return self.config['workload_start_new_flows_till']
    
    def prepare_before_simulation_starts(self):
        raise NotImplementedError()
    
    def get_workload_statistics(self):
        raise NotImplementedError()
    
    def __next__(self):
        raise NotImplementedError()


class SyntheticWorkloadGenerator(WorkloadGenerator):
    
    @staticmethod
    def get_default_config():
        config = {
            'workload_start_new_flows_till': 10 * 1000000,
            
            # parameters for two level markov model
            'workload_probability_stay_in_l': 0.8,
            'workload_probability_stay_in_h': 0.4,
            # the workload generator takes this value and multiplies it with _l and _h to get the real probabilities
            'workload_probability_factor': 0.8,
            'workload_flow_arrival_l': 120,
            'workload_flow_arrival_h': 15,
            
            # lambda is used for: workload_flow_arrival_l * lambda and workload_flow_arrival_h * lambda
            'workload_lambda': 60,
            
            'workload_packet_inter_arrival_expected_time': 800,
            'workload_packets_per_flow': 150,
            
            'number_of_sf_types': 2,
            
            # processing rate is per 1s, when using one cpu share, so we normalze the rate to servers with a capacity of 80
            'sf_processing_rate': [
                1000000 // 160 // 80,
                1000000 // 250 // 80,
            ],
            
            # configure sfc traffic classes
            'tClasses': [[0, 1],
                         [0],
                         [1]],
            
            # we take the deadline of each sfc times "workload_deadline_scaling"
            'workload_deadline_scaling': 10
        }
        
        # we set the deadline of each sfc according to the delay each of its sf requires to process.
        config['workload_deadline_per_packet'] = []
        for tc in config['tClasses']:
            delay = 0
            for sf in tc:
                delay += 1000000 / config['sf_processing_rate'][sf]
            config['workload_deadline_per_packet'].append(int(delay))
        
        return config
    
    def __init__(self, sim: 'Sim', workload_rand: random.Random, config: Dict):
        super().__init__(sim, workload_rand, config)
        self.packet_generator = None
        self.flows = None
    
    def __next__(self):  # -> "IngressEvent"
        return self.packet_generator.__next__()
    
    def prepare_before_simulation_starts(self):
        self.packet_generator = self.create_packet_generator()
        
        if next(self.packet_generator) is not None:
            raise NameError("the very first call must be None")
    
    def get_workload_statistics(self):
        # what is the packets to be processed for each sf type
        flow_pros: Flow.Props = self.sim.props.flow
        flow_size = self.config['workload_packets_per_flow']
        # number of packets, p/s, req. processing time per second at with 1 server capacity (so this is also the req. capacity)
        per_sf_demand = {sf: [0, 0, 0] for sf in range(len(self.sim.props.sfi.processingRateOfSfType))}
        for flow in self.flows:
            index = 0
            eoc = None
            while eoc is None or not eoc:
                sf, eoc = flow_pros.sfc_class_to_sf[flow.sfc_class + index]
                per_sf_demand[sf][0] += flow_size
                index += 1
        
        exp_time = self.get_expected_workload_time() / 1000000  # in seconds
        sfi_props: SFI.Props = self.sim.props.sfi
        
        for sf in per_sf_demand:
            # per second, how many packets to serve
            per_sf_demand[sf][1] = per_sf_demand[sf][0] / exp_time
            # how much processing time does it take with one server capacity
            per_sf_demand[sf][2] = per_sf_demand[sf][1] / sfi_props.processingRateOfSfType[sf]
        
        return per_sf_demand
    
    def create_packet_generator(self) -> Generator:
        traffic_class = self.get_traffic_classes()
        workload_flow_arrival_l = int(self.config['workload_lambda'] * self.config['workload_flow_arrival_l'])
        workload_flow_arrival_h = int(self.config['workload_lambda'] * self.config['workload_flow_arrival_h'])
        workload_packet_inter_arrival_expected_time = self.config['workload_packet_inter_arrival_expected_time']
        workload_packets_per_flow = self.config['workload_packets_per_flow']
        workload_probability_stay_in_l = self.config['workload_probability_stay_in_l'] * self.config[
            'workload_probability_factor']
        workload_probability_stay_in_h = self.config['workload_probability_stay_in_h'] * self.config[
            'workload_probability_factor']
        workload_start_new_flows_till = self.config['workload_start_new_flows_till']
        
        workload_deadline_per_packet = [int(self.config['workload_deadline_scaling'] * d)
                                        for d
                                        in self.config['workload_deadline_per_packet']]
        
        assert len(workload_deadline_per_packet) == len(traffic_class)
        
        self.config['workload_effective_deadline_per_packet'] = workload_deadline_per_packet
        print(f"Deadlines randomized: {workload_deadline_per_packet}")
        
        burstiness = 1.0 / workload_probability_stay_in_h + 1.0 / workload_probability_stay_in_l
        print(f"Workload has a burstiness of {burstiness}")
        
        all_sff = list(self.sim.props.sff.allSFFs.values())
        flow_arrival_state_high = False
        self.flows: Deque[Flow] = deque()
        number_of_all_flows_hops = 0
        
        ingress_nodes = all_sff[:]
        
        for ingress in ingress_nodes:
            flow_start_time = 0
            
            while flow_start_time < workload_start_new_flows_till:
                # switch state of flow arrival times?
                if flow_arrival_state_high:
                    # transition to low state?
                    if np.random.rand() > workload_probability_stay_in_h:
                        flow_arrival_state_high = False
                else:
                    if np.random.rand() > workload_probability_stay_in_l:
                        flow_arrival_state_high = True
                
                next_inter_flow_time = np.random.poisson(workload_flow_arrival_h if flow_arrival_state_high
                                                         else workload_flow_arrival_l)
                flow_start_time += next_inter_flow_time
                
                egress = np.random.choice(all_sff)
                
                random_traffic_class_index = np.random.choice(range(len(traffic_class)))
                random_traffic_class = traffic_class[random_traffic_class_index]
                
                self.flows.append(Flow(sim=self.sim, sf_type_chain=random_traffic_class,
                                       qos_max_delay=workload_deadline_per_packet[random_traffic_class_index],
                                       desired_egress_ssf_id=egress.id,
                                       ingress_sff_id=ingress.id,
                                       start_time=flow_start_time))
                number_of_all_flows_hops += len(random_traffic_class)
        
        print(f"Created {len(self.flows)} flows.. start sorting now")
        # sort flows based on start_time
        self.flows = deque(sorted(self.flows, key=lambda f: f.start_time))
        
        flow_sizes = np.random.poisson(workload_packets_per_flow, len(self.flows)).tolist()
        print(f"All flows have in total {number_of_all_flows_hops} "
              f"hops, i.e., a scheduler has to take at least that many decisions")
        
        yield None
        
        while len(self.flows) > 0:
            f = self.flows.popleft()
            single_flow_size = flow_sizes.pop()
            packet_start_times = list(
                np.random.poisson(workload_packet_inter_arrival_expected_time, single_flow_size))
            
            for packet in range(single_flow_size):
                yield Packet.create_wrap_in_event(time_ingress=int(f.start_time + packet_start_times.pop()),
                                                  flow=f,
                                                  transmission_size=1)
