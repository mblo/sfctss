#!/usr/bin/env python3
# coding=utf-8

import time
from itertools import accumulate
from typing import Dict, List

from ..events import BaseEvent
from ..simulator import Sim, SchedulingFailure

from ..model.server import Server, ServerCpuPolicy
from ..model.sff import SFF
from ..model.sfi import SFI
from ..model.core import Flow, Packet

from ..rate_estimator import RateEstimator, EWMA


class ACP(object):
    
    def __init__(self, scheduler: 'BaseScheduler'):
        self.scheduler = scheduler
        self.dist_information = None  # sff_id -> ∀ sf unrolled the ratio of load / service
        #                                         time of information
        
        self.detour = 0
        self.non_detour = 0
    
    def check_packet_to_process_not_locally_and_update_path(self, packet: Packet):
        if self.scheduler.static_sff_rates_per_sf_sorted_sff is None:
            self.scheduler.update_cum_weights_of_sff_rates()
        
        forward_to_neighbor = False
        
        # forward because we cannot serve?
        next_sf = packet.toBeVisited[0]
        if next_sf not in self.scheduler.mySFF.SFIsPerType or len(self.scheduler.mySFF.SFIsPerType[next_sf]) == 0:
            forward_to_neighbor = True
        
        # if we are not forced to forward to a neighbor, check the load
        if not forward_to_neighbor and len(self.scheduler.static_sff_rates_per_sf_sorted_sff[next_sf]) > 0:
            # get the load of the affected queue
            arrival_rate = self.scheduler.get_arrival_rate_estimate(next_sf)
            service_rate = self.scheduler.mySFF.service_rate_per_sf[next_sf]
            assert service_rate > 0
            
            threshold_low = self.scheduler.admission_control_threshold_low
            threshold_high = self.scheduler.admission_control_threshold_high
            load = arrival_rate / service_rate
            
            # if self.scheduler.mySFF.id == 0:
            #     print(arrival_rate, load)
            
            if threshold_high > load > threshold_low:
                # load is still ok, but we forward with a certain ratio
                to_test = (load - threshold_low) / (threshold_high - threshold_low)
                if self.scheduler.sim.random.random() <= to_test:
                    forward_to_neighbor = True
            
            elif load >= threshold_high:
                # load is above higher threshold, forward packet
                forward_to_neighbor = True
        
        if forward_to_neighbor:
            # check if we are the only guys who have a sfi of certain type, if so, ignore local load and process it locally
            if (next_sf not in self.scheduler.static_sff_rates_per_sf_sorted_sff or
                    next_sf not in self.scheduler.static_sff_rates_per_sf_cum_weights or
                    len(self.scheduler.static_sff_rates_per_sf_sorted_sff[next_sf]) == 0):
                # there is no one with a SFI, so do not search for a path
                pass
            elif (next_sf in self.scheduler.static_sff_rates_per_sf_sorted_sff and
                  next_sf in self.scheduler.static_sff_rates_per_sf_cum_weights and
                  len(self.scheduler.static_sff_rates_per_sf_sorted_sff[next_sf]) == 1 and
                  self.scheduler.static_sff_rates_per_sf_sorted_sff[next_sf] == self.scheduler.mySFF.id):
                # we are the only ones with such a SFI, so process locally (ignore load)
                forward_to_neighbor = False
            else:
                self.recommend_forwarding_neighbor_and_update_path(packet=packet)
        
        return forward_to_neighbor
    
    def recommend_forwarding_neighbor_and_update_path(self, packet: Packet):
        next_sf = packet.toBeVisited[0]
        # select a neighbor
        assert next_sf in self.scheduler.static_sff_rates_per_sf_cum_weights
        assert 0 < len(self.scheduler.static_sff_rates_per_sf_cum_weights[next_sf])
        target_sff_id = self.scheduler.sim.random.choices(
            self.scheduler.static_sff_rates_per_sf_sorted_sff[next_sf],
            cum_weights=self.scheduler.static_sff_rates_per_sf_cum_weights[next_sf],
            k=1)[0]
        
        path_to_other_sff = SFF.get_multi_hop_path_for(self.scheduler.sim, self.scheduler.mySFF.id, target_sff_id)
        for next_sff in path_to_other_sff:
            packet.fullPath.append((SFF.__name__, self.scheduler.sim.props.sff.allSFFs[next_sff]))


class BaseScheduler(object):
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            self.drop_packet_after_scheduling_attempts = 50
    
    def __repr__(self):
        return f'{self.__class__.__name__}{vars(self)}'
    
    def __init__(self, sim: Sim,
                 incremental: bool = True,
                 oracle=True,
                 activate_acp: bool = False,
                 admission_control_threshold_low: float = 0.8,
                 admission_control_threshold_high: float = 1.1):
        self.incremental = incremental
        self.oracle = oracle
        self.mySFF: SFF = None
        self.sim: Sim = sim
        self.time_scheduling_starts = None
        self.acp = ACP(self) if activate_acp else None
        
        self.admission_control_threshold_low = admission_control_threshold_low
        self.admission_control_threshold_high = admission_control_threshold_high
        
        assert admission_control_threshold_low < admission_control_threshold_high
        
        # we have two main data structures with information about all the SFIs available at other SFFs
        # and at all SFFs (other+my sff)
        # __sff__rates holds for each sf type, the aggregates sfi rate of all SFFs
        self.static_sff_rates_per_sf_cum_weights: Dict[int, List[float]] = None
        self.static_sff_rates_per_sf_sorted_sff: Dict[int, List[int]] = None
        
        # __sfi__rates holds for each sf type, the sfi rates
        self.static_sfi_rates_per_sf_cum_weights: Dict[int, List[float]] = None
        self.static_sfi_rates_per_sf_sorted_sfi: Dict[int, List[int]] = None
        
        self.scheduling_attempts = 0
        
        self.rate_estimator: Dict[int, RateEstimator] = {}
    
    def inform_rate_estimator_about_packet_arrival(self, packet: Packet):
        of_sf_type = packet.toBeVisited[0]
        if of_sf_type not in self.rate_estimator:
            self.rate_estimator[of_sf_type] = EWMA(self.sim)
        
        self.rate_estimator[of_sf_type].packet_arrival()
    
    def update_cum_weights_of_sff_rates(self, include_own_sff: bool = False):
        self.static_sff_rates_per_sf_cum_weights = {}
        self.static_sff_rates_per_sf_sorted_sff = {}
        # get all sff instanes, but not my sff, and sort them based on the sff_id
        # we have to sort them, so that the simulator runs deterministic (dictionaries are not sorted deterministic)
        list_of_sff = [self.sim.props.sff.allSFFs[sff_id] for sff_id in sorted(self.sim.props.sff.allSFFs.keys()) if
                       (sff_id != self.mySFF.id or include_own_sff)]
        
        # for each sf type
        for sf in range(len(self.sim.props.sfi.processingRateOfSfType)):
            # get the rate of SFF for the given sf, i.e., we take for each SFF the static! rate of its SFIs.
            # this is the upper limit, the SFF is capable of serving for this SF.
            # it is the upper limit, because we take the rate we would get, if all SFIs (of the given sf type)
            # of this SFF are working with full server power.
            rates = [sff.service_rate_per_sf[sf] for sff in list_of_sff
                     if sf in sff.service_rate_per_sf]
            # we take the cum weights of these rates and store them for later
            self.static_sff_rates_per_sf_sorted_sff[sf] = [sff.id for sff in list_of_sff
                                                           if sf in sff.service_rate_per_sf]
            self.static_sff_rates_per_sf_cum_weights[sf] = list(accumulate(rates))
            assert len(self.static_sff_rates_per_sf_sorted_sff[sf]) == len(self.static_sff_rates_per_sf_cum_weights[sf])
            
            # if len(self.static_sff_rates_per_sf_sorted_sff[sf]) == 0:
            #     raise NameError(f"there is no other SFF which hosts a SFI of type {sf}")
        # check that we have for each sf type some entries
        assert len(self.static_sff_rates_per_sf_cum_weights) == len(self.sim.props.sfi.processingRateOfSfType)
    
    def update_cum_weights_of_sfi_rates(self, include_own_sff: bool = False):
        # analogously to update_cum_weights_of_sff_rates, this method creates the data structure with respect ot SFIs
        sfi_props: SFI.Props = self.sim.props.sfi
        self.static_sfi_rates_per_sf_cum_weights = {}
        # get all sfis
        list_of_sfi = [sfi_props.all_sfi[sfi_id] for sfi_id in sorted(sfi_props.all_sfi.keys()) if
                       (sfi_props.all_sfi[sfi_id].sffId != self.mySFF.id or include_own_sff)]
        self.static_sfi_rates_per_sf_sorted_sfi = {}
        for sf in range(len(sfi_props.processingRateOfSfType)):
            rates = [sfi.get_expected_processing_rate() for sfi in list_of_sfi if sfi.of_type == sf]
            self.static_sfi_rates_per_sf_cum_weights[sf] = list(accumulate(rates))
            self.static_sfi_rates_per_sf_sorted_sfi[sf] = [sfi.id for sfi in list_of_sfi if sfi.of_type == sf]
            assert len(self.static_sfi_rates_per_sf_sorted_sfi[sf]) == len(self.static_sfi_rates_per_sf_cum_weights[sf])
        # check that we have for each sf type some entries
        assert len(self.static_sfi_rates_per_sf_cum_weights) == len(self.sim.props.sfi.processingRateOfSfType)
    
    def assign_sff(self, sff: SFF):
        self.mySFF = sff
        assert self.sim == sff.sim
    
    def requires_queues_per_class(self):
        # whether the scheduler asks for a queue per class at the sff, or a single queue
        return False
    
    def get_arrival_rate_estimate(self, of_sf_type: int):
        if of_sf_type in self.rate_estimator:
            return self.rate_estimator[of_sf_type].get_estimated_rate()
        return 0
    
    def handle_packet_arrival(self, packet: Packet):
        # notification for packet arrival
        # is there something to do for us?
        if len(packet.fullPath) > packet.pathPosition:
            print(Packet.debug_packet(packet))
            raise NameError(f'something is going wrong. a packet was given to the scheduler, '
                            f'but the packet has some steps in its path!')
        
        # first we need to check with the ACP if we should enqueue the packet or if we should process it locally
        
        if self.acp is not None:
            if self.acp.check_packet_to_process_not_locally_and_update_path(packet):
                # forward packet
                
                # get the packet from the scheduler's queue
                if self.requires_queues_per_class():
                    popped_packet = self.mySFF.packet_queue_per_class[Flow.get_packet_class_of_packet(packet)].pop()
                else:
                    popped_packet = self.mySFF.packet_queue.pop()
                
                assert popped_packet == packet
                
                # fake queueing time, so reset the packet's timer
                packet.timeQueueScheduling += packet.get_delta_of_time_mark()
                
                # if we forward, check if there are some other guys who are able to process the packet
                next_sf = packet.toBeVisited[0]
                if (next_sf not in self.static_sff_rates_per_sf_sorted_sff or
                        len(self.static_sff_rates_per_sf_sorted_sff[next_sf]) == 0):
                    # there is no other sff, so simply reject the packet
                    packet.reject()
                    raise SchedulingFailure(f'no sfi found for sf type {next_sf}')
                
                if packet.seenByScheduler > self.sim.props.base_scheduler.drop_packet_after_scheduling_attempts:
                    packet.drop_timed_out()
                    return
                
                # inform my sff to forward the packet
                self.mySFF.handle_packet_from_scheduler(packet)
                return
        
        self.inform_rate_estimator_about_packet_arrival(packet)
        
        # handle packet by the schedulers logic
        self.apply_scheduling_logic_for_packet(packet)
    
    def apply_scheduling_logic_for_packet(self, packet: Packet):
        raise NotImplemented
    
    def trigger_scheduling_logic(self) -> bool:
        return False
    
    def notify_sfi_finished_processing_of_packet(self, sfi: SFI, packet: Packet):
        # notification that a sfi finished processing
        pass
    
    def applies_round_robin(self):
        # this method is used for test cases, to run RR tests if the scheduler applies RR logic
        return False
    
    def is_always_able_to_build_full_path(self):
        # this method is used for test cases, to check whether this scheduler should be alble to build always
        # a full path, if requested (incremental=False)
        return True
    
    def supports_cpu_policy(self, cpu_policy: ServerCpuPolicy):
        # this method is used for test cases, to check whether this scheduler should be able to handle the given CPU policy
        return True
    
    def mark_time_scheduling_starts(self):
        assert self.time_scheduling_starts is None
        self.time_scheduling_starts = time.time()
    
    def reset_timer(self):
        assert self.time_scheduling_starts is not None
        self.time_scheduling_starts = None
    
    def get_time_delta_of_scheduling(self):
        delta = time.time() - self.time_scheduling_starts
        self.time_scheduling_starts = None
        return delta
    
    def get_load_of_sfis_of_sf(self, sf: int):
        raise NameError("not implemented")


class RejectScheduler(BaseScheduler):
    
    def apply_scheduling_logic_for_packet(self, packet: Packet):
        self.mySFF.packet_queue.pop()
        packet.reject()


class DoSchedulingEvent(BaseEvent):
    def __init__(self, delay, scheduler: 'BaseScheduler'):
        super().__init__(scheduler.sim.currentTime + delay)
        self.scheduler = scheduler
    
    def process_event(self):
        if self.scheduler.sim.DEBUG:
            print("doScheduling event triggers scheduler {0} of sff {1}".
                  format(self.scheduler, self.scheduler.mySFF.id))
        self.scheduler.trigger_scheduling_logic()
