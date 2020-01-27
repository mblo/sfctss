#!/usr/bin/env python3
# coding=utf-8
from itertools import cycle
from statistics import mean
from typing import Iterable

from .core import *
from ..simulator import Sim, SchedulingFailure


class SFF(object):
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            self.counter_packets_put_on_wire: int = 0
            self.consider_link_capacity: bool = None
            self.linkBwCap: List[List[int]] = None
            self.linkBwRemaining: List[List[int]] = None
            self.linkLatency: List[List[int]] = None
            self.latencyProvider: Dict[int, Iterable[int]] = None
            
            self.end_to_end_latency: List[List[int]] = None
            self.end_to_end_bw: List[List[int]] = None
            self.end_to_end_next_hop: List[List[int]] = None
            
            self.allSFFs: Dict[int, 'SFF'] = dict()
    
    @staticmethod
    @Sim.register_simulation_start_hook
    def init_data_structure(sim: Sim):
        # make sure that we reset the data structure only if we are already at the beginning of the simulation,
        # of if somebody calls this function explicitly (before starting the simulation)
        sff_props = sim.props.sff
        if not sim.run or sff_props.linkLatency is None:
            di = len(sff_props.allSFFs)
            sff_props.linkLatency = [[0 for _ in range(di)] for _ in range(di)]
            sff_props.linkBwRemaining = [[0 for _ in range(di)] for _ in range(di)]
            sff_props.linkBwCap = [[0 for _ in range(di)] for _ in range(di)]
            
            sff_props.end_to_end_latency = [[0 for _ in range(di)] for _ in range(di)]
            sff_props.end_to_end_bw = [[0 for _ in range(di)] for _ in range(di)]
            sff_props.end_to_end_next_hop = [[0 for _ in range(di)] for _ in range(di)]
    
    @staticmethod
    @Sim.register_stop_sim_catch_all_packets_hooks
    def callback_get_all_packets_from_sff(sim: Sim):
        for sff in sim.props.sff.allSFFs.values():
            if sff.scheduler.requires_queues_per_class():
                for q in sff.packet_queue_per_class:
                    for p in sff.packet_queue_per_class[q]:
                        p.timeQueueScheduling += p.get_delta_of_time_mark()
                        yield p
            else:
                for p in sff.packet_queue:
                    p.timeQueueScheduling += p.get_delta_of_time_mark()
                    yield p
    
    def __repr__(self):
        return f"SFF({str(self.id)}/{self.get_number_of_queued_packets()})"
    
    def __init__(self, sim: Sim, scheduler):
        if sim.props.sff.linkLatency is not None:
            raise NameError(
                "Cannot create new SFF after initializing the data structures. " +
                "You have to create all SFFs, and then setup the connections.")
        
        self.id = len(sim.props.sff.allSFFs)
        sim.props.sff.allSFFs[self.id] = self
        self.sim = sim
        
        self.scheduler = scheduler
        self.SFIsPerType: Dict[int, List['SFI']] = dict()
        self.servers = set()
        
        self.service_rate_per_sf: Dict[int, float] = {}
        
        # this is either a single queue, or a dictionary of queues
        if self.scheduler.requires_queues_per_class():
            self.packet_queue_per_class: Dict[int, deque] = dict()
        else:
            self.packet_queue: deque = deque()
        
        scheduler.assign_sff(self)
        
        # holds all queued events when the outgoing link does not provide
        # enough capacity
        self.outQueue = dict()
    
    @staticmethod
    def init_end_to_end_paths(sim: Sim):
        di = len(sim.props.sff.allSFFs)
        # initialize with direct edges
        for s in range(di):
            for d in range(di):
                assert (sim.props.sff.end_to_end_bw[s][d] == 0)
        
        # draw randomly 500 latencies and calculate the mean
        expected_latencies = [mean([next(sim.props.sff.latencyProvider[dist]) for _ in range(500)])
                              for dist in sim.props.sff.latencyProvider]
        
        for s in range(di):
            for d in range(di):
                if sim.props.sff.linkBwCap[s][d] > 0:
                    sim.props.sff.end_to_end_bw[s][d] = sim.props.sff.linkBwCap[s][d]
                    sim.props.sff.end_to_end_latency[s][d] = expected_latencies[sim.props.sff.linkLatency[s][d]]
                    sim.props.sff.end_to_end_next_hop[s][d] = d
        
        # this is simply floyd warshall
        for via in range(di):
            for s in range(di):
                for d in range(di):
                    if s == d:
                        continue
                    if s == via or d == via:
                        continue
                    # check if s->d using "via" is a valid connection
                    if sim.props.sff.end_to_end_bw[s][via] > 0 and sim.props.sff.end_to_end_bw[via][d] > 0:
                        # faster?
                        new_latency = sim.props.sff.end_to_end_latency[s][via] + sim.props.sff.end_to_end_latency[via][
                            d]
                        if sim.props.sff.end_to_end_bw[s][d] == 0 or new_latency < sim.props.sff.end_to_end_latency[s][
                            d]:
                            # either new latency is faster, or there was no connection before
                            sim.props.sff.end_to_end_latency[s][d] = new_latency
                            sim.props.sff.end_to_end_next_hop[s][d] = sim.props.sff.end_to_end_next_hop[s][via]
                            sim.props.sff.end_to_end_bw[s][d] = min(sim.props.sff.end_to_end_bw[s][via],
                                                                    sim.props.sff.end_to_end_bw[via][d])
    
    def get_number_of_queued_packets(self):
        if self.scheduler.requires_queues_per_class():
            return sum(map(len, self.packet_queue_per_class.values()))
        else:
            return len(self.packet_queue)
    
    def route_packet_to_sfi(self, packet: 'Packet', sfi: 'SFI'):
        if self.sim.DEBUG:
            print("route packet to sfi " + str(sfi))
        
        packet.mark_time()
        sfi_props: 'SFI.Props' = self.sim.props.sfi
        sff_props: SFF.Props = self.sim.props.sff
        
        delay = next(sff_props.latencyProvider[sfi_props.latency_provider])
        self.sim.schedule_event(NetworkDelayEvent(delay=delay,
                                                  inner_packet=packet,
                                                  source=self,
                                                  dest_id=sfi.id,
                                                  source_is_sff=True,
                                                  dest_is_sff=False))
    
    def free_bw_resource_to_dest_id(self, dest_id, packet: Packet):
        assert self.sim.props.sff.consider_link_capacity
        self.sim.props.sff.linkBwRemaining[self.id][dest_id] += packet.transmission_size
    
    # sends a packet to a SFF
    def route_packet_to_sff_id(self, packet: Packet, dest_id):
        if self.sim.DEBUG:
            print("route packet to sff" + str(dest_id))
        # mark current time for statistics
        packet.mark_time()
        
        sff_props: SFF.Props = self.sim.props.sff
        
        if sff_props.linkBwCap[self.id][dest_id] < packet.transmission_size:
            print("end 2 end links says capacity:{0} using next hop {1}".
                  format(sff_props.end_to_end_bw[self.id][dest_id], sff_props.end_to_end_next_hop[self.id][dest_id]))
            raise NameError(f"cannot route this packet {self.id}->{dest_id}, because bw capacity " +
                            f"({sff_props.linkBwCap[self.id][dest_id]}) is not sufficient for packet " +
                            f"of size ({packet.transmission_size})!")
        if sff_props.consider_link_capacity:
            if not (dest_id in self.outQueue):
                self.outQueue[dest_id] = []
            if (len(self.outQueue[dest_id]) == 0 and packet.transmission_size <=
                    sff_props.linkBwRemaining[self.id][dest_id]):
                self.put_packet_on_wire(packet=packet, dest_id=dest_id)
            else:
                # enqueue packet
                self.outQueue[dest_id].append(packet)
        else:
            self.put_packet_on_wire(packet=packet, dest_id=dest_id)
    
    # internal method, don't call directly
    # this method does not check if some packet are in the out queue
    # use route_packet_to_sff_id() for sending packets to a SFF
    def put_packet_on_wire(self, packet: Packet, dest_id):
        packet.timeQueueNetwork += packet.get_delta_of_time_mark()
        packet.mark_time()
        sff_props: SFF.Props = self.sim.props.sff
        # enough bw so that we can send the packet immediately
        delay = next(sff_props.latencyProvider[sff_props.linkLatency[self.id][dest_id]])
        self.sim.schedule_event(NetworkDelayEvent(delay=delay,
                                                  inner_packet=packet, source=self, dest_id=dest_id))
        if sff_props.consider_link_capacity:
            sff_props.linkBwRemaining[self.id][dest_id] -= packet.transmission_size
    
    def route_packet_to_next_hop(self, packet: Packet):
        next_hop_type, next_hop = packet.fullPath[packet.pathPosition]
        packet.pathPosition += 1
        
        if self.sim.PACKET_ID_TO_DEBUG == packet.id:
            print(f'** packet at sff {self.id}, route to next hop')
        
        if next_hop_type == 'SFF':
            if self.sim.DEBUG:
                print(". packet goes to another SFF")
            # we send this packet to the other SFF
            if packet.id == self.sim.PACKET_ID_TO_DEBUG:
                print("** debug packet route to {0}".format(str(next_hop)))
            self.route_packet_to_sff_id(packet, next_hop.id)
        elif next_hop_type == 'SFI':
            if self.sim.DEBUG:
                print(". packet goes to a SFI")
            # we send this packet to the SFI
            self.route_packet_to_sfi(packet, next_hop)
        else:
            NameError(f"unknown next hop: {next_hop_type}")
    
    def check_and_update_packet_and_return_if_process_locally(self, packet: Packet, source: str) -> bool:
        if self.sim.TRACE_PACKET_PATH:
            packet.visitedHops.append(self)
        if self.sim.DEBUG:
            print(f'at SFF {self.id}, receive a packet from {source}; '
                  f'packet: id:{packet.id} sfc:{packet.flow.sfTypeChain} '
                  f'remaining path:{packet.fullPath[packet.pathPosition:]}')
        if packet.id == self.sim.PACKET_ID_TO_DEBUG:
            print(
                f"** @{self.sim.currentTime} debug packet at {str(self)}, path={packet.fullPath}, coming from {source}")
        if packet.flow.qosMaxDelay < (self.sim.currentTime - packet.time_ingress):
            if self.sim.DEBUG:
                print(". drop packet because of timeout")
            # print(". drop packet because of timeout " + source)
            
            packet.drop_timed_out(self)
            return False
        
        if packet.processing_done:
            # do we have to add the path to the egress?
            if len(packet.toBeVisited) == 0 and len(
                    packet.fullPath) == packet.pathPosition and self.id != packet.flow.desiredEgressSSFid:
                # add the path to the egress
                path_to_dest = self.get_multi_hop_path_for(self.sim, self.id, packet.flow.desiredEgressSSFid)
                for sff_id in path_to_dest:
                    packet.fullPath.append((SFF.__name__, self.sim.props.sff.allSFFs[sff_id]))
            
            # we reached already the egress?
            if len(packet.fullPath) == packet.pathPosition:
                if self.sim.DEBUG:
                    print(". packet reached egress")
                
                # we should be at the egress
                assert (self.id == packet.flow.desiredEgressSSFid)
                # there should be no remaining sf type
                assert (len(packet.toBeVisited) == 0)
                
                packet.done()
                return False
            else:
                self.route_packet_to_next_hop(packet)
                return False
        elif len(packet.fullPath) > packet.pathPosition:
            self.route_packet_to_next_hop(packet)
            return False
        
        return True
    
    def handle_packet_from_ingress(self, packet: Packet):
        if self.check_and_update_packet_and_return_if_process_locally(packet, f'ingress'):
            self.apply_logic_packet_from_ingress(packet)
    
    def handle_packet_from_other_sff(self, packet: Packet, other_sff: 'SFF'):
        if self.check_and_update_packet_and_return_if_process_locally(packet, f'a sff {other_sff.id}'):
            self.apply_logic_packet_from_other_sff(packet, other_sff)
    
    def handle_packet_from_sfi(self, packet: Packet, sfi: 'SFI'):
        if self.check_and_update_packet_and_return_if_process_locally(packet, f'a sfi {sfi.id}'):
            self.apply_logic_packet_from_sfi(packet, sfi)
    
    def handle_packet_from_scheduler(self, packet: Packet):
        if self.sim.PACKET_ID_TO_DEBUG == packet.id:
            print(f'** receive packet from scheduler..')
            Packet.debug_print_path(packet.fullPath, packet.pathPosition)
        
        if self.check_and_update_packet_and_return_if_process_locally(packet, 'scheduler'):
            Packet.debug_packet(packet)
            raise NameError(f'scheduler returns a packet {packet.id}, but sff does not know how to proceed with this '
                            f'packet')
    
    def apply_logic_packet_from_other_sff(self, packet, other_sff: 'SFF'):
        self.put_packet_in_queue(packet)
        self.inform_scheduler_about_packet(packet)
    
    def apply_logic_packet_from_ingress(self, packet):
        self.put_packet_in_queue(packet)
        self.inform_scheduler_about_packet(packet)
    
    def apply_logic_packet_from_sfi(self, packet, sfi: 'SFI'):
        self.put_packet_in_queue(packet)
        self.inform_scheduler_about_packet(packet)
    
    def inform_scheduler_about_packet(self, packet):
        if self.sim.DEBUG or packet.id == self.sim.PACKET_ID_TO_DEBUG:
            print(f'** send packet ({packet.id}) to scheduler')
        packet.seenByScheduler += 1
        try:
            self.scheduler.handle_packet_arrival(packet=packet)
        except SchedulingFailure as e:
            if self.sim.STOP_SIMULATION_IF_SCHEDULER_WAS_UNSUCCESSFUL:
                raise e
            elif self.sim.DEBUG:
                print(
                    ". scheduler was unsuccessful to schedule a packet: " +
                    str(e))
    
    def put_packet_in_queue(self, packet):
        if self.scheduler.requires_queues_per_class():
            queue = Flow.get_packet_class_of_packet(packet)
            
            if queue not in self.packet_queue_per_class:
                self.packet_queue_per_class[queue] = deque()
            if self.sim.DEBUG:
                print(f'. add packet of sfc {packet.flow.sfc_identifier} '
                      f'at position {packet.sfc_position} to queue {queue}')
            self.packet_queue_per_class[queue].append(packet)
        else:
            self.packet_queue.append(packet)
        
        packet.mark_time()  # mark time when this packet was queued to the scheduler
    
    def register_sfi(self, sfi: 'SFI'):
        if not (sfi.of_type in self.SFIsPerType):
            self.SFIsPerType[sfi.of_type] = []
        # check here that we don't get for a server multiplel SFI of the same
        # type
        for otherSFI in self.SFIsPerType[sfi.of_type]:
            if otherSFI.server == sfi.server:
                raise NameError(
                    "this configuration is not allowed! A SFF should" +
                    " see for a single server only one SFI instance per type!")
        self.SFIsPerType[sfi.of_type].append(sfi)
        self.servers.add(sfi.server)
        
        if sfi.of_type not in self.service_rate_per_sf:
            self.service_rate_per_sf[sfi.of_type] = 0
        self.service_rate_per_sf[sfi.of_type] += sfi.get_expected_processing_rate()
    
    def sfi_finishes_processing_of_packet(self, sfi: 'SFI', packet: Packet):
        # the given SFI is finished with processing, so depeding on the server cpu sharing policy,
        # this server might be free now
        self.scheduler.notify_sfi_finished_processing_of_packet(sfi, packet)
    
    @staticmethod
    def get_multi_hop_bw_for(sim: Sim, source_id, dest_id):
        sff_props: SFF.Props = sim.props.sff
        if sff_props.end_to_end_bw[0][1] == 0:
            SFF.init_end_to_end_paths(sim)
        
        if sff_props.end_to_end_bw[source_id][dest_id] == 0:
            raise NameError("graph is not connected")
        
        return sff_props.end_to_end_bw[source_id][dest_id]
    
    @staticmethod
    def get_multi_hop_latency_for(sim: Sim, source_id, dest_id):
        if source_id == dest_id:
            return 0
        
        sff_props: SFF.Props = sim.props.sff
        if sff_props.end_to_end_bw[0][1] == 0:
            SFF.init_end_to_end_paths(sim)
        
        if sff_props.end_to_end_bw[source_id][dest_id] == 0:
            raise NameError("graph is not connected")
        
        return sff_props.end_to_end_latency[source_id][dest_id]
    
    @staticmethod
    def get_next_hop_for(sim: Sim, source_id, dest_id):
        sff_props: SFF.Props = sim.props.sff
        if sff_props.end_to_end_bw[0][1] == 0:
            SFF.init_end_to_end_paths(sim)
        
        if sff_props.end_to_end_bw[source_id][dest_id] == 0:
            raise NameError("graph is not connected")
        
        return sff_props.end_to_end_next_hop[source_id][dest_id]
    
    @staticmethod
    def get_multi_hop_route_between_ids(sim: Sim, from_id, to_id):
        if from_id == to_id:
            return []
        steps = SFF.get_multi_hop_path_for(sim, from_id, to_id)
        all_sff: SFF.Props = sim.props.sff.allSFFs
        return [(SFF.__name__, all_sff[step]) for step in steps]
    
    @staticmethod
    def get_multi_hop_path_for(sim: Sim, source_id, dest_id):
        sff_props: SFF.Props = sim.props.sff
        if sff_props.end_to_end_bw[0][1] == 0:
            SFF.init_end_to_end_paths(sim)
        
        if sff_props.end_to_end_bw[source_id][dest_id] == 0:
            raise NameError(f"graph is not connected; failed to find path for {source_id}->{dest_id}")
        
        path = []
        
        next_hop = sff_props.end_to_end_next_hop[source_id][dest_id]
        path.append(next_hop)
        
        while next_hop != dest_id:
            next_hop = sff_props.end_to_end_next_hop[next_hop][dest_id]
            path.append(next_hop)
        
        return path
    
    @staticmethod
    def check_if_connection_exists(sim: Sim, source_id, dest_id):
        if sim.props.sff.linkBwCap[source_id][dest_id] > 0:
            return True
        return False
    
    @staticmethod
    def setup_latency_distribution(sim: Sim, id: int, values: List[int]):
        sff_props: SFF.Props = sim.props.sff
        if sff_props.latencyProvider is None:
            sff_props.latencyProvider = {}
        sff_props.latencyProvider[id] = cycle(values)
    
    @staticmethod
    def setup_connection(sim: Sim, source_id, destination_id,
                         bw_cap, latency_provider: int, bidirectional: bool = False):
        sff_props: SFF.Props = sim.props.sff
        # do we have to setup the data structures?
        if sff_props.linkLatency is None:
            SFF.init_data_structure(sim)
        assert (len(sff_props.linkLatency) > source_id)
        assert (len(sff_props.linkLatency) > destination_id)
        assert (source_id != destination_id)
        assert sff_props.latencyProvider is not None
        if latency_provider not in sff_props.latencyProvider:
            raise NameError(f"unknown latency provider id given: {latency_provider}")
        
        # bw_cap == 0, means that there is no connection!
        sff_props.linkBwCap[source_id][destination_id] = int(bw_cap)
        sff_props.linkLatency[source_id][destination_id] = latency_provider
        sff_props.linkBwRemaining[source_id][destination_id] = int(bw_cap)
        if bidirectional:
            sff_props.linkBwCap[destination_id][source_id] = int(bw_cap)
            sff_props.linkLatency[destination_id][source_id] = latency_provider
            sff_props.linkBwRemaining[destination_id][source_id] = int(bw_cap)
    
    @staticmethod
    def get_delay_of_connection(src: 'SFF', dest: 'SFF') -> int:
        if src == dest:
            return 0
        assert src.sim == dest.sim
        return SFF.get_multi_hop_latency_for(src.sim, src.id, dest.id)
    
    @staticmethod
    def set_consider_bw_capacity(sim: Sim, consider_bandwidth_capacity_of_links: bool):
        sim.props.sff.consider_link_capacity = consider_bandwidth_capacity_of_links
