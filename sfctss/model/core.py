#!/usr/bin/env python3
# coding=utf-8
from collections import deque
from enum import unique, Enum
from typing import Dict, List, Tuple

from ..events import PacketHoldingEvent
from ..simulator import Sim


@unique
class ServerCpuPolicy(Enum):
    static = 1
    dynamic = 2
    one_at_a_time = 3


class Flow(object):
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            # parameters: the sf type chain configuration this flow belongs to
            self.lastId: int = 0
            
            # the sfc id is simply the actual sfc written as a string
            # the sfc class is an integer which refers to the buffer/class of this sfc of position 0.
            #   so when this sfc has 3 entries, it reserves buffer/class "sfc class", "sfc class"+1, "sfc class"+2
            
            # holds a map from sfc id to sfc class
            self.sfc_classes: Dict[str, int] = dict()
            
            self.statistics_drops_per_class: Dict[int, int] = dict()
            
            # holds a map from sfc class to deadline of this sfc
            self.sfc_class_to_deadline: Dict[int, int] = dict()
            self.max_deadline: int = None
            
            # holds a map from sfc class to egress id of this sfc
            self.sfc_class_to_egress: Dict[int, int] = dict()
            
            # holds a map from sfc class to actual (sf, bool: last_sf_in_sfc)
            self.sfc_class_to_sf: Dict[int, Tuple[int, bool]] = dict()
            
            # holds the next class which is not used so far
            self.sfc_next_free_class = 0
            
            # if set true, the same SFC but with different egress, will end up in different classes
            self.individual_class_per_egress: bool = False
    
    @staticmethod
    def get_sfc_identifier(sim: Sim, sfc_type: list, egress_id: int):
        if sim.props.flow.individual_class_per_egress:
            return "-".join(str(c) for c in sfc_type) + f":{egress_id}"
        else:
            return "-".join(str(c) for c in sfc_type)
    
    @staticmethod
    def register_sfc_for_packet_classes(sim: Sim, sfc_type: list, qos_max_delay: int, egress_id: int):
        sfc_identifier = Flow.get_sfc_identifier(sim, sfc_type, egress_id)
        assert sfc_identifier not in sim.props.flow.sfc_classes
        
        sim.props.flow.sfc_classes[sfc_identifier] = sim.props.flow.sfc_next_free_class
        sim.props.flow.sfc_next_free_class += len(sfc_type)
        
        if sim.run:
            raise NameError("it is not allowed to create new flows of NEW SFC/egress combinations after " +
                            "starting the simulation! (because of caching)")
        
        if sim.DEBUG:
            print(f"..register sfc identifier {sfc_identifier} for packet class "
                  f"{sim.props.flow.sfc_classes[sfc_identifier]}")
        
        for i in range(len(sfc_type)):
            this_class_identifier = sim.props.flow.sfc_classes[sfc_identifier] + i
            sim.props.flow.sfc_class_to_sf[this_class_identifier] = (
                sfc_type[i], (i + 1) == len(sfc_type))
            if this_class_identifier in sim.props.flow.sfc_class_to_deadline:
                raise NameError("we store the qos delay per class, which is used by the optimal scheduler, however," +
                                f" your setup has different values for the same SFC {sfc_identifier}")
            sim.props.flow.sfc_class_to_deadline[this_class_identifier] = qos_max_delay
            if sim.props.flow.individual_class_per_egress:
                sim.props.flow.sfc_class_to_egress[this_class_identifier] = egress_id
            
            for sff in sim.props.sff.allSFFs.values():
                if sff.scheduler.requires_queues_per_class():
                    sff.packet_queue_per_class[this_class_identifier] = deque()
            sim.props.flow.statistics_drops_per_class[this_class_identifier] = 0
    
    @staticmethod
    def get_packet_class_of_packet(packet: 'Packet'):
        return packet.flow.sim.props.flow.sfc_classes[packet.flow.sfc_identifier] + packet.sfc_position
    
    @staticmethod
    def get_sf_and_eoc_of_packet(packet: 'Packet') -> tuple:
        return Flow.get_sf_and_eoc_of(packet.flow.sim, packet.flow.sfc_identifier, packet.sfc_position)
    
    @staticmethod
    def get_sf_and_eoc_of(sim: Sim, sfc_identifier: str, at_position: int) -> tuple:
        flow_pros: Flow.Props = sim.props.flow
        return flow_pros.sfc_class_to_sf[flow_pros.sfc_classes[sfc_identifier] + at_position]
    
    @staticmethod
    def debug_get_sfc_identifier_and_pos_of_packet_class(sim: Sim, packet_class: int):
        if not sim.DEBUG:
            # print("WARNING this method is not intended to be used for normal runs")
            pass
        # find the sfc_identifier that has the largest packet_class but still smaller/equal than the given
        flow_pros: Flow.Props = sim.props.flow
        found_sfc = None
        found_packet_class = -1
        for other_sfc_identifier in flow_pros.sfc_classes:
            if found_sfc is None or (
                    packet_class >= flow_pros.sfc_classes[other_sfc_identifier] > found_packet_class):
                found_sfc = other_sfc_identifier
                found_packet_class = flow_pros.sfc_classes[other_sfc_identifier]
        assert found_sfc is not None
        return found_sfc, (packet_class - found_packet_class)
    
    def __init__(self, sim: Sim, sf_type_chain: List[int], qos_max_delay: int,
                 desired_egress_ssf_id: int, ingress_sff_id: int, start_time: int = 0):
        self.sim = sim
        flow_props: Flow.Props = sim.props.flow
        flow_props.lastId += 1
        self.id = flow_props.lastId
        self.sfTypeChain = [int(i) for i in sf_type_chain]
        self.desiredEgressSSFid = desired_egress_ssf_id
        self.ingress_sff_id = int(ingress_sff_id)
        self.qosMaxDelay = qos_max_delay
        self.start_time = start_time
        
        assert len(self.sfTypeChain) > 0
        
        if flow_props.max_deadline is None or flow_props.max_deadline < self.qosMaxDelay:
            flow_props.max_deadline = self.qosMaxDelay
        
        self.sfc_identifier = Flow.get_sfc_identifier(sim, sf_type_chain, desired_egress_ssf_id)
        if self.sfc_identifier not in sim.props.flow.sfc_classes:
            Flow.register_sfc_for_packet_classes(sim, sf_type_chain, qos_max_delay, desired_egress_ssf_id)
        
        self.sfc_class = sim.props.flow.sfc_classes[self.sfc_identifier]
    
    def __str__(self):
        return "Flow(id={0},chain={1},egress={2},qos={3})".format(
            self.id, self.sfTypeChain, self.desiredEgressSSFid, self.qosMaxDelay)


class IngressEvent(PacketHoldingEvent):
    
    def __init__(self, inner_packet: 'Packet', sff_id):
        super().__init__(inner_packet.time_ingress, inner_packet)
        self.sff_id = sff_id
    
    def update_packet_time_tracking(self):
        pass
    
    # send packet to ingress SFF
    def process_event(self):
        sim = self.inner_packet.flow.sim
        sim.last_packet_ingress_time = self.time
        
        stat_props = sim.props.sim_stats
        if stat_props.workloadStats is not None:
            stat_props.workloadStats.add_entry(self.time,
                                               self.inner_packet.flow.id,
                                               '-'.join(map(str, self.inner_packet.flow.sfTypeChain)),
                                               self.sff_id,
                                               self.inner_packet.flow.desiredEgressSSFid,
                                               self.inner_packet.flow.qosMaxDelay)
        
        packet_props = self.inner_packet.flow.sim.props.packet
        packet_props.counter_packet_in_system += 1
        packet_props.counter_ingress_packets += 1
        sim.props.sff.allSFFs[self.sff_id].handle_packet_from_ingress(self.inner_packet)


class Packet(object):
    teardown_hooks = []
    
    @staticmethod
    def register_tear_down(func):
        Packet.teardown_hooks.append(func)
        return func
    
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            self.counter_ingress_packets: int = 0
            self.counter_packet_in_system: int = 0
            
            # We count the number of packets which are still in the simulation after ending the workload, so that
            # we are able to calculate the effective avg delay correctly
            self.counter_packet_after_workload_end_in_system_no_timeout: int = 0
            
            self.all: list = []
            self.statsRatiosQos: float = 0
            self.statsPacketsSuccessfulProcessed: int = 0
            self.statsPacketsRejectedSchedule: int = 0
            self.statsPacketsRejectedProcessingDelay: int = 0
            self.statsSumDelay: int = 0
            self.lastId: int = 0
            self.statsPacketsTotalCount: int = 0
            
            self.dones: list = []
    
    def __init__(self, time_ingress: int, flow: Flow, transmission_size: int):
        self.scheduler_flag = dict()
        packet_props: Packet.Props = flow.sim.props.packet
        if flow.sim.KEEP_LIST_OF_ALL_PACKETS:
            packet_props.all.append(self)
        packet_props.lastId += 1
        self.id = packet_props.lastId
        self.time_ingress = int(time_ingress)
        self.flow = flow
        
        assert flow is not None
        assert self.time_ingress >= self.flow.start_time
        
        self.processing_done = False
        
        # holds the position with respect to the sfc, so position 1 means,
        # that the next SFI to be processed is of type of the second element in the chain
        self.sfc_position = 0
        
        self.pathPosition = 0  # holds the position of the total path
        # holds the full path, some schedulers will build this list
        # incrementally
        self.fullPath = []
        # holds all remaining SF types, that need to be visited
        self.toBeVisited = list(flow.sfTypeChain)
        
        self.ingress_sff_id = flow.ingress_sff_id
        
        self.transmission_size = int(transmission_size)
        self.seenByScheduler = 0
        packet_props.statsPacketsTotalCount += 1
        
        # statistic values
        self.timeQueueProcessing = 0
        self.timeQueueNetwork = 0
        self.timeQueueScheduling = 0
        
        self.timeProcessing = 0
        self.timeNetwork = 0
        self.realTimeScheduling = 0
        
        self.timeMarker = None
        
        self.delay = None
        self.final_state = None
        
        self.callback_when_be_dropped = None
        
        if flow.sim.TRACE_PACKET_PATH:
            self.visitedHops = []
    
    @staticmethod
    @Sim.register_sim_oneliner_text_provider
    def sim_oneline_info(sim: Sim):
        packet_props: Packet.Props = sim.props.packet
        
        avg_succ = (0 if (packet_props.statsPacketsSuccessfulProcessed
                          + packet_props.statsPacketsRejectedProcessingDelay) == 0 else
                    round(packet_props.statsPacketsSuccessfulProcessed
                          / (packet_props.statsPacketsSuccessfulProcessed
                             + packet_props.statsPacketsRejectedProcessingDelay), ndigits=4))
        
        return f'Packets delivered {packet_props.statsPacketsSuccessfulProcessed}/' \
               f'{packet_props.statsPacketsTotalCount} ' \
               f'ingress: {packet_props.counter_ingress_packets} ' \
               f'(timeout:{packet_props.statsPacketsRejectedProcessingDelay} ' \
               f'reject:{packet_props.statsPacketsRejectedSchedule} ' \
               f'avg succ:{avg_succ} ) '
    
    @staticmethod
    def create_wrap_in_event(time_ingress: int, flow: Flow, transmission_size: int) -> 'IngressEvent':
        p = Packet(time_ingress, flow, transmission_size)
        return IngressEvent(inner_packet=p, sff_id=flow.ingress_sff_id)
    
    @staticmethod
    def create_and_schedule(time_ingress: int, flow: Flow, transmission_size: int, ingress_sff_id: int):
        return flow.sim.schedule_event(
            Packet.create_wrap_in_event(time_ingress, flow, transmission_size, ingress_sff_id))
    
    def set_callback_when_dropped(self, callback):
        self.callback_when_be_dropped = callback
    
    # mark the current time, used for statistics purpose
    def mark_time(self):
        assert (self.timeMarker is None)
        self.timeMarker = self.flow.sim.currentTime
    
    # returns the delta of current time and time marker
    def get_delta_of_time_mark(self):
        assert (self.timeMarker is not None)
        tmp = self.flow.sim.currentTime - self.timeMarker
        self.timeMarker = None
        return tmp
    
    def tear_down(self, final_state):
        sim: Sim = self.flow.sim
        packet_props: Packet.Props = sim.props.packet
        
        if sim.PACKET_ID_TO_DEBUG == self.id:
            print(f"Debug packet gets tear down")
            Packet.debug_packet(self)
        
        if sim.KEEP_LIST_OF_ALL_PACKETS:
            packet_props.all.remove(self)
        self.delay = sim.currentTime - self.time_ingress
        self.final_state = final_state
        packet_props.counter_packet_in_system -= 1
        packet_props.statsSumDelay += self.delay
        
        for f in Packet.teardown_hooks:
            f(self)
        
        self.flow = None
        self.callback_when_be_dropped = None
        self.fullPath = None
        self.scheduler_flag = None
    
    def handle_stop_simulation(self):
        # this method will be called when the simulation done, but the packet is somewhere in the simulation
        packet_props: Packet.Props = self.flow.sim.props.packet
        if (self.flow.sim.currentTime - self.time_ingress) >= self.flow.qosMaxDelay:
            self.drop_timed_out(end_of_sim=True)
        else:
            packet_props.counter_packet_in_system -= 1
            packet_props.statsPacketsSuccessfulProcessed += 1
            packet_props.counter_packet_after_workload_end_in_system_no_timeout += 1
    
    def drop_timed_out(self, caller=None, end_of_sim=False):
        if self.id == self.flow.sim.PACKET_ID_TO_DEBUG:
            print(f"** drop debug packet because of timeout, caller:{caller}")
        
        props = self.flow.sim.props
        props.packet.statsPacketsRejectedProcessingDelay += 1
        
        if self.processing_done:
            # this is a very rare case, but the timeout happens during forwarding the packet to the desired egress sff
            # so this means, we would get the wrong packet class, because the pointer in already on the next entry
            class_of_packet = Flow.get_packet_class_of_packet(self) - 1
            props.flow.statistics_drops_per_class[class_of_packet] += 1
        else:
            props.flow.statistics_drops_per_class[Flow.get_packet_class_of_packet(self)] += 1
        
        if (self.callback_when_be_dropped is not None) and (not end_of_sim):
            self.callback_when_be_dropped(self, caller)
            self.callback_when_be_dropped = None
        self.tear_down("timeout")
    
    def reject(self):
        if self.id == self.flow.sim.PACKET_ID_TO_DEBUG:
            print("** reject debug packet")
        
        self.flow.sim.props.packet.statsPacketsRejectedSchedule += 1
        self.tear_down("rejectSchedule")
    
    def done(self):
        packet_props: Packet.Props = self.flow.sim.props.packet
        if self.id == self.flow.sim.PACKET_ID_TO_DEBUG:
            print("** debug packet is done")
        packet_props.statsPacketsSuccessfulProcessed += 1
        
        self.delay = self.flow.sim.currentTime - self.time_ingress
        packet_props.statsRatiosQos += float(self.delay) / self.flow.qosMaxDelay
        
        self.tear_down("done")
    
    @staticmethod
    def debug_packet(packet):
        print("config:")
        print("\t id: {0}".format(str(packet.id)))
        print("\t time ingress: {0}".format(str(packet.time_ingress)))
        print("\t flow: {0}".format(str(packet.flow)))
        print("\t sfc position processed of SFIs: {0}".format(str(packet.sfc_position)))
        print("\t SFs not yet scheduled: {0}".format(str(packet.toBeVisited)))
        print("\t path position: {0}".format(str(packet.pathPosition)))
        print("\t transmission_size: {0}".format(str(packet.transmission_size)))
        print("\t seenByScheduler: {0}".format(str(packet.seenByScheduler)))
        if packet.flow.sim.TRACE_PACKET_PATH:
            print("\t visited hops: {0}".format(
                str(list(map(str, packet.visitedHops)))))
        
        Packet.debug_print_path(packet.fullPath, current_position=packet.pathPosition)
    
    @staticmethod
    def debug_print_path(path: list, current_position=None):
        print("this path has {0} steps".format(len(path)))
        for i, (t, instance) in enumerate(path):
            if current_position is not None and i == current_position:
                print("\t ---- current position")
            if t == 'SFF':
                print("\ttype: SFF instance id: {0}".format(str(instance.id)))
            elif t == 'SFI':
                print("\ttype: SFI type: {0} @ SFF {1} of Server {2}".format(str(instance.of_type), instance.sffId,
                                                                             instance.server.id))
            else:
                raise NameError(f"Something goes wrong with the path! - {path}")


class NetworkDelayEvent(PacketHoldingEvent):
    def __init__(self, delay, inner_packet: Packet, source, dest_id,
                 source_is_sff=True, dest_is_sff=True):
        sim = inner_packet.flow.sim
        super().__init__(sim.currentTime + delay, inner_packet)
        if sim.PACKET_ID_TO_DEBUG == inner_packet.id:
            print("** create network delay event of debug packet")
        self.source = source
        self.destID = dest_id
        self.source_is_sff = source_is_sff
        self.dest_is_sff = dest_is_sff
        sim.props.sff.counter_packets_put_on_wire += 1
    
    def update_packet_time_tracking(self):
        self.inner_packet.timeNetwork += self.inner_packet.get_delta_of_time_mark()
    
    # free resources of sourceSFF, and send packet to destSSF
    def process_event(self):
        sff_props = self.inner_packet.flow.sim.props.sff
        if sff_props.consider_link_capacity and self.source_is_sff and self.dest_is_sff:
            self.source.free_bw_resource_to_dest_id(self.destID, self.inner_packet)
            # check if we have to deque the next packet
            if len(self.source.outQueue[self.destID]) > 0:
                # check if available bw is enough for the next packet,
                # or if we have to wait till the next packet is done
                if (self.source.outQueue[self.destID][0].transmission_size <=
                        sff_props.linkBwRemaining[self.source.id][self.destID]):
                    self.source.put_packet_on_wire(
                        packet=self.source.outQueue[self.destID].pop(0), dest_id=self.destID)
        
        if self.inner_packet.flow.sim.PACKET_ID_TO_DEBUG == self.inner_packet.id:
            sim = self.inner_packet.flow.sim
            print(f"** @{sim.currentTime} at event {sim.current_event} " +
                  f"network delay event is done, so get time delta of network")
        self.update_packet_time_tracking()
        # do statistics for network time
        if self.source_is_sff and self.dest_is_sff:
            sff_props.allSFFs[self.destID].handle_packet_from_other_sff(self.inner_packet, self.source)
        elif self.dest_is_sff:
            sff_props.allSFFs[self.destID].handle_packet_from_sfi(self.inner_packet, self.source)
        elif self.source_is_sff:
            self.inner_packet.flow.sim.props.sfi.all_sfi[self.destID].enqueue_packet(self.inner_packet)
        else:
            if self.destID != self.source.sffId:
                raise NameError("invalid action, trying to send a packet from one SFI to another SFI (directly)")
            self.inner_packet.flow.sim.props.sfi.all_sfi[self.destID].enqueue_packet(self.inner_packet)
