# coding=utf-8

from .core import *
from collections import deque
from typing import Set, Tuple
import math



class GreedyShortestDeadlineFirstScheduler(BaseScheduler):
    
    def __init__(self, sim: Sim,
                 incremental: bool = True, oracle=True,
                 admission_control_threshold_low: float = 0.8,
                 admission_control_threshold_high: float = 1.1
                 ):
        super().__init__(sim=sim,
                         incremental=incremental,
                         oracle=oracle,
                         activate_acp=not oracle,
                         admission_control_threshold_low=admission_control_threshold_low,
                         admission_control_threshold_high=admission_control_threshold_high)
        self.admission_control_threshold_low = admission_control_threshold_low
        self.admission_control_threshold_high = admission_control_threshold_high
        
        assert admission_control_threshold_low < admission_control_threshold_high
        
        if not self.oracle:
            self.rate_estimator: Dict[int, RateEstimator] = {}
    
    def is_always_able_to_build_full_path(self):
        return self.oracle and not self.incremental
    
    def apply_scheduling_logic_for_packet(self, packet: Packet):
        sff_props: SFF.Props = self.sim.props.sff
        
        # get the packet from the scheduler's queue
        if self.requires_queues_per_class():
            popped_packet = self.mySFF.packet_queue_per_class[Flow.get_packet_class_of_packet(packet)].pop()
        else:
            popped_packet = self.mySFF.packet_queue.pop()
        
        assert popped_packet == packet
        
        packet.timeQueueScheduling += packet.get_delta_of_time_mark()
        
        self.mark_time_scheduling_starts()
        
        # this packet starts from this SFF, so search from this point
        p_at_sff: SFF = self.mySFF
        
        if self.sim.DEBUG:
            print(". heuristic scheduler @ {0}".format(p_at_sff))
        
        scheduled_path = []
        
        incremental_path = False
        
        # for each remaining sf type of the requested chain of this packet
        while 0 < len(packet.toBeVisited) and not incremental_path:
            next_sf_type = packet.toBeVisited.pop(0)
            # we need to get a SFI for this corresponding sf type
            # try to get this SFI from the SFF where this packet is currently
            # (p_at_sff)
            
            sff_to_check = sff_props.allSFFs if self.oracle else [p_at_sff.id]
            
            sfi_to_check = []
            #  get a list of all possible SFIs
            # calculate for each of these SFIs the cost value
            best_latency = -1
            for sffIDToAsk in sff_to_check:
                sff_to_ask = sff_props.allSFFs[sffIDToAsk]
                delay_of_sff_connection = 0 if p_at_sff == sff_to_ask else SFF.get_multi_hop_latency_for(self.sim,
                                                                                                         p_at_sff.id,
                                                                                                         sff_to_ask.id)
                if best_latency != -1 and best_latency <= delay_of_sff_connection:
                    # we skip this sff if the latency to this sff is bigger than the best option we found so far
                    continue
                
                if next_sf_type in sff_to_ask.SFIsPerType:
                    for sfi in sff_to_ask.SFIsPerType[next_sf_type]:
                        delay = sfi.get_expected_waiting_time() + sfi.get_expected_processing_time()
                        if p_at_sff != sff_to_ask:
                            delay += SFF.get_delay_of_connection(p_at_sff, sff_to_ask)
                        if best_latency == -1:
                            best_latency = delay
                        best_latency = min(best_latency, delay)
                        sfi_to_check.append((delay, sff_to_ask, sfi))
            
            if len(sfi_to_check) == 0:
                if not self.oracle:
                    raise NameError(f'something is going wrong. I don\'t have any SFI which could serve this packet,'
                                    f'but ACP should have handeled this case!?')
                
                packet.realTimeScheduling += self.get_time_delta_of_scheduling()
                try:
                    raise SchedulingFailure("failed to find a path for {0}".format(
                        str(packet.flow.sfTypeChain)))
                except SchedulingFailure as e:
                    raise e
                finally:
                    packet.reject()
            
            # find the sfi with the lowest delay till packet is processed
            best_sfi = sfi_to_check[0]
            for sfi_tuple in sfi_to_check:
                if sfi_tuple[0] <= best_sfi[0]:
                    # update the best sfi, but for equal values we prefer to stay at our sff
                    if sfi_tuple[0] != best_sfi[0] or sfi_tuple[1] == self.mySFF:
                        best_sfi = sfi_tuple
            
            sff_to_ask = best_sfi[1]
            sfi = best_sfi[2]
            
            if self.sim.DEBUG:
                print(". found a SFI at SFF {0}".format(sff_to_ask))
            
            # do we need to go to another SFF, or do we stay at the
            # current SFF?
            if p_at_sff != sff_to_ask:
                # we need to go to a different SFF, but before,
                # we have to go back to the original SFF, so that
                # if the packet is at a SFI, it goes fist back
                # to the SFF of this SFI, and then to the next SFF
                
                # so go to p_at_sff if the previous path element was
                # a SFI
                if (len(scheduled_path) >
                        0 and scheduled_path[-1][0] == SFI.__name__):
                    scheduled_path.append((SFF.__name__, p_at_sff))
                
                path_to_other_sff = SFF.get_multi_hop_path_for(self.sim, p_at_sff.id, sff_to_ask.id)
                if self.sim.DEBUG:
                    print(". path to this guy contains {0} intermediate SFFs".format(len(path_to_other_sff)))
                for next_sff in path_to_other_sff:
                    scheduled_path.append((SFF.__name__, sff_props.allSFFs[next_sff]))
                
                p_at_sff = sff_to_ask
            
            scheduled_path.append((SFI.__name__, sfi))
            
            # if we are in incremental scheduling mode, we set the incremental_path flag,
            # so that we stop scheduling from here on
            if self.incremental:
                # incremental_path, so we found a different SFF,
                # hence we stop scheduling here
                incremental_path = True
                # and then go back to the SFF for scheduling
                scheduled_path.append((SFF.__name__, sff_to_ask))
        
        if not incremental_path:
            # finally, add the egress SFF
            if p_at_sff.id != packet.flow.desiredEgressSSFid:
                # go back to the sff
                scheduled_path.append((SFF.__name__, p_at_sff))
                path_to_dest = SFF.get_multi_hop_path_for(self.sim, p_at_sff.id, packet.flow.desiredEgressSSFid)
                for sff_id in path_to_dest:
                    scheduled_path.append((SFF.__name__, sff_props.allSFFs[sff_id]))
            else:
                scheduled_path.append((SFF.__name__, sff_props.allSFFs[packet.flow.desiredEgressSSFid]))
        
        if self.sim.DEBUG:
            Packet.debug_print_path(scheduled_path)
        
        packet.fullPath += scheduled_path
        self.scheduling_attempts += 1
        
        if packet.id == self.sim.PACKET_ID_TO_DEBUG:
            print("** debug packet visited the scheduler, current status:")
            Packet.debug_packet(packet)
        
        packet.realTimeScheduling += self.get_time_delta_of_scheduling()
        self.mySFF.handle_packet_from_scheduler(packet)


class LoadUnawareRoundRobinScheduler(BaseScheduler):
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            self.sfi_round_robin_marker = dict()
    
    def __init__(self, sim: Sim, incremental: bool = True, oracle=True):
        super().__init__(sim=sim,
                         incremental=incremental,
                         oracle=oracle,
                         activate_acp=False)
    
    def applies_round_robin(self):
        return True
    
    def is_always_able_to_build_full_path(self):
        return not self.incremental
    
    def apply_scheduling_logic_for_packet(self, packet: Packet):
        sfi_props: SFI.Props = self.sim.props.sfi
        sff_props: SFF.Props = self.sim.props.sff
        
        # get the packet from the scheduler's queue
        if self.requires_queues_per_class():
            popped_packet = self.mySFF.packet_queue_per_class[Flow.get_packet_class_of_packet(packet)].pop()
        else:
            popped_packet = self.mySFF.packet_queue.pop()
        
        assert popped_packet == packet
        
        packet.timeQueueScheduling += packet.get_delta_of_time_mark()
        
        self.mark_time_scheduling_starts()
        
        # this packet starts from this SFF, so search from this point
        p_at_sff_id = self.mySFF.id
        
        scheduled_path = []
        
        # for each remaining sf type of the requested chain of this packet
        while 0 < len(packet.toBeVisited):
            next_sf_type = packet.toBeVisited.pop(0)
            
            # get the sfi which has to serve this packet
            if self.static_sfi_rates_per_sf_cum_weights is None:
                self.update_cum_weights_of_sfi_rates(include_own_sff=True)
            
            assert isinstance(next_sf_type, int)
            if (next_sf_type not in self.static_sfi_rates_per_sf_sorted_sfi
                    or next_sf_type not in self.static_sfi_rates_per_sf_cum_weights
                    or len(self.static_sfi_rates_per_sf_sorted_sfi[next_sf_type]) == 0
                    or len(self.static_sfi_rates_per_sf_cum_weights[next_sf_type]) == 0):
                packet.realTimeScheduling += self.get_time_delta_of_scheduling()
                packet.reject()
                if self.sim.DEBUG:
                    print(f"I'm not aware of any SFI of the required type:{next_sf_type}, so I have to reject the packet")
                raise SchedulingFailure(f"failed to find a path for {packet}")
            
            elif (len(self.static_sfi_rates_per_sf_sorted_sfi[next_sf_type]) !=
                  len(self.static_sfi_rates_per_sf_cum_weights[next_sf_type])):
                raise NameError(f'data structures broken')
            
            target_sfi_id = self.sim.random.choices(self.static_sfi_rates_per_sf_sorted_sfi[next_sf_type],
                                                    cum_weights=self.static_sfi_rates_per_sf_cum_weights[next_sf_type],
                                                    k=1)[0]
            
            target_sff_id = sfi_props.all_sfi[target_sfi_id].sffId
            
            # is this a different sff of where we are currently?
            
            if p_at_sff_id != target_sff_id:
                # we need to go to a different SFF, but before,
                # we have to go back to the original SFF, so that
                # if the packet is at a SFI, it goes fist back
                # to the SFF of this SFI, and then to the next SFF
                # (we need to check this, because thi scheduler supports to schedule the whole path at once)
                
                # so go to p_at_sff if the previous path element was
                # a SFI
                if (len(scheduled_path) >
                        0 and scheduled_path[-1][0] == SFI.__name__):
                    scheduled_path.append((SFF.__name__, sff_props.allSFFs[p_at_sff_id]))
                
                path_to_other_sff = SFF.get_multi_hop_path_for(self.sim, p_at_sff_id, target_sff_id)
                for p in path_to_other_sff:
                    scheduled_path.append((SFF.__name__, sff_props.allSFFs[p]))
                
                p_at_sff_id = target_sff_id
            
            scheduled_path.append((SFI.__name__, sfi_props.all_sfi[target_sfi_id]))
            
            if self.incremental:
                scheduled_path.append((SFF.__name__, sff_props.allSFFs[target_sff_id]))
                break
        
        if self.sim.DEBUG:
            Packet.debug_print_path(scheduled_path)
        
        packet.fullPath += scheduled_path
        self.scheduling_attempts += 1
        
        if packet.id == self.sim.PACKET_ID_TO_DEBUG:
            print("** debug packet visited the scheduler, current status:")
            Packet.debug_packet(packet)
        
        packet.realTimeScheduling += self.get_time_delta_of_scheduling()
        self.mySFF.handle_packet_from_scheduler(packet)


class MppScheduler(BaseScheduler):
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            self.allow_up_to_x_packets_underway_per_server: int = None
            self.do_sanity_checks: bool = False
            self.map_server_to_classes: dict = None
            self.r_matrix = None
            self.batch_scheduling = None
            self.blocked_sfi: Set[SFI] = set()
            self.packet_underway_counter_per_server: Dict[Server, int] = None
    
    def __init__(self,
                 sim: Sim,
                 incremental: bool = True, oracle=True,
                 block_sfi_while_packet_on_wire: bool = False,
                 consider_alpha_by_using_timeouts: bool = True,
                 allow_up_to_x_packets_underway_per_server: int = 1,
                 admission_control_threshold_low: float = 0.1,
                 admission_control_threshold_high: float = 1.3,
                 batch_scheduling: int = 1):
        super().__init__(sim=sim,
                         incremental=incremental,
                         oracle=oracle,
                         activate_acp=not oracle,
                         admission_control_threshold_high=admission_control_threshold_high,
                         admission_control_threshold_low=admission_control_threshold_low)
        
        self.block_sfi_while_packet_on_wire = block_sfi_while_packet_on_wire
        self.consider_alpha_by_using_timeouts = consider_alpha_by_using_timeouts
        self.free_server_count = -1  # important that we init with -1
        self.sim.props.mpp_scheduler.allow_up_to_x_packets_underway_per_server = allow_up_to_x_packets_underway_per_server
        
        self.assert_on_reject = False
        
        self.accessible_sf = None
        
        if self.sim.props.mpp_scheduler.batch_scheduling is None:
            self.sim.props.mpp_scheduler.batch_scheduling = batch_scheduling
        else:
            assert self.sim.props.mpp_scheduler.batch_scheduling == batch_scheduling
        
        if allow_up_to_x_packets_underway_per_server < batch_scheduling:
            raise NameError(f"Invalid configuration: batch_scheduling ({batch_scheduling}) > "
                            f"allow_underway_per_server ({allow_up_to_x_packets_underway_per_server})")
        
        if allow_up_to_x_packets_underway_per_server < 1:
            raise NameError("Invalid configuration, at least 1 packets needs to be underway")
        
        if not self.incremental:
            raise NameError("%s Scheduler does support incremental scheduling")
    
    def supports_cpu_policy(self, cpu_policy: ServerCpuPolicy):
        if cpu_policy == ServerCpuPolicy.one_at_a_time:
            return True
        return False
    
    def is_always_able_to_build_full_path(self):
        return False
    
    def requires_queues_per_class(self):
        return True
    
    def get_load_of_sfis_of_sf(self, sf: int):
        return self.get_arrival_rate_estimate(sf) / self.mySFF.service_rate_per_sf[sf]
    
    def cache_map_server_to_classes(self):
        print(".. cache map server->classes")
        sfi_props: SFI.Props = self.sim.props.sfi
        flow_props: Flow.Props = self.sim.props.flow
        mpp_sched_props: MppScheduler.Props = self.sim.props.mpp_scheduler
        
        assert (mpp_sched_props.map_server_to_classes is None)
        mpp_sched_props.map_server_to_classes = dict()
        if self.sim.DEBUG:
            print("MppScheduler creates map server->classes")
        mpp_sched_props.packet_underway_counter_per_server = {s: 0 for s in self.sim.props.server.all_servers}
        for sfi_id in sfi_props.all_sfi:
            sfi_of_class = sfi_props.all_sfi[sfi_id]
            server = sfi_of_class.server
            if server not in mpp_sched_props.map_server_to_classes:
                affected_classes = []
                # find all sfis that are running on this server
                sf_types = set()
                for sfi in server.SFIs:
                    sf_types.add(sfi.of_type)
                # find all affected traffic classes
                for sfc_class in flow_props.sfc_class_to_sf:
                    sf = flow_props.sfc_class_to_sf[sfc_class][0]
                    if sf in sf_types:
                        affected_classes.append(sfc_class)
                
                mpp_sched_props.map_server_to_classes[server] = affected_classes
        # check if we can reach all queues
        reachable = set()
        for server in mpp_sched_props.map_server_to_classes:
            for queue in mpp_sched_props.map_server_to_classes[server]:
                reachable.add(queue)
        if len(reachable) != flow_props.sfc_next_free_class:
            missing = set(range(flow_props.sfc_next_free_class)).difference(reachable)
            # assert(len(missing)> 0)
            for m in missing:
                missing_sf = flow_props.sfc_class_to_sf[m][0]
                for sfi_id in sfi_props.all_sfi:
                    if sfi_props.all_sfi[sfi_id].of_type == missing_sf:
                        raise NameError(
                            f"I cannot serve all possible queues, but this is strange because there is a sfi")
                if self.assert_on_reject:
                    raise NameError(
                        f"I cannot serve all possible queues, because there is no SFI of type: {missing_sf}, {m}, {flow_props.sfc_class_to_sf[m]}")
            
            if self.assert_on_reject:
                raise NameError(
                    f"I cannot serve all possible queues. Missing: {missing} of {flow_props.sfc_classes} having "
                    f"{flow_props.sfc_class_to_sf}")
        print("... [DONE]")
    
    def is_a_valid_activity(self, activity_id):
        properties = self.get_properties_of_activity(activity_id)
        sf, _ = self.sim.props.flow.sfc_class_to_sf[properties['queue']]
        return sf == self.sim.props.sfi.all_sfi[properties["sfi_id"]].of_type
    
    def get_properties_of_activity(self, activity_id):
        tmp_1 = len(self.sim.props.sff.allSFFs) * self.sim.props.flow.sfc_next_free_class
        sfi_id = int(activity_id / tmp_1)
        remainder = activity_id - tmp_1 * sfi_id
        queue = int(remainder / len(self.sim.props.sff.allSFFs))
        remainder -= queue * len(self.sim.props.sff.allSFFs)
        sff_id = remainder
        return {'sfi_id': sfi_id, 'queue': queue, 'sff_id': sff_id}
    
    def get_activity_for(self, sfi_id: int, queue: int, sff_id_of_packet_class: int) -> int:
        return len(self.sim.props.sff.allSFFs) * self.sim.props.flow.sfc_next_free_class * sfi_id + \
               len(self.sim.props.sff.allSFFs) * queue + \
               sff_id_of_packet_class
    
    def cache_r_matrix(self):
        print(".. cache r matrix")
        sfi_props: SFI.Props = self.sim.props.sfi
        sff_props: SFF.Props = self.sim.props.sff
        flow_props: Flow.Props = self.sim.props.flow
        mpp_sched_props: MppScheduler.Props = self.sim.props.mpp_scheduler
        
        assert (mpp_sched_props.r_matrix is None)
        # how many activities do we have? max(SFI.id) * len(buffers) * len(sff)
        total_activities = self.sim.props.sfi.next_free_id * flow_props.sfc_next_free_class * len(sff_props.allSFFs)
        
        mpp_sched_props.r_matrix = {key: 0 for key in range(total_activities)}
        
        if self.sim.DEBUG:
            print("MppScheduler creates r_matrix")
        
        alpha_normalized_enumerator = math.pow(flow_props.max_deadline, 2)
        
        for sff_id in sff_props.allSFFs:
            sff = sff_props.allSFFs[sff_id]
            for queue in flow_props.sfc_class_to_sf:
                
                # get alpha for this queue, and set r values accordingly
                alpha_denominator = math.pow(flow_props.sfc_class_to_deadline[queue], 2)
                alpha = alpha_normalized_enumerator / alpha_denominator
                
                if not self.consider_alpha_by_using_timeouts:
                    alpha = 1
                
                assert (1 <= alpha)
                
                for sfi_id in sfi_props.all_sfi:
                    sfi = sfi_props.all_sfi[sfi_id]
                    if sfi.server.cpu_policy != ServerCpuPolicy.one_at_a_time:
                        raise NameError("this scheduler does not support other cpu sharing methods than one at a time."
                                        " (we consider R as static)")
                    activity = self.get_activity_for(sfi_id=sfi.id, sff_id_of_packet_class=sff.id, queue=queue)
                    sf, is_last_of_sfc = flow_props.sfc_class_to_sf[queue]
                    if sf == sfi.of_type:
                        # calculate the rate of the activity
                        delay = sfi.get_expected_processing_time()
                        if sfi.sffId != sff.id:
                            delay += sff.get_multi_hop_latency_for(self.sim, source_id=sff.id, dest_id=sfi.sffId)
                        
                        mpp_sched_props.r_matrix[activity] = alpha * 1000000.0 / delay
                        assert (mpp_sched_props.r_matrix[activity] > 0)
                    else:
                        # set to 0, so we dont have to change something
                        pass
        
        if self.sim.DEBUG:
            print(".. with a total of {0} entries".format(len(mpp_sched_props.r_matrix)))
        print("... [DONE]")
    
    def notify_sfi_finished_processing_of_packet(self, sfi: SFI, packet: Packet):
        mpp_sched_props: MppScheduler.Props = self.sim.props.mpp_scheduler
        
        if self.sim.DEBUG:
            print(f"MppScheduler gets notified that sfi {sfi} is finished of packet {packet}, "
                  f"so remove server {sfi.server} from the blocked list")
        
        if 'mpp_locking' in packet.scheduler_flag and packet.scheduler_flag['mpp_locking'] is True:
            mpp_sched_props.packet_underway_counter_per_server[sfi.server] -= 1
            packet.scheduler_flag['mpp_locking'] = False
        
        # check if this was the last event, if so, we do not queue at the sfi,
        # if not, we might queue at the sfi
        if mpp_sched_props.packet_underway_counter_per_server[sfi.server] == 0:
            # since we run always in one at a time policy and we do not queue packets at the SFI, the server HAS TO
            # BE FREE if we are finished with a sfi
            assert (sfi.server.is_free())
        
        if self.block_sfi_while_packet_on_wire:
            assert sfi.free
            mpp_sched_props.blocked_sfi.remove(sfi)
        
        if (mpp_sched_props.allow_up_to_x_packets_underway_per_server -
                mpp_sched_props.packet_underway_counter_per_server[sfi.server] >=
                mpp_sched_props.batch_scheduling):
            # inform all affected schedulers
            sff_props: SFF.Props = self.sim.props.sff
            sff_to_be_informed = sorted(sfi.server.SFF_ids)
            self.sim.random.shuffle(sff_to_be_informed)
            for sff_id in sff_to_be_informed:
                # schedule a scheduling event which terminates now this is required,
                # because we should do scheduling after
                # the callback of this function (control flow of the packet needs to be finished first)
                sff_props.allSFFs[sff_id].scheduler.free_server_count += 1
                self.sim.schedule_event(DoSchedulingEvent(delay=0, scheduler=sff_props.allSFFs[sff_id].scheduler))
    
    def notify_packet_was_dropped(self, packet, caller):
        # we have to get the affected server, so that we release the lock
        sfi = None
        if caller is not None:
            if caller.__class__.__name__ == SFF.__name__:
                # get the sfi from the packet's path
                for path_cls, path_instance in packet.fullPath[packet.pathPosition:]:
                    if path_cls == SFI.__name__:
                        # we found the next sfi, this is the guy we are intrested in
                        sfi = path_instance
                        break
            elif caller.__class__.__name__ == SFI.__name__:
                sfi = caller
            else:
                raise NameError(f"packet was dropped, but caller is neither SFI nor SFF, caller={caller}")
        
        if sfi is not None:
            self.notify_sfi_finished_processing_of_packet(sfi, packet)
        else:
            # nothing found, this is a race condition
            raise NameError("I was not able to find a scheduled SFI in the packet's path, but there should be at least "
                            "one next SFI!")
    
    def apply_scheduling_logic_for_packet(self, packet: Packet):
        ## check if next hop matches available SFIs
        expected_sf = packet.toBeVisited[0]
        
        if self.accessible_sf is None:
            self.accessible_sf = set()
            if self.oracle:
                sfi_props: SFI.Props = self.sim.props.sfi
                for sfi_id in sfi_props.all_sfi:
                    sfi = sfi_props.all_sfi[sfi_id]
                    self.accessible_sf.add(sfi.of_type)
            else:
                for sf in self.mySFF.service_rate_per_sf:
                    if (self.mySFF.service_rate_per_sf[sf] > 0):
                        self.accessible_sf.add(sf)
        
        if (expected_sf not in self.accessible_sf):
            queue = Flow.get_packet_class_of_packet(packet)
            
            popped_packet = self.mySFF.packet_queue_per_class[queue].pop()
            assert (packet == popped_packet)
            packet.reject()
        else:
            self.trigger_scheduling_logic()
    
    def trigger_scheduling_logic(self):
        sfi_props: SFI.Props = self.sim.props.sfi
        sff_props: SFF.Props = self.sim.props.sff
        flow_props: Flow.Props = self.sim.props.flow
        mpp_sched_props: MppScheduler.Props = self.sim.props.mpp_scheduler
        
        # check cached data structure
        if mpp_sched_props.map_server_to_classes is None:
            self.cache_map_server_to_classes()
        
        if mpp_sched_props.r_matrix is None:
            self.cache_r_matrix()
        
        if self.free_server_count == 0:
            return False
        
        if self.sim.DEBUG:
            print("Mpp Scheduler starts scheduling")
        
        if mpp_sched_props.do_sanity_checks:
            packets_queued_before = sum(
                [len(x) for sff in sff_props.allSFFs.values() for x in sff.packet_queue_per_class.values()])
        
        successfully_scheduled = 0
        take_the_next_decision = True
        # we repeat the scheduler, as long as we found a scheduling decision in the previous loop
        # if we are in oracle case, we check all servers, if non oracle, then only the servers of my sff
        servers_to_process_packet: List[Server] = list(
            mpp_sched_props.map_server_to_classes.keys()) if self.oracle else self.mySFF.servers
        
        # now select all sffs, from which we have to check the buffers to take a packet from the queue
        # if oracle, we check all SFF, if non oracle, we check only my own sff
        source_sff_to_check: List[int] = list(sff_props.allSFFs.keys()) if self.oracle else [self.mySFF.id]
        pop_sff: deque[int] = deque()
        
        while take_the_next_decision:
            # now filter this list if we block server on which we already scheduled a packet from somewhere.
            # this is only important when in oracle scheduling case
            servers_to_process_packet = [s for s in servers_to_process_packet
                                         if mpp_sched_props.allow_up_to_x_packets_underway_per_server -
                                         mpp_sched_props.packet_underway_counter_per_server[s] >=
                                         mpp_sched_props.batch_scheduling]
            
            self.free_server_count = len(servers_to_process_packet)
            if self.free_server_count == 0:
                break
            
            # build a list of the best activities I found
            # this map is Server from which I take a packet -> (p_value, activity)
            best_activity_among_all_servers: Dict[Server, Tuple[int, int]] = dict()
            
            # we now measure the real time of scheduling, just for statistics reasons
            self.mark_time_scheduling_starts()
            for server_to_process_sfi in servers_to_process_packet:
                if self.sim.DEBUG:
                    print(".. found a free server {0}".format(server_to_process_sfi))
                
                # activity -> p_value
                p_values: Dict[int, int] = dict()
                
                # this list holds all classes, which could be served by this server (by any of its SFIs)
                affected_classes_of_server: List[int] = mpp_sched_props.map_server_to_classes[server_to_process_sfi]
                
                # now we select each possible SFF from which we could take a packet
                while len(pop_sff) > 0:
                    source_sff_to_check.remove(pop_sff.popleft())
                
                for sff_source_id in source_sff_to_check:
                    sff_source = sff_props.allSFFs[sff_source_id]
                    # and check for this sff each queue,
                    # corresponding to any of the classes that are of interest for this server
                    sff_has_nothing = True
                    for queue in sff_source.packet_queue_per_class:
                        # now we check if there is a packet in this queue
                        
                        while len(sff_source.packet_queue_per_class[queue]) > 0:
                            p = sff_source.packet_queue_per_class[queue][0]
                            if p.flow.qosMaxDelay < self.sim.currentTime - p.time_ingress:
                                # drop this packet
                                packet = sff_source.packet_queue_per_class[queue].popleft()
                                packet.timeQueueScheduling += packet.get_delta_of_time_mark()
                                packet.drop_timed_out(self)
                            else:
                                break
                        
                        if len(sff_source.packet_queue_per_class[queue]) > 0:
                            sff_has_nothing = False
                            if queue in affected_classes_of_server:
                                # yes, so get the SF of this class
                                type_of_class, last_sf_of_sfc = flow_props.sfc_class_to_sf[queue]
                                # select now a corresponding SFI of this server
                                sfi_target: SFI
                                for sfi_target in server_to_process_sfi.SFIs:
                                    # and check if this sfi is of the requested sf type for this class/queue
                                    # and check if (in case of non oracle mode) we are allowed to use this sfi
                                    if sfi_target.of_type == type_of_class and (self.oracle or
                                                                                sfi_target.sffId == sff_source_id):
                                        
                                        # do we block this sfi?
                                        if self.block_sfi_while_packet_on_wire:
                                            if sfi_target in mpp_sched_props.blocked_sfi:
                                                continue
                                        
                                        # no, so get the activity id of this action
                                        activity = self.get_activity_for(sfi_id=sfi_target.id, queue=queue,
                                                                         sff_id_of_packet_class=sff_source.id)
                                        
                                        # get the p value
                                        p_value = self.get_p_value_for(activity=activity)
                                        
                                        if self.sim.DEBUG:
                                            print(f'.... found a possible activity {activity} with p:{p_value}, '
                                                  f'doing {self.get_properties_of_activity(activity)}, with sfi of type '
                                                  f'{sfi_props.all_sfi[self.get_properties_of_activity(activity)["sfi_id"]].of_type} '
                                                  f'for sfc identifier '
                                                  f'{Flow.debug_get_sfc_identifier_and_pos_of_packet_class(self.sim, queue)}')
                                        
                                        # add this as an possible option
                                        p_values[activity] = p_value
                    if sff_has_nothing:
                        pop_sff.append(sff_source_id)
                
                # now we select for this server (from which we could take a packet),
                # the best activity among all possible
                best = self.select_best_activity(p_values)
                if best is not None:
                    # we found any, so lets ensure that we will run the whole scheduling a 2nd time,
                    # just if in this scheduling round we will
                    # select a different server from which we will take a packet
                    take_the_next_decision = True
                    best_activity_among_all_servers[server_to_process_sfi] = (p_values[best], best)
            
            if len(best_activity_among_all_servers) == 0:
                if self.sim.DEBUG:
                    print("... there is no possible activity I could schedule")
                self.reset_timer()
                take_the_next_decision = False
            else:
                # print(best_activity_among_all_servers)
                best_p = None
                best_server = None
                for server_to_process_sfi in best_activity_among_all_servers:
                    if best_p is None or best_activity_among_all_servers[server_to_process_sfi][0] > best_p:
                        best_server = server_to_process_sfi
                        best_p = best_activity_among_all_servers[server_to_process_sfi][0]
                
                best = best_activity_among_all_servers[best_server][1]
                server_to_process_sfi = best_server
                # print(f"select {best_server} with activity {best}")
                
                # schedule a packet from this queue and proceed with the next server
                activity_id = best
                properties = self.get_properties_of_activity(activity_id)
                target_sfi: SFI = sfi_props.all_sfi[properties['sfi_id']]
                target_sff: SFF = sff_props.allSFFs[target_sfi.sffId]
                from_sff: SFF = sff_props.allSFFs[properties['sff_id']]
                from_queue = properties['queue']
                
                assert target_sfi.server == server_to_process_sfi
                
                assert mpp_sched_props.packet_underway_counter_per_server[target_sfi.server] < \
                       mpp_sched_props.allow_up_to_x_packets_underway_per_server
                
                if self.block_sfi_while_packet_on_wire:
                    mpp_sched_props.blocked_sfi.add(target_sfi)
                
                scheduled_path = []
                
                # do we have to go to another SFF?
                if target_sff != from_sff:
                    assert self.oracle
                    path = SFF.get_multi_hop_path_for(self.sim, from_sff.id, target_sff.id)
                    for next_sff in path:
                        scheduled_path.append((SFF.__name__, sff_props.allSFFs[next_sff]))
                
                # push to the SFI
                scheduled_path.append((SFI.__name__, target_sfi))
                
                # push back to the SFF
                scheduled_path.append((SFF.__name__, target_sff))
                
                # how many packet shall we send on this path?
                packet_count = min(mpp_sched_props.batch_scheduling,
                                   mpp_sched_props.allow_up_to_x_packets_underway_per_server -
                                   mpp_sched_props.packet_underway_counter_per_server[target_sfi.server])
                time_delta_of_scheduling = self.get_time_delta_of_scheduling()
                
                self.scheduling_attempts += 1
                
                while packet_count > 0 and len(from_sff.packet_queue_per_class[from_queue]) > 0:
                    # pop a packet from queue and attach path
                    packet = from_sff.packet_queue_per_class[from_queue].popleft()
                    
                    packet.realTimeScheduling += time_delta_of_scheduling
                    packet.timeQueueScheduling += packet.get_delta_of_time_mark()
                    
                    time_left = packet.flow.qosMaxDelay - (self.sim.currentTime - packet.time_ingress)
                    
                    min_time = (self.mySFF.get_multi_hop_latency_for(self.sim,
                                                                       self.mySFF.id, target_sff.id)
                                + self.mySFF.get_multi_hop_latency_for(self.sim,
                                                                       target_sff.id, packet.flow.desiredEgressSSFid))

                    if time_left < min_time:
                        if self.sim.DEBUG:
                            print(". drop packet because of timeout inside scheduling queue")
                        
                        packet.drop_timed_out(self)
                        continue
                    
                    packet_count -= 1
                    
                    packet.set_callback_when_dropped(lambda p, caller: self.notify_packet_was_dropped(p, caller))
                    
                    mpp_sched_props.packet_underway_counter_per_server[target_sfi.server] += 1
                    packet.scheduler_flag['mpp_locking'] = True
                    
                    
                    expected_sf = packet.toBeVisited.pop(0)
                    if expected_sf != target_sfi.of_type:
                        raise NameError("the scheduler messed up something!")
                    
                    packet.fullPath += scheduled_path
                    
                    if packet.id == self.sim.PACKET_ID_TO_DEBUG:
                        print(
                            f"** packet was scheduled from sff {from_sff} queue {from_queue} "
                            f"to sff {target_sff} on sfi {target_sfi} running on server {target_sfi.server} ")
                    
                    if self.sim.DEBUG:
                        print(f"... select activity {best} with p {best_p} from sff {from_sff} "
                              f"queue {from_queue} to sff {target_sff} on sfi {target_sfi} ")
                        Packet.debug_packet(packet)
                    
                    successfully_scheduled += 1
                    from_sff.handle_packet_from_scheduler(packet)
        
        # do sanity check?
        if mpp_sched_props.do_sanity_checks:
            if successfully_scheduled == 0 and not self.oracle:
                
                # check if non of my sfis is free, and if so, then there should be no packet in any of the queues
                for sf in self.mySFF.SFIsPerType:
                    waiting_queues_for_sf = []
                    for queue in self.mySFF.packet_queue_per_class:
                        if 0 < len(self.mySFF.packet_queue_per_class[queue]):
                            # there are packets in this queue, check the sf type of this queue
                            sfc, pos = Flow.debug_get_sfc_identifier_and_pos_of_packet_class(self.sim, queue)
                            flow_sf, eos = Flow.get_sf_and_eoc_of(self.sim, sfc, pos)
                            if flow_sf == sf:
                                waiting_queues_for_sf.append(queue)
                    
                    # if there is no queue waiting, we don't have to check the sfis
                    if 0 == len(waiting_queues_for_sf):
                        continue
                    
                    for sfi in self.mySFF.SFIsPerType[sf]:
                        if sfi.free and sfi.server.is_free():
                            raise NameError(f'there is a free sfi {sfi} of a sff {self.mySFF}, '
                                            f'which could serve a packet from a queue ('
                                            f'{[(queue, Flow.debug_get_sfc_identifier_and_pos_of_packet_class(self.sim, queue)) for queue in waiting_queues_for_sf]}).')
        
        return successfully_scheduled > 0
    
    def get_p_value_for(self, activity: int):
        sfi_props: SFI.Props = self.sim.props.sfi
        sff_props: SFF.Props = self.sim.props.sff
        flow_props: Flow.Props = self.sim.props.flow
        mpp_sched_props: MppScheduler.Props = self.sim.props.mpp_scheduler
        
        properties = self.get_properties_of_activity(activity)
        source_sff = sff_props.allSFFs[properties["sff_id"]]
        target_sfi = sfi_props.all_sfi[properties["sfi_id"]]
        target_sff = sff_props.allSFFs[target_sfi.sffId]
        queue = properties["queue"]
        
        expected_sf, end_of_sfc = flow_props.sfc_class_to_sf[queue]
        
        assert expected_sf == target_sfi.of_type
        assert queue in source_sff.packet_queue_per_class
        
        # here, we subtract the length of the "on the way packets"
        # rate of this activity times the queue length at source sff
        
        subtract_sfi_queue = 0
        
        p_value = mpp_sched_props.r_matrix[activity] * (
                len(source_sff.packet_queue_per_class[queue]) -
                mpp_sched_props.packet_underway_counter_per_server[target_sfi.server] -
                subtract_sfi_queue)
        
        # backpressure?
        if not end_of_sfc:
            next_queue = queue + 1
            if next_queue in target_sff.packet_queue_per_class:
                p_value -= mpp_sched_props.r_matrix[activity] * len(target_sff.packet_queue_per_class[next_queue])
        
        return p_value
    
    def select_best_activity(self, p_values: Dict[int, float]):
        sff_props: SFF.Props = self.sim.props.sff
        
        # find the best activity
        best: Tuple[float, int, int] = (None, None, None)  # p_val, activity, time_when_packet_entered_sff
        for activity, p_value in p_values.items():
            # we select the max value, or
            #   if values are equally, we take the activity which has the older packet in the queue,
            #   but packet time with respect to time when packet entered sff for queueing
            #       if also the time is equal, then we select the activity with the largest id
            properties = self.get_properties_of_activity(activity)
            time_of_packet = sff_props.allSFFs[properties['sff_id']].packet_queue_per_class[properties['queue']][
                0].timeMarker
            if best[0] is None or best[0] < p_value or (
                    best[0] == p_value and (best[2] > time_of_packet or (
                    best[2] == time_of_packet and best[1] < activity))):
                # get time of packet
                best = (p_value, activity, time_of_packet)
        return best[1]
    
    def debug_print_r_matrix(self):
        sfi_props: SFI.Props = self.sim.props.sfi
        sff_props: SFF.Props = self.sim.props.sff
        flow_props: Flow.Props = self.sim.props.flow
        mpp_sched_props: MppScheduler.Props = self.sim.props.mpp_scheduler
        
        for random_sff in sff_props.allSFFs.values():
            if isinstance(random_sff.scheduler, MppScheduler):
                random_sff.scheduler.cache_r_matrix()
                break
        
        for queue in range(len(flow_props.sfc_class_to_sf)):
            sf = flow_props.sfc_class_to_sf[queue][0]
            print(f" packet class {queue},  which refers to SF {sf}")
            
            options = dict()
            
            for dest_sff in sff_props.allSFFs.values():
                for sfi in sfi_props.all_sfi.values():
                    if sfi.of_type == sf and sfi.sffId == dest_sff.id:
                        # I found a sfi at the dest sff which could serve this queue
                        # now loop through all source SFFs from which I could take a packet
                        for source_sff in sff_props.allSFFs.values():
                            activity = source_sff.scheduler.get_activity_for(sfi_id=sfi.id,
                                                                             sff_id_of_packet_class=source_sff.id,
                                                                             queue=queue)
                            assert source_sff.scheduler.is_a_valid_activity(activity)
                            if source_sff not in options:
                                options[source_sff] = []
                            options[source_sff].append(activity)
            
            for source_sff in options:
                print(f"\t taking a packet from SFF {source_sff.id}")
                print(f"\t\t {[round(mpp_sched_props.r_matrix[x], 1) for x in options[source_sff]]}")
