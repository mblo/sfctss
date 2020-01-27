# coding=utf-8

from .core import *


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
