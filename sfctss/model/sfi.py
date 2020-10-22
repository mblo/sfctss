#!/usr/bin/env python3
# coding=utf-8

from .sff import *
from ..events import PacketHoldingEvent
from ..simulator import Sim


class SfiProcessEvent(PacketHoldingEvent):
    def __init__(self, processing_time, inner_packet: Packet, sfi: 'SFI'):
        super().__init__(inner_packet.flow.sim.currentTime + processing_time, inner_packet)
        self.sfi = sfi
    
    def update_packet_time_tracking(self):
        self.inner_packet.timeProcessing += self.inner_packet.get_delta_of_time_mark()
    
    def process_event(self):
        self.update_packet_time_tracking()
        self.sfi.finished_processing(self.inner_packet)


class SFI(object):
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            self.all_sfi: Dict[int, 'SFI'] = dict()
            self.next_free_id = 0
            self.latency_provider = -1
            self.processingRateOfSfType: List[int] = None
    
    # parameters: the SF type (int), the server which hosts this SFI, the SFF
    # which does scheduling of this SFI
    def __init__(self, of_type, hosting_server: 'Server', responsible_sff_id):
        assert isinstance(of_type, int)
        assert isinstance(responsible_sff_id, int)
        self.sim = hosting_server.sim
        sfi_props: SFI.Props = self.sim.props.sfi
        if sfi_props.processingRateOfSfType is None:
            raise NameError("you have to setup sf types")
        assert (len(sfi_props.processingRateOfSfType) > of_type)
        self.of_type = of_type
        self.server: 'Server' = hosting_server
        self.sffId = responsible_sff_id
        self.refreshShares = False
        self.cpuShares = 0
        self.cachedTimeToProcessAPacket = 1
        self.queue: deque = deque()
        self.free = True
        self.id = sfi_props.next_free_id
        sfi_props.all_sfi[self.id] = self
        sfi_props.next_free_id += 1
        
        self.sim.props.sff.allSFFs[responsible_sff_id].register_sfi(self)
    
    @staticmethod
    @Sim.register_stop_sim_catch_all_packets_hooks
    def callback_to_get_all_packets_from_sfi(sim: Sim):
        for sfi in sim.props.sfi.all_sfi.values():
            for p in sfi.queue:
                p.timeQueueProcessing += p.get_delta_of_time_mark()
                yield p
    
    def __repr__(self):
        return f"SFI({self.id}/sf{self.of_type}/{'f' if self.free else 'b'}/{len(self.queue)})"
    
    # setup the internal data structure, so that it supports SF types of numberOfSfTypes,
    # hence the data structure holds for sf types 0...(numberOfSfTypes-1),
    # (len(data) = numberOfSfTypes)
    @staticmethod
    def init_data_structure(sim: Sim, number_of_sf_types=1, latency_provider_sff_sfi=0):
        assert (sim.props.sfi.processingRateOfSfType is None)
        sim.props.sfi.processingRateOfSfType = [0 for _ in range(number_of_sf_types)]
        sim.props.sfi.latency_provider = latency_provider_sff_sfi
    
    @staticmethod
    def setup_sf_processing_rate_per_1s(sim: Sim, of_type, with_mu):
        """we set processing rate as number of packets per 1s,
        so a value of e.g., 22 means, per 1 share of processing from a server,
        a SFI of this type will process 22 packets per 1s
        hence, when a packet arrives and the queue of this SFI is empty,
        the packet will be done after 1000000 / (22 * CPU shares) µs"""
        of_type = int(of_type)
        sfi_props: SFI.Props = sim.props.sfi
        sff_props: SFF.Props = sim.props.sff
        if sfi_props.processingRateOfSfType is None or len(sfi_props.processingRateOfSfType) <= of_type:
            raise (
                NameError(
                    "You have to setup the data structure with the max " +
                    "number of SF types used. Run SFI.init_data_structure"))
        sfi_props.processingRateOfSfType[of_type] = with_mu
        # refresh cachedTimeToProcessAPacket
        for sffID in sff_props.allSFFs:
            if of_type in sff_props.allSFFs[sffID].SFIsPerType:
                for sfi in sff_props.allSFFs[sffID].SFIsPerType[of_type]:
                    sfi.refresh_processing_speed()
    
    # frees up all server shares this SFI has
    def free_all_server_shares(self):
        assert self.free
        self.server.availableShares += self.cpuShares
        self.cpuShares = 0
    
    # when called, this SFI tries to get as many shares from its server,
    # as considered based on the SFIs weight
    # this method updates the shares only, if the SFI is not in processing mode.
    # if this method is called during processing mode, we set a flag for updating
    # the shares when the processing is done.
    # otherwise, we might free resources,
    # which are still in usage. Hence, some shares are used simultaneously
    def refresh_server_shares(self):
        if not self.free:
            self.refreshShares = True
            return
        self.refreshShares = False
        # weight is in the range of [0,1000]
        # we stick to int arithmetic, for performance reasons
        w = self.server.sfiWeights[self]
        shares_target = int(self.server.processing_cap * w /
                            self.sim.SERVER_CPU_SHARE_GRANULARITY)
        # if Sim.DEBUG:
        #     print(
        #         "* targetShares is {3} based on weight {0} processing_cap {1} granularity {2}".format(
        #             str(w), str(
        #                 self.server.processing_cap), str(
        #                 Sim.SERVER_CPU_SHARE_GRANULARITY), str(shares_target)))
        
        if self.server.cpu_policy != ServerCpuPolicy.one_at_a_time:
            assert (shares_target > 0)
        
        # if Sim.DEBUG:
        #     print("{0} has a weight of {1}, holds {2} shares and wants {3} shares".format(
        #         self, str(w), str(self.cpuShares), str(shares_target)))
        if self.cpuShares > shares_target:
            # we have to free some shares
            to_give_free = self.cpuShares - shares_target
            assert to_give_free > 0
            self.server.availableShares += to_give_free
            self.cpuShares = shares_target
        else:
            # we are allowed to take more shares,
            needed_shares = shares_target - self.cpuShares
            if self.server.availableShares >= needed_shares:
                # there are enough free shares, so take them
                self.server.availableShares -= needed_shares
                self.cpuShares = shares_target
            else:
                # some shares are missing, but take as many as possible
                self.cpuShares += self.server.availableShares
                self.server.availableShares = 0
                # reschedule this method to get more shares later
                if self.sim.DEBUG:
                    print("{0} shares missing, i have only {1}".format(
                        self, str(self.cpuShares)))
                self.refreshShares = True
        if self.sim.DEBUG:
            print("{0} holds now {1} shares, server has {2} left".format(
                self, str(self.cpuShares), str(self.server.availableShares)
            ))
        self.refresh_processing_speed()
    
    # returns the expected waiting time when a packet will be scheduled on this sfi
    # however, the returned value might be strongly wrong, depending on the server cpu policy
    def get_expected_waiting_time(self) -> int:
        if self.server.cpu_policy == ServerCpuPolicy.one_at_a_time:
            return len(self.queue) * int(
                1000000 / (self.server.processing_cap * self.sim.props.sfi.processingRateOfSfType[self.of_type]))
        else:
            return self.cachedTimeToProcessAPacket * len(self.queue)
    
    def get_expected_processing_rate(self) -> float:
        if self.server.cpu_policy == ServerCpuPolicy.one_at_a_time:
            return self.server.processing_cap * self.sim.props.sfi.processingRateOfSfType[self.of_type]
        else:
            return 1000000.0 / self.get_expected_processing_time()
    
    def get_expected_processing_time(self) -> int:
        if self.server.cpu_policy == ServerCpuPolicy.one_at_a_time:
            delay = int(1000000 // self.get_expected_processing_rate())
            if delay == 0:
                raise NameError("the speed of this sfi is too high, so that the time for processing is less than 1."
                                "Server capacity: {0}, µ of sfi:{1}".format(self.server.processing_cap,
                                                                            self.sim.props.sfi.processingRateOfSfType[
                                                                                self.of_type]))
            return delay
        else:
            return self.cachedTimeToProcessAPacket
    
    # updates the time required for each packet based on the amount of shares
    # the sfi has
    def refresh_processing_speed(self):
        # if Sim.DEBUG:
        #     print(
        #         "{0} has {1} shares and updates the time required for a packet".format(
        #             self, self.cpuShares))
        
        assert (self.sim.props.sfi.processingRateOfSfType[self.of_type] > 0)
        
        # if cpu policy of the server is one at a time, we might have 0 cpu shares
        if self.server.cpu_policy == ServerCpuPolicy.one_at_a_time:
            if self.cpuShares == 0:
                return
        else:
            # otherwise check that we hat at least some shares
            assert (self.cpuShares > 0)
        
        self.cachedTimeToProcessAPacket = int(
            1000000 / (self.sim.props.sfi.processingRateOfSfType[self.of_type] * self.cpuShares))
        # print(f"sfi processing time for type {self.of_type} is {self.cachedTimeToProcessAPacket}")
    
    # notify the sfi that it is allowed for starting processing queued events
    def notify_for_processing(self):
        assert self.free
        assert len(self.queue) > 0
        assert self.server.ask_for_processing(self)
        self.free = False
        self.internal_schedule_event(self.queue.popleft())
    
    def finished_processing(self, packet: Packet):
        # do we need to refresh our server shares we reserve?
        if self.refreshShares:
            self.free = True
            self.refresh_server_shares()
            self.free = False
        
        # handle next hop of packet
        if self.sim.DEBUG:
            print(f"processing done for packet {packet.id} at sfi {self.id}")
            Packet.debug_print_path(packet.fullPath)
        
        if self.server.cpu_policy == ServerCpuPolicy.one_at_a_time:
            self.free = True
            self.server.sfi_finishes_processing(self)
        else:
            if len(self.queue) > 0:
                self.internal_schedule_event(self.queue.popleft())
            else:
                self.free = True
        
        sff_props: SFF.Props = self.sim.props.sff
        sff_props.allSFFs[self.sffId].sfi_finishes_processing_of_packet(self, packet)
        
        next_hop_type, next_hop = packet.fullPath[packet.pathPosition]
        packet.pathPosition += 1
        # we also have to add 1 to the position with respect to the sfc
        
        # is this the last stop of this sfc?
        if Flow.get_sf_and_eoc_of_packet(packet)[1]:
            packet.processing_done = True
        
        packet.sfc_position += 1
        
        packet.mark_time()
        sfi_props: SFI.Props = self.sim.props.sfi
        sff_props: SFF.Props = self.sim.props.sff
        
        delay = next(sff_props.latencyProvider[sfi_props.latency_provider])
        
        if next_hop_type == SFF.__name__:
            # we send this packet to a SFF
            # this should be the SFF to which this SFI belongs to
            assert (self.sffId == next_hop.id)
            # since this does not take any latency, we simply call handle
            # packet
            if self.sim.PACKET_ID_TO_DEBUG == packet.id:
                print(f'** packet processed at sfi {self.id}, send it back to sff')
            self.sim.schedule_event(NetworkDelayEvent(delay=delay,
                                                      inner_packet=packet,
                                                      source=self,
                                                      dest_id=next_hop.id,
                                                      source_is_sff=False,
                                                      dest_is_sff=True))
        
        elif next_hop_type == SFI.__name__:
            # we send this packet to the next SFI
            # this SFI should have the same SFF as I have, otherwise this is not
            # allowed
            if self.sim.PACKET_ID_TO_DEBUG == packet.id:
                print(f'** packet processed at sfi {self.id}, send it to next sfi {next_hop.id}')
            self.sim.schedule_event(NetworkDelayEvent(delay=delay,
                                                      inner_packet=packet,
                                                      source=self,
                                                      dest_id=next_hop.id,
                                                      source_is_sff=False,
                                                      dest_is_sff=False))
        else:
            NameError("unknown next hop")
    
    def internal_schedule_event(self, packet: Packet):
        assert not self.free
        
        # check if packet is already outdated
        while packet.flow.qosMaxDelay < (self.sim.currentTime - packet.time_ingress + self.cachedTimeToProcessAPacket):
            if self.sim.DEBUG:
                print("drop packet because of timeout")
            
            # add up queue time of this packet
            packet.timeQueueProcessing += packet.get_delta_of_time_mark()
            
            # we drop a packet, so we have to be in free state when calling this
            self.free = True
            packet.drop_timed_out(self)
            # is there any other packet and are we still in free state?
            # in case of one at a time, we have to check whether we are allowed to proceed
            if len(self.queue) > 0 and self.free and \
                    (self.server.cpu_policy != ServerCpuPolicy.one_at_a_time or self.server.ask_for_processing(self)):
                # yes, so set us a busy
                self.free = False
                packet = self.queue.popleft()
            else:
                self.free = True
                # we have to tell the server that we are done
                if self.server.cpu_policy == ServerCpuPolicy.one_at_a_time:
                    self.server.sfi_finishes_processing(self)
                return
        
        if self.sim.DEBUG:
            print(f".. start processing a packet {packet.id} at sfi {self.id} @{self.sim.currentTime}")
            
            if self.server.cpu_policy == ServerCpuPolicy.one_at_a_time:
                # now we are in isolated mode, sanity check
                running_server = set()
                for sfi_id in self.sim.props.sfi.all_sfi:
                    sfi = self.sim.props.sfi.all_sfi[sfi_id]
                    server = sfi.server
                    if not sfi.free:
                        assert server not in running_server
                        running_server.add(server)
                
                assert self.server.availableShares == 0
                assert self.cpuShares == self.server.processing_cap
        
        packet.set_callback_when_dropped(None)
        # add up queue time of this packet
        packet.timeQueueProcessing += packet.get_delta_of_time_mark()
        
        # process packet
        packet.mark_time()
        self.sim.schedule_event(
            SfiProcessEvent(
                processing_time=self.cachedTimeToProcessAPacket,
                inner_packet=packet,
                sfi=self))
    
    def enqueue_packet(self, packet: Packet):
        if self.sim.TRACE_PACKET_PATH:
            packet.visitedHops.append(self)
        
        if packet.id == self.sim.PACKET_ID_TO_DEBUG:
            print(f"** receive debug packet at sfi {self}")
        
        # remember current time so that we track queue time for this packet
        packet.mark_time()
        
        if self.free:
            if self.server.ask_for_processing(self):
                # server is not free, so queue this packet
                
                self.free = False
                self.internal_schedule_event(packet)
            else:
                self.queue.append(packet)
        
        else:
            self.queue.append(packet)
