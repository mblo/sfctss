#!/usr/bin/env python3
# coding=utf-8
from typing import Set

from .sfi import *
from ..events import BaseEvent
from ..simulator import Sim


class ServerCpuShareEvent(BaseEvent):
    def __init__(self, interval, server):
        super().__init__(server.sim.currentTime + interval)
        self.ignoreWhenFinished = True
        self.server = server
        self.interval = interval
    
    def process_event(self):
        # reschedule next update
        self.server.sim.schedule_event(ServerCpuShareEvent(
            interval=self.interval, server=self.server))
        # trigger cpu share update
        self.server.update_dynamic_cpu_weights()


class Server(object):
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            self.lastId: int = 0
            self.all_servers: List['Server'] = []
    
    def __init__(self, sim: Sim, processing_cap, cpu_policy: ServerCpuPolicy):
        self.SFIs = []
        self.sim = sim
        self.SFF_ids: Set[int] = set()
        sim.props.server.lastId += 1
        self.id = sim.props.server.lastId
        self.processing_cap = int(processing_cap)
        self.availableShares = self.processing_cap
        self.sfiWeights = {}
        self.cpu_policy = cpu_policy
        
        self.stats_idle_time = 0
        self.stats_last_time_idle = 0
        
        if self.cpu_policy == ServerCpuPolicy.dynamic:
            sim.schedule_event(ServerCpuShareEvent(
                interval=sim.SERVER_CPU_POLICY_DYNAMIC_INTERVAL, server=self))
        
        sim.props.server.all_servers.append(self)
    
    def __repr__(self):
        return f"Server({str(self.id)}/{'f' if self.is_free() else 'b'})"
    
    # checks if all hosted sfis are in idle mode
    def is_free(self):
        for sfi in self.SFIs:
            if not sfi.free:
                return False
        
        return True
    
    # tests whether the asking sfi is allowed to go in processing mode
    # if so, we update the cpu shares, update all affected sfis and return true
    def ask_for_processing(self, asking_sfi: SFI):
        is_free = self.is_free()
        
        if self.sim.DEBUG:
            print(
                f"server {self.id} is asked for giving processing token to a sfi {asking_sfi.id}. My answer: {is_free}")
        
        if is_free:
            self.stats_idle_time += self.sim.currentTime - self.stats_last_time_idle
            self.stats_last_time_idle = self.sim.currentTime
        
        if self.cpu_policy != ServerCpuPolicy.one_at_a_time:
            return True
        else:
            if is_free:
                for sfi in self.sfiWeights:
                    if asking_sfi == sfi:
                        self.sfiWeights[sfi] = self.sim.SERVER_CPU_SHARE_GRANULARITY
                    else:
                        self.sfiWeights[sfi] = 0
                
                self.notify_sfi_to_recalculate_shares()
                # we have to inform the asking sfi two times,
                # since it might happen that in the previous call this sfi
                # was triggered before a different sfi freed his shares
                asking_sfi.refresh_server_shares()
                return True
            else:
                return False
    
    def sfi_finishes_processing(self, sfi: SFI):
        self.stats_last_time_idle = self.sim.currentTime
        # if we are in cpu policy one at a time, we check if we have to inform some other sfi to start processing
        
        if self.cpu_policy == ServerCpuPolicy.one_at_a_time:
            if self.is_free():
                shuffled_sfi = [i for i in self.SFIs]
                self.sim.random.shuffle(shuffled_sfi)
                for sfi in shuffled_sfi:
                    if len(sfi.queue) > 0:
                        sfi.notify_for_processing()
                        return
    
    def notify_sfi_to_recalculate_shares(self):
        for sfi in self.SFIs:
            sfi.refresh_server_shares()
    
    def update_dynamic_cpu_weights(self):
        if len(self.SFIs) == 0:
            if self.sim.DEBUG:
                print("WARN - server {0} has no SFIs".format(self))
            return
        # get the total queue length among all SFIs
        total_queue_length = 0
        for sfi in self.SFIs:
            total_queue_length += len(sfi.queue)
        denominator = total_queue_length + len(self.SFIs)
        # we have to make sure, that each sfiWeights is at least as big enough,
        # so that each SFI gets at least one CPU share (prevent starvation!)
        weight_for_one_share = int(
            self.sim.SERVER_CPU_SHARE_GRANULARITY / self.processing_cap) + 1
        weights_free_to_assign = self.sim.SERVER_CPU_SHARE_GRANULARITY - \
                                 weight_for_one_share * len(self.SFIs)
        
        weight_remainder = self.sim.SERVER_CPU_SHARE_GRANULARITY
        for sfi in self.sfiWeights:
            enumerator = (len(sfi.queue) + 1)
            self.sfiWeights[sfi] = weight_for_one_share + \
                                   int(weights_free_to_assign * enumerator / denominator)
            weight_remainder -= self.sfiWeights[sfi]
            if self.sim.DEBUG:
                print(" * {0} has an enume of {1} and gets weight {2}".format(
                    sfi, str(enumerator), str(self.sfiWeights[sfi])))
        while weight_remainder > 0:
            for sfi in self.SFIs:
                if weight_remainder > 0:
                    self.sfiWeights[sfi] += 1
                    weight_remainder -= 1
        
        self.notify_sfi_to_recalculate_shares()
    
    def add_sfi(self, of_type, with_sff_id):
        self.SFF_ids.add(with_sff_id)
        sfi = SFI(of_type, self, with_sff_id)
        
        self.SFIs.append(sfi)
        if len(self.SFIs) > self.processing_cap and self.cpu_policy == ServerCpuPolicy.static:
            raise NameError("more SFIs running ({0}) on this server ".format(str(len(
                self.SFIs))) + "than cpuShares ({0}) available".format(str(self.processing_cap)))
        
        self.sfiWeights[sfi] = 0
        if self.cpu_policy == ServerCpuPolicy.static:
            eq = int(self.sim.SERVER_CPU_SHARE_GRANULARITY / len(self.sfiWeights))
            if self.sim.DEBUG:
                print(
                    "{0} splits his shares ({1}) according to the static policy, each weight is {2}".format(
                        self, str(
                            self.processing_cap), str(eq)))
            assert (eq > 0)
            for sfi in self.sfiWeights:
                self.sfiWeights[sfi] = eq
                # in case of static policy, we are allowed to reset all shares,
                # at this point in time, because no SFI runs a packet currently
                sfi.free_all_server_shares()
            self.notify_sfi_to_recalculate_shares()
        elif self.cpu_policy == ServerCpuPolicy.dynamic:
            self.update_dynamic_cpu_weights()
