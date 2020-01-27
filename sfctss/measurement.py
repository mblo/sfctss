#!/usr/bin/env python3
# coding=utf-8
import re
import time
from typing import List

from .model.server import Server
from .model.sff import SFF
from .model.sfi import SFI
from .model.core import Flow, Packet
from .simulator import Sim
from .events import BaseEvent


class StatisticsPollingEvent(BaseEvent):
    def __init__(self, interval, stats_writer: 'SimStatsPoller', first_event=False):
        super().__init__(stats_writer.sim.currentTime + (0 if first_event else interval))
        self.ignoreWhenFinished = True
        self.stats_writer = stats_writer
        self.interval = interval
    
    def process_event(self):
        # reschedule next update
        self.stats_writer.sim.schedule_event(
            StatisticsPollingEvent(interval=self.interval, stats_writer=self.stats_writer))
        self.stats_writer.poll_statistics()


class OverviewStatisticsEvent(BaseEvent):
    
    def __init__(self, list_of_snapshots: List[int], sim: Sim):
        super().__init__(list_of_snapshots.pop(0))
        self.ignoreWhenFinished = True
        self.sim = sim
        self.list_of_snapshots = list_of_snapshots
    
    def process_event(self):
        # reschedule next update
        stat_props: SimStats.Props = self.sim.props.sim_stats
        # if overview statistics is not active, ignore this event
        if stat_props.overviewStats is None:
            return
        
        if len(self.list_of_snapshots) > 0:
            self.sim.schedule_event(
                OverviewStatisticsEvent(list_of_snapshots=self.list_of_snapshots, sim=self.sim))
        SimStats.do_overview_statistics_snapshot(self.sim)


class SimStatsWriter(object):
    
    def __init__(self, sim: Sim, filepath):
        self.filepath = filepath
        self.sim = sim
        self.entries = []
        self.workload_is_over = False
        
        sim.props.sim_stats.allStatsWriter.append(self)
        if sim.DEBUG:
            print("* genStatWriter for {0} called".format(self.filepath))
    
    def init_file(self):
        with open(self.filepath, "w") as f:
            f.write(self.header_row())
    
    def header_row(self):
        raise NameError("must be implemented by subclass")
    
    def flush(self):
        if self.sim.DEBUG:
            print(
                "* flush {0} elements to {1}".format(str(len(self.entries)), self.filepath))
        with open(self.filepath, "a") as f:
            for e in self.entries:
                f.write("\n")
                f.write(e)
        self.entries = []
    
    def inform_workload_is_over(self):
        self.workload_is_over = True


class SimStatsRowWriter(SimStatsWriter):
    
    def __init__(self, sim: Sim, filepath, columns: list, data_types: list):
        super().__init__(sim, filepath)
        self.columns = columns
        self.data_types = data_types
        self.init_file()
    
    def header_row(self):
        return self.sim.props.sim_stats.SEPARATOR.join(self.columns)
    
    def add_entry(self, *vals):
        s = []
        assert (len(vals) == len(self.columns))
        for i, v in enumerate(vals):
            s.append(str(self.data_types[i](v)))
        self.entries.append(self.sim.props.sim_stats.SEPARATOR.join(s))
        if len(self.entries) > self.sim.props.sim_stats.FLUSH_ENTRIES:
            self.flush()


class SimStatsKvWriter(SimStatsWriter):
    
    def __init__(self, sim: Sim, filepath, data_type: type = float):
        super().__init__(sim, filepath)
        self.init_file()
        self.datatype = data_type
    
    def header_row(self):
        return self.sim.props.sim_stats.SEPARATOR.join(["time", "key", "value"])
    
    def add_kv_entry(self, key, value):
        self.entries.append(self.sim.props.sim_stats.SEPARATOR.join([
            str(self.sim.currentTime), str(key), str(self.datatype(value))]))
        if len(self.entries) > self.sim.props.sim_stats.FLUSH_ENTRIES:
            self.flush()


class SimStatsCdfWriter(SimStatsWriter):
    
    def __init__(self, sim: Sim, filepath, buckets: int, per_group: bool):
        super().__init__(sim, filepath)
        self.init_file()
        self.buckets = buckets
        self.bucket_holder = dict()
        self.per_group = per_group
    
    def prepare_flush(self):
        sep = self.sim.props.sim_stats.SEPARATOR
        for key in self.bucket_holder:
            s_key = str(key)
            for group in self.bucket_holder[key]:
                s_group = s_key + sep + str(group)
                for bucket, number in enumerate(self.bucket_holder[key][group]):
                    self.entries.append(sep.join([s_group, str(bucket), str(number)]))
        
        self.bucket_holder = None  # intentionally, we set bucket_holder to None, so that after flushing this guy,
        # there is no reason to 1. flush it again, 2. add some more data points
    
    def header_row(self):
        return self.sim.props.sim_stats.SEPARATOR.join(["key", "group", "bucket", "count"])
    
    def add_data_point(self, key, group, value: float):
        if value > 1 or value < 0:
            raise NameError(f"received invalid value ({value}) for cdf - expect to receive normalized valued [0,1]")
        
        if key not in self.bucket_holder:
            self.bucket_holder[key] = dict()
        
        if not self.per_group:
            group = "all"
        
        if group not in self.bucket_holder[key]:
            self.bucket_holder[key][group] = [0 for _ in range(self.buckets)]
        
        bucket = self.buckets - 1 if value == 1 else int(value * self.buckets)
        self.bucket_holder[key][group][bucket] += 1


class SimStatsPoller(SimStatsRowWriter):
    
    def __init__(self, sim: Sim, filepath, interval):
        super().__init__(sim, filepath, ['time', 'stat',
                                         'key', 'value'], [int, str, str, float])
        self.interval = interval
        self.sim.schedule_event(StatisticsPollingEvent(
            interval=interval, stats_writer=self, first_event=True))
        self.allServers = []
        self.SFIs = dict()
        self.SFFs = []
        self.overview = False
        self.overview_last_value_cache = dict()
    
    def poll_statistics(self):
        flow_props: Flow.Props = self.sim.props.flow
        packet_props: Packet.Props = self.sim.props.packet
        server_props: Server.Props = self.sim.props.server
        sff_props: SFF.Props = self.sim.props.sff
        sfi_props: SFI.Props = self.sim.props.sfi
        
        # simulator events length
        if self.sim.DEBUG:
            self.add_entry(self.sim.currentTime, "simulator", "queue", self.sim.event_list.len())
        
        if self.overview:
            idle_time = 0
            for s in self.sim.props.server.all_servers:
                idle_time += s.stats_idle_time
                if s.is_free():
                    idle_time += self.sim.currentTime - s.stats_last_time_idle
            
            if len(self.overview_last_value_cache) == 0:
                self.overview_last_value_cache["packets_done"] = packet_props.statsPacketsSuccessfulProcessed
                self.overview_last_value_cache["time"] = self.sim.currentTime
                self.overview_last_value_cache["idle_time"] = idle_time
                self.overview_last_value_cache["packet_ingress"] = packet_props.counter_ingress_packets
                self.overview_last_value_cache["packet_rejected"] = packet_props.statsPacketsRejectedSchedule
                self.overview_last_value_cache["packet_timeout"] = packet_props.statsPacketsRejectedProcessingDelay
                self.overview_last_value_cache["packet_successful"] = packet_props.statsPacketsSuccessfulProcessed
            else:
                rates = {
                    "goodput": (packet_props.statsPacketsSuccessfulProcessed - self.overview_last_value_cache[
                        "packets_done"]) /
                               ((self.sim.currentTime - self.overview_last_value_cache["time"]) / 1000000),
                    "packet_rejected_rate": (packet_props.statsPacketsRejectedSchedule - self.overview_last_value_cache[
                        "packet_rejected"]) /
                                            ((self.sim.currentTime - self.overview_last_value_cache["time"]) / 1000000),
                    
                    "packet_ingress_rate": (packet_props.counter_ingress_packets - self.overview_last_value_cache[
                        "packet_ingress"]) /
                                           ((self.sim.currentTime - self.overview_last_value_cache["time"]) / 1000000),
                    
                    "packet_timeout_rate": (packet_props.statsPacketsRejectedProcessingDelay -
                                            self.overview_last_value_cache[
                                                "packet_timeout"]) /
                                           ((self.sim.currentTime - self.overview_last_value_cache["time"]) / 1000000),
                    
                    "packet_success_rate": (packet_props.statsPacketsSuccessfulProcessed -
                                            self.overview_last_value_cache[
                                                "packet_successful"]) /
                                           ((self.sim.currentTime - self.overview_last_value_cache["time"]) / 1000000),
                    
                    "server_idle_ratio": (idle_time - self.overview_last_value_cache["idle_time"]) /
                                         (self.sim.currentTime - self.overview_last_value_cache["time"]) /
                                         len(server_props.all_servers),
                }
                for k, v in rates.items():
                    self.add_entry(self.sim.currentTime, "overview", k, v)
            
            self.add_entry(self.sim.currentTime, "overview", "packet_in_system", packet_props.counter_packet_in_system)
            self.add_entry(self.sim.currentTime, "overview", "packet_total_count", packet_props.statsPacketsTotalCount)
            self.add_entry(self.sim.currentTime, "overview", "packet_rejected",
                           packet_props.statsPacketsRejectedSchedule)
            self.add_entry(self.sim.currentTime, "overview", "packet_timeout",
                           packet_props.statsPacketsRejectedProcessingDelay)
            self.add_entry(self.sim.currentTime, "overview", "packet_successful",
                           packet_props.statsPacketsSuccessfulProcessed)
            self.add_entry(self.sim.currentTime, "overview", "packet_total_delay", packet_props.statsSumDelay)
            if packet_props.statsPacketsSuccessfulProcessed > 0:
                self.add_entry(self.sim.currentTime, "overview", "packet_delivered_delay_avg",
                               packet_props.statsRatiosQos / packet_props.statsPacketsSuccessfulProcessed)
            else:
                self.add_entry(self.sim.currentTime, "overview", "packet_delivered_delay_avg", 1)
            
            self.add_entry(self.sim.currentTime, "overview", "sff_queue",
                           sum([sff.get_number_of_queued_packets() for sff in sff_props.allSFFs.values()]))
            self.add_entry(self.sim.currentTime, "overview", "sfi_queue",
                           sum([len(sfi.queue) for sfi in sfi_props.all_sfi.values()]))
            
            self.overview_last_value_cache["packets_done"] = packet_props.statsPacketsSuccessfulProcessed
            self.overview_last_value_cache["time"] = self.sim.currentTime
            self.overview_last_value_cache["idle_time"] = idle_time
            self.overview_last_value_cache["packet_ingress"] = packet_props.counter_ingress_packets
            self.overview_last_value_cache["packet_rejected"] = packet_props.statsPacketsRejectedSchedule
            self.overview_last_value_cache["packet_timeout"] = packet_props.statsPacketsRejectedProcessingDelay
            self.overview_last_value_cache["packet_successful"] = packet_props.statsPacketsSuccessfulProcessed
        
        # sfi statistics
        for sfiType in self.SFIs:
            for sfi in self.SFIs[sfiType]:
                self.add_entry(self.sim.currentTime, "sfiQueue",
                               sfiType, len(sfi.queue))
                self.add_entry(self.sim.currentTime, "sfiState",
                               sfiType, int(sfi.free))
                self.add_entry(self.sim.currentTime, "sfiCpuShares",
                               sfiType, sfi.cpuShares)
        
        # server statistics
        free_servers = 0
        for server in self.allServers:
            is_free = server.is_free()
            if is_free:
                free_servers += 1
            # self.add_entry(self.sim.currentTime, "server",
            #                "availableShares", server.availableShares)
            self.add_entry(self.sim.currentTime, "server", "is_free", 1 if is_free else 0)
        
        # sff Statistics
        if len(self.SFFs) > 0:
            # self.add_entry(self.sim.currentTime, "sff", "networkQueue",
            #                sum([len(x) for x in sff.outQueue.values()]))
            
            queue_per_sf = {k: 0 for k in range(len(sfi_props.processingRateOfSfType))}
            for sff in self.SFFs:
                if sff_props.consider_link_capacity:
                    self.add_entry(self.sim.currentTime, "sff", "networkQueue",
                                   sum([len(x) for x in sff.outQueue.values()]))
                if sff.scheduler.requires_queues_per_class():
                    for q in sff.packet_queue_per_class:
                        queue_per_sf[flow_props.sfc_class_to_sf[q][0]] += len(sff.packet_queue_per_class[q])
                else:
                    queue_per_sf[0] += len(sff.packet_queue)
                
                self.add_entry(self.sim.currentTime, "sff_queue", sff.id, sff.get_number_of_queued_packets())
            
            for sf in queue_per_sf:
                self.add_entry(self.sim.currentTime, "sf_queue", sf, queue_per_sf[sf])


class SimStats(object):
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            self.exp_id = None
            self.flowIDs: set = set()
            self.flowStats: SimStatsRowWriter = None
            self.allStatsWriter: List[SimStatsWriter] = []
            self.SEPARATOR: str = ','
            self.FLUSH_ENTRIES: int = 10000
            self.debugStats: SimStatsKvWriter = None
            self.packetStats: SimStatsRowWriter = None
            self.packetCdfStats: SimStatsCdfWriter = None
            self.pollStatistics: SimStatsPoller = None
            self.exp_stats: SimStatsKvWriter = None
            self.serverStats: SimStatsRowWriter = None
            self.overviewStats: SimStatsKvWriter = None
            self.workloadStats: SimStatsRowWriter = None
    
    @staticmethod
    def activate_packet_statistics(sim: Sim):
        if sim.props.sim_stats.exp_id is None:
            raise NameError("you have to activate statistics ... SimStats.activate")
        filepath = "stats_{0}_packets.csv".format(sim.props.sim_stats.exp_id)
        sim.props.sim_stats.packetStats = SimStatsRowWriter(
            sim=sim,
            filepath=filepath,
            columns=[
                "id",
                "flow_id",
                "ingress_time",
                "status",
                "time_total",
                "time_processing",
                "time_processing_queue",
                "time_network",
                "time_network_queue",
                "time_queue_scheduling",
                "real_time_scheduling",
                "seen_by_schedulers",
                "ingress_egress_delay"],
            data_types=[
                int,
                int,
                int,
                str,
                int,
                int,
                int,
                int,
                int,
                int,
                float,
                int,
                int])
    
    @staticmethod
    def activate(sim: Sim, exp_id: str, configuration: dict = None):
        stat_props: SimStats.Props = sim.props.sim_stats
        assert (stat_props.exp_id is None)
        if not re.match('^[a-zA-Z0-9\-_]+$', exp_id):
            raise NameError("experiment ID should be alphanumeric")
        stat_props.exp_id = exp_id
        filepath = "stats_{0}.csv".format(stat_props.exp_id)
        stat_props.exp_stats = SimStatsKvWriter(sim=sim, filepath=filepath, data_type=str)
        stat_props.exp_stats.add_kv_entry(key="exp_id", value=exp_id)
        if configuration is not None:
            for k in configuration:
                stat_props.exp_stats.add_kv_entry(key=k, value=f"\"{configuration[k]}\"")
    
    @staticmethod
    def activate_workload_statistics(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        if stat_props.exp_id is None:
            raise NameError("you have to activate statistics ... SimStats.activate")
        filepath = "stats_{0}_workload.csv".format(stat_props.exp_id)
        stat_props.workloadStats = SimStatsRowWriter(sim=sim, filepath=filepath,
                                                     columns=["time", "flow_id", "chain", "ingress",
                                                              "egress", "qos_delay"],
                                                     data_types=[int, int, str, int,
                                                                 int, int])
    
    @staticmethod
    def activate_flow_statistics(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        if stat_props.exp_id is None:
            raise NameError("you have to activate statistics ... SimStats.activate")
        filepath = "stats_{0}_flows.csv".format(stat_props.exp_id)
        stat_props.flowStats = SimStatsRowWriter(sim=sim, filepath=filepath,
                                                 columns=["flow_id", "chain",
                                                          "egress", "qos_delay"],
                                                 data_types=[int, str, int, int])
    
    @staticmethod
    def activate_packet_cdf_statistics(sim: Sim, cdf_buckets: int, per_group: bool = True):
        stat_props: SimStats.Props = sim.props.sim_stats
        if stat_props.exp_id is None:
            raise NameError("you have to activate statistics ... SimStats.activate")
        filepath = "stats_{0}_packet_cdf.csv".format(stat_props.exp_id)
        stat_props.packetCdfStats = SimStatsCdfWriter(sim=sim, filepath=filepath,
                                                      buckets=cdf_buckets,
                                                      per_group=per_group)
    
    @staticmethod
    def activate_server_statistics(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        if stat_props.exp_id is None:
            raise NameError("you have to activate statistics ... SimStats.activate")
        filepath = "stats_{0}_server.csv".format(stat_props.exp_id)
        stat_props.serverStats = SimStatsRowWriter(sim=sim, filepath=filepath,
                                                   columns=["server_id", "capacity", "number_of_sfi",
                                                            "number_of_sff",
                                                            "idle_time"],
                                                   data_types=[int, int, int, int, int])
    
    @staticmethod
    def activate_overview_statistics(sim: Sim, optional_time_snapshots: List[int] = None):
        stat_props: SimStats.Props = sim.props.sim_stats
        if stat_props.exp_id is None:
            raise NameError("you have to activate statistics ... SimStats.activate")
        filepath = "stats_{0}_overview.csv".format(stat_props.exp_id)
        stat_props.overviewStats = SimStatsKvWriter(sim=sim, filepath=filepath, data_type=float)
        if optional_time_snapshots is not None:
            sim.schedule_event(OverviewStatisticsEvent(list_of_snapshots=optional_time_snapshots, sim=sim))
    
    @staticmethod
    def activate_debug_statistics(sim: Sim, filepath):
        stat_props: SimStats.Props = sim.props.sim_stats
        stat_props.debugStats = SimStatsKvWriter(sim=sim, filepath=filepath)
    
    @staticmethod
    def activate_polling_statistics(sim: Sim, interval):
        stat_props: SimStats.Props = sim.props.sim_stats
        if stat_props.exp_id is None:
            raise NameError("you have to activate statistics ... SimStats.activate")
        filepath = "stats_{0}_polling.csv".format(stat_props.exp_id)
        stat_props.pollStatistics = SimStatsPoller(sim=sim,
                                                   filepath=filepath, interval=interval)
    
    @staticmethod
    def activate_polling_sff_statistics(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        assert (not (sim.props.sim_stats.pollStatistics is None))
        stat_props.pollStatistics.SFFs = []
        if len(sim.props.sff.allSFFs) == 0:
            raise NameError("you should call this method after setting up your experiment")
        for sff in sim.props.sff.allSFFs.values():
            stat_props.pollStatistics.SFFs.append(sff)
    
    @staticmethod
    def activate_polling_sfi_statistics(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        assert (not (stat_props.pollStatistics is None))
        stat_props.pollStatistics.SFIs = dict()
        if len(sim.props.sff.allSFFs) == 0:
            raise NameError("you should call this method after setting up your experiment")
        for sff in sim.props.sff.allSFFs.values():
            for sfiArray in sff.SFIsPerType.values():
                for sfi in sfiArray:
                    stat_props.pollStatistics.SFIs.setdefault(
                        sfi.of_type, []).append(sfi)
    
    @staticmethod
    def activate_polling_server_statistics(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        assert (not (stat_props.pollStatistics is None))
        stat_props.pollStatistics.allServers = []
        if len(sim.props.sff.allSFFs) == 0:
            raise NameError("you should call this method after setting up your experiment")
        for sff in sim.props.sff.allSFFs.values():
            for sfiArray in sff.SFIsPerType.values():
                for sfi in sfiArray:
                    stat_props.pollStatistics.allServers.append(sfi.server)
    
    @staticmethod
    def activate_polling_overview_statistics(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        assert (not (stat_props.pollStatistics is None))
        stat_props.pollStatistics.overview = True
    
    @staticmethod
    @Packet.register_tear_down
    def callback_packet_teardown(packet: Packet):
        sim = packet.flow.sim
        stat_props: SimStats.Props = sim.props.sim_stats
        if not (stat_props.packetStats is None):
            stat_props.packetStats.add_entry(
                packet.id,
                packet.flow.id,
                packet.time_ingress,
                packet.final_state,
                packet.delay,
                packet.timeProcessing,
                packet.timeQueueProcessing,
                packet.timeNetwork,
                packet.timeQueueNetwork,
                packet.timeQueueScheduling,
                packet.realTimeScheduling,
                packet.seenByScheduler,
                SFF.get_multi_hop_latency_for(sim, packet.ingress_sff_id, packet.flow.desiredEgressSSFid))
        
        expected_delay = packet.timeProcessing + packet.timeNetwork + \
                         packet.timeQueueScheduling + packet.timeQueueProcessing + packet.timeQueueNetwork
        if not packet.delay == expected_delay:
            print(f"\n timeProcessing:{packet.timeProcessing} timeNetwork:{packet.timeNetwork} "
                  f"timeQueueNetwork:{packet.timeQueueNetwork} timeQueueProcessing:{packet.timeQueueProcessing} "
                  f"timeQueueScheduling:{packet.timeQueueScheduling} \n.. sum: {expected_delay} "
                  f"ingress time:{packet.time_ingress} now:{sim.currentTime} -> delay:{packet.delay}")
            raise NameError("something goes wrong with this packet!")
        
        if not (stat_props.packetCdfStats is None):
            # latency (service quality)
            value = packet.delay / packet.flow.qosMaxDelay if (packet.final_state == "done") else 1
            stat_props.packetCdfStats.add_data_point(key="latency",
                                                     group=packet.flow.sfc_identifier,
                                                     value=value)
        
        if not (stat_props.flowStats is None):
            if not (packet.flow.id in stat_props.flowIDs):
                stat_props.flowIDs.add(packet.flow.id)
                stat_props.flowStats.add_entry(packet.flow.id,
                                               '-'.join(map(str,
                                                            packet.flow.sfTypeChain)),
                                               packet.flow.desiredEgressSSFid,
                                               packet.flow.qosMaxDelay)
    
    @staticmethod
    @Sim.register_workload_over_hook
    def workload_over_hook(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        for sw in stat_props.allStatsWriter:
            sw.inform_workload_is_over()
    
    @staticmethod
    def do_overview_statistics_snapshot(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        packet_props: Packet.Props = sim.props.packet
        server_props: Server.Props = sim.props.server
        sff_props: SFF.Props = sim.props.sff
        
        # grab all statistics
        stat_props.overviewStats.add_kv_entry("packet_total_count", packet_props.statsPacketsTotalCount)
        stat_props.overviewStats.add_kv_entry("packet_rejected", packet_props.statsPacketsRejectedSchedule)
        stat_props.overviewStats.add_kv_entry("packet_timeout", packet_props.statsPacketsRejectedProcessingDelay)
        stat_props.overviewStats.add_kv_entry("packet_successful", packet_props.statsPacketsSuccessfulProcessed)
        if packet_props.statsPacketsSuccessfulProcessed > 0:
            tmp = (packet_props.statsPacketsSuccessfulProcessed -
                   packet_props.counter_packet_after_workload_end_in_system_no_timeout)
            if tmp > 0:
                stat_props.overviewStats.add_kv_entry("packet_delivered_delay_avg",
                                                      packet_props.statsRatiosQos /
                                                      tmp)
            else:
                stat_props.overviewStats.add_kv_entry("packet_delivered_delay_avg", 1)
        else:
            stat_props.overviewStats.add_kv_entry("packet_delivered_delay_avg", 1)
        stat_props.overviewStats.add_kv_entry("packet_total_delay", packet_props.statsSumDelay)
        
        idle_time = 0
        for s in server_props.all_servers:
            idle_time += s.stats_idle_time
            if s.is_free():
                idle_time += sim.currentTime - s.stats_last_time_idle
        
        stat_props.overviewStats.add_kv_entry("server_idle_time_total", idle_time)
        stat_props.overviewStats.add_kv_entry("server_idle_time_per_server",
                                              idle_time / len(server_props.all_servers))
        stat_props.overviewStats.add_kv_entry("simulation_time", sim.lastRelevantTime)
        stat_props.overviewStats.add_kv_entry("scheduler_attempts",
                                              sum([s.scheduler.scheduling_attempts for s in
                                                   sff_props.allSFFs.values()]))
        stat_props.overviewStats.add_kv_entry("time_of_last_ingress", sim.last_packet_ingress_time)
    
    @staticmethod
    @Sim.register_simulation_done_hook
    def sim_done_hook(sim: Sim):
        stat_props: SimStats.Props = sim.props.sim_stats
        server_props: Server.Props = sim.props.server
        
        if stat_props.exp_stats is not None:
            stat_props.exp_stats.add_kv_entry("runtime", str(time.time() - sim.start_time))
            stat_props.exp_stats.flush()
        
        if stat_props.serverStats is not None:
            for server in server_props.all_servers:
                stat_props.serverStats.add_entry(server.id, server.processing_cap, len(server.SFIs),
                                                 len(server.SFF_ids),
                                                 server.stats_idle_time +
                                                 (sim.currentTime - server.stats_last_time_idle)
                                                 if server.is_free() else 0)
            stat_props.serverStats.flush()
        
        if stat_props.overviewStats is not None:
            SimStats.do_overview_statistics_snapshot(sim)
            stat_props.overviewStats.flush()
        
        if stat_props.debugStats is not None:
            stat_props.debugStats.flush()
        
        if stat_props.packetStats is not None:
            stat_props.packetStats.flush()
        
        if stat_props.packetCdfStats is not None:
            stat_props.packetCdfStats.prepare_flush()
            stat_props.packetCdfStats.flush()
        
        if stat_props.pollStatistics is not None:
            stat_props.pollStatistics.flush()
        
        if stat_props.serverStats is not None:
            stat_props.serverStats.flush()
