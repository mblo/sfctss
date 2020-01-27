#!/usr/bin/env python3
# coding=utf-8
from .model.core import Flow
from .model.sff import SFF
from .model.sfi import SFI
from .scheduler.core import BaseScheduler
from .simulator import Sim


class BG_Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def colour_print(value, warning: bool):
    return f'{BG_Colors.WARNING if warning else BG_Colors.OKGREEN}{value}{BG_Colors.ENDC}'


def print_sim_snapshot(sim: Sim, per_sfi_stats: bool = False):
    if sim.stats is None:
        sim.calculate_simple_statistics()
    
    queued_packets_sff = sum([sff.get_number_of_queued_packets() for sff in sim.props.sff.allSFFs.values()])
    print(f' packets queued among all SFFs:'
          f' {colour_print(queued_packets_sff, queued_packets_sff > 0)}')
    print(
        f' SFI status: '
        f'{len([sfi for sfi in sim.props.sfi.all_sfi.values() if sfi.free])}'
        f'/{len(sim.props.sfi.all_sfi)} free/all')
    queued_packets_sfi = [len(sfi.queue) for sfi in sim.props.sfi.all_sfi.values()]
    if per_sfi_stats:
        print(f' packets queued among all SFIs:')
        for id, of_type, packets in [(sfi.id, sfi.of_type, len(sfi.queue)) for sfi in sim.props.sfi.all_sfi.values()]:
            print(f'\tid {id}: {packets} of type {of_type}')
    else:
        print(f' packets queued among all SFIs:'
              f'{colour_print(sum(queued_packets_sfi), sum(queued_packets_sfi) > 0)}')
    
    print(f' idle time of all servers: {sim.stats["server_idle_time"]}, '
          f'on avg {sim.stats["server_idle_time_ratio"]}% each server over time')
    
    print(f' statistics\n\t{sim.props.packet.statsPacketsRejectedProcessingDelay}: timeout')
    print(f'\t{sim.props.packet.statsPacketsRejectedSchedule}: scheduler reject')
    print(f'\t{sim.props.packet.statsPacketsSuccessfulProcessed}: successful processed')
    print(f'\t{sim.props.packet.statsPacketsTotalCount}: total')


def validate_path(path: list, start_sff: SFF):
    last_sff: SFF = start_sff
    last_hop = start_sff
    sim = start_sff.sim
    
    for entry_type, entry in path:
        if entry_type == SFF.__name__:
            assert entry.sim == sim
            # last hop was SFI?
            if last_hop.__class__.__name__ == SFI.__name__:
                # check if SFI belongs to this SFF
                assert (last_hop.sffId == entry.id)
            else:
                # last hop is SFF, so check if there exists a connection
                assert (SFF.check_if_connection_exists(sim, last_hop.id, entry.id))
            last_sff = entry
        else:
            # entry if sfi,
            # check if its part of the last SFF
            assert (entry.sffId == last_sff.id)
        
        last_hop = entry


def print_simple_stats(sim: Sim, per_sfi_stats: bool = False):
    sim.calculate_simple_statistics()
    print("simple stats")
    print(f"\tsuccess rate: \t\t{sim.stats['success_rate']}")
    print(f"\tservice_quality: \t{sim.stats['service_quality']}")
    
    print(
        f"\t packets fed to the simulator:\t\t\t{sim.props.packet.statsPacketsTotalCount} "
        f"({sim.props.flow.lastId} flows, {sim.props.flow.sfc_next_free_class} traffic classes)")
    print("\t packets rejected during scheduling:\t\t{0}".format(
        str(sim.props.packet.statsPacketsRejectedSchedule)))
    print("\t packets dropped because of delay max out:\t{0}".format(
        str(sim.props.packet.statsPacketsRejectedProcessingDelay)))
    print(
        f"\t total scheduling attempts: \t\t\t{sum([s.scheduler.scheduling_attempts for s in sim.props.sff.allSFFs.values()])}")
    print(f"\t packets successful processed:\t\t\t{sim.props.packet.statsPacketsSuccessfulProcessed} " +
          f"({sim.props.packet.counter_packet_after_workload_end_in_system_no_timeout} packets still in "
          f"the simulation when workload was over, but delay < QoS deadline)")
    if (
            sim.props.packet.statsPacketsSuccessfulProcessed - sim.props.packet.counter_packet_after_workload_end_in_system_no_timeout) > 0:
        print("\t successful, avg delay/max delay:\t\t{0}".format(
            str(sim.props.packet.statsRatiosQos / (
                    sim.props.packet.statsPacketsSuccessfulProcessed - sim.props.packet.counter_packet_after_workload_end_in_system_no_timeout))))
    print("\t total delay among all packets:\t\t\t{0}".format(
        str(sim.props.packet.statsSumDelay)))
    sim.print_sim_snapshot(per_sfi_stats=per_sfi_stats)


def print_infrastructure(sim: Sim, with_snap: bool = False, per_sfi_stats: bool = False):
    print("SFFs:")
    sff: SFF
    for sff in sim.props.sff.allSFFs.values():
        print(f" SFF {sff.id}")
        if sim.props.sff.linkBwCap is not None:
            print(
                f"\tconnected to: {[other.id for other in sim.props.sff.allSFFs.values() if SFF.check_if_connection_exists(sim, sff.id, other.id)]}")
        print(f"\t{len(sff.SFIsPerType)} sf types: {sorted(sff.SFIsPerType.keys())}")
        print(f'\t{sum([len(x) for x in sff.SFIsPerType.values()])} SFIs: '
              f'{["sf " + str(sf) + " -> " + str(len(sff.SFIsPerType[sf])) + " sfis" for sf in sff.SFIsPerType]}')
        print(f"\t{len(sff.servers)} servers: {sorted([s.id for s in sff.servers])}")
        print(f"\t{sff.scheduler}")
        if with_snap:
            sched: BaseScheduler = sff.scheduler
            found = {}
            if sched.requires_queues_per_class():
                for packet_class in sff.packet_queue_per_class:
                    if 0 < len(sff.packet_queue_per_class[packet_class]):
                        found_sfc, pos = Flow.debug_get_sfc_identifier_and_pos_of_packet_class(sim, packet_class)
                        
                        for rel in range(-5, 5, 1):
                            print(
                                f'.. {packet_class + rel} -> {Flow.debug_get_sfc_identifier_and_pos_of_packet_class(sim, packet_class + rel)}')
                        
                        sf, eoc = Flow.get_sf_and_eoc_of(sim, found_sfc, pos)
                        found_sfc = f'{found_sfc}/{pos}/{sf}'
                        if found_sfc not in found:
                            found[found_sfc] = len(sff.packet_queue_per_class[packet_class])
                        else:
                            found[found_sfc] += len(sff.packet_queue_per_class[packet_class])
            for sfc in found:
                print(f'\t  {found[sfc]} packets of sfc: {sfc}')
            print_sim_snapshot(sim, per_sfi_stats)
    
    print("Servers:")
    for server in sim.props.server.all_servers:
        print(f" Server {server.id} hosting {len(server.SFIs)} SFIs of {len(server.SFF_ids)} SFFs")
        print(f"\t {server.SFIs}")
    
    print("SFI statistics")
    
    # for each SF type, how many instances do we have?
    # for each SF type, what is the processing capacity (total/effective shared)
    sfi_props: SFI.Props = sim.props.sfi
    # sfi count, raw capacity, shared capacity
    sfi_capacity = {sf: [0, 0, 0] for sf in range(len(sfi_props.processingRateOfSfType))}
    for sfi in sfi_props.all_sfi.values():
        sfi_capacity[sfi.of_type][0] += 1
        sfi_capacity[sfi.of_type][1] += sfi.server.processing_cap
        sfi_capacity[sfi.of_type][2] += sfi.server.processing_cap / len(sfi.server.SFIs)
    
    for sf in sfi_capacity:
        print(f"\t{sf}: {sfi_capacity[sf][0]} SFIs, {sfi_capacity[sf][1]}/{round(sfi_capacity[sf][2], 1)} capacity")
    
    print("SFI / SFF statistics")
    sff_count = len(sim.props.sff.allSFFs)
    for sf in sfi_capacity:
        print(f"\t{sf}: {round(sfi_capacity[sf][0] / sff_count, 1)} SFIs, "
              f"{round(sfi_capacity[sf][1] / sff_count, 1)}/{round(sfi_capacity[sf][2] / sff_count, 1)} capacity")
    
    return sfi_capacity
