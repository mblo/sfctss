#!/usr/bin/env python3
# coding=utf-8
import math
import os

from .simulator import Sim


@Sim.register_ui_update_hook
def update_ui(sim: Sim, ticks, split_server_table):
    fill_packet = len(str(sim.props.packet.lastId))
    fill_sfi = len(str(sim.props.sfi.next_free_id))
    fill_sff = len(str(len(sim.props.sff.allSFFs)))
    fill_sf = len(str(len(sim.props.sfi.processingRateOfSfType)))
    fill_server = len(str(sim.props.server.lastId))
    os.system('cls' if os.name == 'nt' else 'clear')
    print("#" * 50)
    sim.update_sim_status_oneliner(ticks, newline=True)
    print("#" * 50)
    
    left_margin = 34 + fill_sff + fill_packet
    
    packets_seen = sim.props.packet.counter_packet_in_system + sim.props.packet.statsPacketsSuccessfulProcessed + \
                   sim.props.packet.statsPacketsRejectedProcessingDelay + sim.props.packet.statsPacketsRejectedSchedule
    print(f' packets in the system: {str(sim.props.packet.counter_packet_in_system).rjust(fill_packet, " ")} ' +
          f' total SFF-SFF hops: {sim.props.sff.counter_packets_put_on_wire} '
          f'({round(sim.props.sff.counter_packets_put_on_wire / packets_seen, 2)} hops / packet)')
    print("#" * 50)
    alphas_per_class = dict()
    for queue in range(sim.props.flow.sfc_next_free_class):
        alpha_normalized_enumerator = math.pow(sim.props.flow.max_deadline, 2)
        alpha_denominator = math.pow(sim.props.flow.sfc_class_to_deadline[queue], 2)
        alphas_per_class[queue] = alpha_normalized_enumerator / alpha_denominator
    
    print(
        f"{' alpha values per class '.rjust(left_margin, ' ')}"
        f"{[str(round(x, 2)).rjust(fill_packet, ' ') for queue in sorted(alphas_per_class.keys()) for x in [alphas_per_class[queue]]]}")
    out = [(queue, f"{(str(sf) + ' ' + ('EX ' if last else '  ')).rjust(fill_packet, ' ')}") for queue, (sf, last) in
           sim.props.flow.sfc_class_to_sf.items()]
    out = sorted(out, key=lambda x: x[0])
    
    print(f"{' sf type of class, and last_of_sfc flag '.rjust(left_margin, ' ')}{[x for _, x in out]}")
    print(
        f"{' deadline of the class '.rjust(left_margin, ' ')}"
        f"{[str(sim.props.flow.sfc_class_to_deadline[x]).rjust(fill_packet, ' ') for x in range(sim.props.flow.sfc_next_free_class)]}")
    
    print(" service rate per sf: " + str(
        [f"SF {i}: {(round(sim.props.sfi.processingRateOfSfType[i], 1))}" for i in
         range(len(sim.props.sfi.processingRateOfSfType))]))
    
    print("#" * 50)
    print("# SFF Overview: SFF.id | total items in queue | items per packet class")
    
    for sff in sim.props.sff.allSFFs.values():
        queues = ""
        
        if sff.scheduler.requires_queues_per_class():
            queues = [str(len(sff.packet_queue_per_class[p_class])).rjust(fill_packet, '_') for p_class in
                      range(sim.props.flow.sfc_next_free_class)]
        
        raw_rates = [round(sff.scheduler.get_arrival_rate_estimate(p_class))
                     for p_class in range(len(sim.props.sfi.processingRateOfSfType))]
        raw_load = [
            f'{round(raw_rates[p_class] / sff.scheduler.mySFF.service_rate_per_sf[p_class], 2) if p_class in sff.scheduler.mySFF.service_rate_per_sf else "/"}|{raw_rates[p_class]}'
            for p_class in range(len(sim.props.sfi.processingRateOfSfType))]
        
        print(f" ID{str(sff.id).rjust(fill_sff, '_')}:  " +
              f"{str(sff.get_number_of_queued_packets()).rjust(fill_packet, '_')} " +
              f"packets in queue, classes: {queues}")
        
        print(f"rate|load /sf; total: {str(raw_load).rjust(fill_packet, ' ')}: ".rjust(left_margin, ' '))
    
    # print the drops per class
    drops = [str(sim.props.flow.statistics_drops_per_class[x]).rjust(fill_packet, '_') for x in
             range(sim.props.flow.sfc_next_free_class)]
    print("")
    print("drops per class: ".rjust(left_margin, ' ') + str(drops))
    
    if not sim.ui_shows_full_state:
        return
    
    print("#" * 50)
    print("# SFI Overview ")
    
    per_sf_queue = {sf: 0 for sf in range(len(sim.props.sfi.processingRateOfSfType))}
    per_sf_active = {sf: 0 for sf in range(len(sim.props.sfi.processingRateOfSfType))}
    
    per_sf_distinct_server = {sf: set() for sf in range(len(sim.props.sfi.processingRateOfSfType))}
    for sfi in sim.props.sfi.all_sfi.values():
        if not sfi.free:
            per_sf_active[sfi.of_type] += 1
        per_sf_distinct_server[sfi.of_type].add(sfi.server)
        per_sf_queue[sfi.of_type] += len(sfi.queue)
    
    for sf in per_sf_queue:
        print(
            f" SF{str(sf).rjust(fill_sf, '0')}: {str(per_sf_queue[sf]).rjust(fill_packet, '_')} packets in queue, " +
            f" [{str(per_sf_active[sf] * '#').ljust(len(per_sf_distinct_server[sf]), '_')}] " +
            f"active SFIs (out of {len([x for x in sim.props.sfi.all_sfi.values() if x.of_type == sf])})")
    
    print("#" * 50)
    print("# SFI/Server details: for each server, id | capacity | idle time")
    server_messages = []
    
    for server in sim.props.server.all_servers:
        idle_time = server.stats_idle_time
        if server.is_free():
            idle_time += sim.currentTime - server.stats_last_time_idle
        
        server_messages.append(f" Server {str(server.id).rjust(fill_server, '_')}, " +
                               f"{server.processing_cap}units | {str(round(idle_time / sim.currentTime * 100, 1)).rjust(4, ' ')}% idle")
        for sfi in server.SFIs:
            server_messages.append(f"   SFI {str(sfi.id).rjust(fill_sfi, '_')} of type " +
                                   f"{str(sfi.of_type).rjust(fill_sf, '_')}:  " +
                                   f"{'OFF' if sfi.free else 'ON '} " +
                                   f" {str(len(sfi.queue)).rjust(fill_packet, '_')} packets in queue")
    
    fill_line = max([len(str(l)) for l in server_messages]) + 2
    
    if split_server_table:
        left = server_messages[:len(server_messages) // 2]
        right = server_messages[len(left):]
        
        while len(left) > 0 and len(right) > 0:
            print(
                f'{left.pop(0)}'.ljust(fill_line, ' ') + (' ' if len(left) % 2 == 0 else '|') + f"  {right.pop(0)}")
        print((left + right + [""]).pop(0))
    else:
        for m in server_messages:
            print(m)
