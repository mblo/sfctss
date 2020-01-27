# coding=utf-8
import itertools
import random

import sure

from behave import given, when, then, step, register_type
from parse_type import TypeBuilder

import sfctss
from sfctss.scheduler.examples import GreedyShortestDeadlineFirstScheduler, LoadUnawareRoundRobinScheduler, RejectScheduler

SCHEDULER_GREEDY_ORACLE = "GreedyOracle"
SCHEDULER_GREEDY_LOCAL = "GreedyLocal"
SCHEDULER_STATIC = "Static"
SCHEDULER_REJECT = "Reject"
schedulers = [SCHEDULER_GREEDY_ORACLE,
              SCHEDULER_GREEDY_LOCAL,
              SCHEDULER_STATIC,
              SCHEDULER_REJECT]
parse_scheduler = TypeBuilder.make_choice(schedulers)
register_type(Scheduler=parse_scheduler)

DO = 'do'
DO_NOT = 'do not'
do_do_not = [DO, DO_NOT]
parse_do_do_not = TypeBuilder.make_choice(do_do_not)
register_type(DoDoNot=parse_do_do_not)

default_config = {
    'sff_bw_capacity': 1000,
    'sfi_rate': 15,
    'cpu_policy': 'one-at-a-time',
    'individual_class_per_egress': False,
    'latency_between_sites': 1000,
    'latency_within_sites': 10,
    'number_of_sf_types': 5,
    'number_of_total_sfis': 100,
    'admission_threshold_high': 1.2,
    'admission_threshold_low': 0.8,
    'scheduler_incremental': True,
    'scheduler_per_packet_scheduling': True,
    'seed': 0,
    'server_capacity': 100,
    'workload_deadline_scaling': 50,
    'workload_flow_arrival_h': 8,
    'workload_flow_arrival_l': 100,
    'workload_lambda': 13,
    'workload_packet_inter_arrival_expected_time': 800,
    'workload_packets_per_flow': 150,
    'workload_probability_factor': 1.0,
    'workload_probability_stay_in_h': 0.5,
    'workload_probability_stay_in_l': 0.6,
    'workload_start_new_flows_till': 1000000
}


@given("an empty simulator setup")
def step_impl(context):
    context.sim_conf = {k: v for k, v in default_config.items()}
    context.sim = sfctss.simulator.Sim(context.sim_conf['seed'])
    context.all_exclusive_servers = []
    context.all_shared_servers = []
    context.all_servers = {}
    context.all_sff = []
    context.random = random.Random()
    context.random.seed(context.sim_conf['seed'])
    context.traffic_classes = []


@step('we have "{number:d}" Servers with capacity "{capacity}"')
def step_impl(context, number, capacity):
    capacity = int(capacity)
    if context.sim_conf['cpu_policy'] == 'one-at-a-time':
        cpu_policy = sfctss.server.ServerCpuPolicy.one_at_a_time
    elif context.sim_conf['cpu_policy'] == 'dynamic':
        cpu_policy = sfctss.server.ServerCpuPolicy.dynamic
    else:
        raise NameError(f"unexpected cpu policy {context.sim_conf['cpu_policy']}")
    
    for i in range(int(number)):
        s = sfctss.server.Server(context.sim, int(capacity), cpu_policy)
        context.all_servers[s.id] = s


@step('we have "{number}" SFIs of type "{sfi_type}" running on server "{server_id}" belonging to sff "{sff_id}"')
def step_impl(context, number, sfi_type, server_id, sff_id):
    s_id = int(server_id)
    if s_id not in context.all_servers:
        raise NameError(f"unknown server id {s_id}")
    sff = [e for e in context.all_sff if e.id == int(sff_id)]
    if len(sff) != 1:
        raise NameError(f"unknown sff id {sff_id}")
    sff = sff[0]
    s: sfctss.server.Server = context.all_servers[s_id]
    print(f"install {int(number)} sfi at serer {server_id}")
    for i in range(int(number)):
        s.add_sfi(of_type=int(sfi_type),
                  with_sff_id=sff.id)


@step(
    'we have "{total_sfi:d}" SFIs of type "{sfi_type:d}" running on "{num_servers}" servers and "{do_share_server:DoDoNot}" share the server')
def step_impl(context, total_sfi, sfi_type, num_servers, do_share_server):
    num_servers = int(num_servers)
    
    if context.sim_conf['cpu_policy'] == 'one-at-a-time':
        cpu_policy = sfctss.server.ServerCpuPolicy.one_at_a_time
    elif context.sim_conf['cpu_policy'] == 'dynamic':
        cpu_policy = sfctss.server.ServerCpuPolicy.dynamic
    else:
        raise NameError(f"unexpected cpu policy {context.sim_conf['cpu_policy']}")
    
    if do_share_server == DO:
        while len(context.all_shared_servers) < num_servers:
            context.all_shared_servers.append(sfctss.server.Server(context.sim, context.sim_conf['server_capacity'], cpu_policy))
        servers = context.random.sample(context.all_shared_servers, num_servers)
    elif do_share_server == DO_NOT:
        servers = []
        for _ in range(num_servers):
            s = sfctss.server.Server(context.sim, context.sim_conf['server_capacity'], cpu_policy)
            servers.append(s)
            context.all_exclusive_servers.append(s)
    else:
        raise NameError(f"unknown parameter for share server: {do_share_server}")
    
    cycle_server = itertools.cycle(servers)
    cycle_sff = itertools.cycle(context.random.choices(context.all_sff, k=total_sfi))
    
    for _ in range(total_sfi):
        cycle_server.__next__().add_sfi(of_type=sfi_type, with_sff_id=cycle_sff.__next__().id)


@given('we have "{total_sff:d}" SFFs using scheduler "{scheduler:Scheduler}"')
def step_impl(context, total_sff, scheduler):
    if scheduler == SCHEDULER_GREEDY_ORACLE:
        sched = lambda: GreedyShortestDeadlineFirstScheduler(sim=context.sim,
                                                             incremental=context.sim_conf['scheduler_incremental'],
                                                             oracle=True,
                                                             admission_control_threshold_low=
                                                             context.sim_conf[
                                                                 'admission_threshold_low'],
                                                             admission_control_threshold_high=
                                                             context.sim_conf[
                                                                 'admission_threshold_high'])
    elif scheduler == SCHEDULER_GREEDY_LOCAL:
        sched = lambda: GreedyShortestDeadlineFirstScheduler(sim=context.sim,
                                                             incremental=context.sim_conf['scheduler_incremental'],
                                                             oracle=False,
                                                             admission_control_threshold_low=
                                                             context.sim_conf[
                                                                 'admission_threshold_low'],
                                                             admission_control_threshold_high=
                                                             context.sim_conf[
                                                                 'admission_threshold_high'])
    elif scheduler == SCHEDULER_STATIC:
        sched = lambda: LoadUnawareRoundRobinScheduler(sim=context.sim,
                                                       incremental=context.sim_conf['scheduler_incremental'],
                                                       oracle=True)
    elif scheduler == SCHEDULER_REJECT:
        sched = lambda: RejectScheduler(sim=context.sim,
                                        incremental=context.sim_conf['scheduler_incremental'],
                                        oracle=context.sim_conf['scheduler_oracle'])
    else:
        raise NameError(f'unsupported scheduler {context.sim_conf["scheduler"]}')
    
    for _ in range(total_sff):
        sff = sfctss.model.SFF(context.sim, sched())
        context.all_sff.append(sff)


@step('we have a traffic class "{traffic_class}" with latency "{qos_delay:d}" ns')
def step_impl(context, traffic_class, qos_delay):
    tc = [int(i) for i in traffic_class.split('-')]
    context.traffic_classes.append((tc, qos_delay))


@step('we have for each traffic class "{num_flows:d}" flows each with "{num_packets_per_flow:d}" packets')
def step_impl(context, num_flows, num_packets_per_flow):
    start_time = 0
    for tc, deadline in context.traffic_classes:
        for _ in range(num_flows):
            flow = sfctss.model.Flow(sim=context.sim, sf_type_chain=tc,
                                     qos_max_delay=deadline,
                                     desired_egress_ssf_id=context.random.sample(context.all_sff, k=1)[0].id,
                                     ingress_sff_id=context.random.sample(context.all_sff, k=1)[0].id,
                                     start_time=start_time + context.random.randint(0, 200))
            last_time = flow.start_time
            start_time = last_time
            for _ in range(num_packets_per_flow):
                context.sim.schedule_event(sfctss.model.Packet.create_wrap_in_event(
                    time_ingress=int(last_time + context.random.randint(1, 20)),
                    flow=flow,
                    transmission_size=1))


@when("we let the simulation run till all processing is done")
def step_impl(context):
    sfctss.sanity.print_infrastructure(context.sim)
    context.sim.run_sim(show_progress=False, ui=False, stop_simulation_when_workload_is_over=False)
    sfctss.sanity.print_simple_stats(context.sim)
    sfctss.sanity.print_infrastructure(context.sim, with_snap=True)


@when("we let the simulation run till all workload is sent")
def step_impl(context):
    sfctss.sanity.print_infrastructure(context.sim)
    context.sim.run_sim(show_progress=False, ui=False, stop_simulation_when_workload_is_over=True)
    sfctss.sanity.print_simple_stats(context.sim)
    sfctss.sanity.print_infrastructure(context.sim, with_snap=True, per_sfi_stats=True)


@then("no packet is still in the simulator")
def step_impl(context):
    def check_remaining_packets():
        for f in context.sim.stop_sim_catch_all_packets_hooks:
            for p in f(context.sim):
                yield p
        
        while context.sim.event_list.len() > 0:
            e = context.sim.event_list.pop_next()
            if isinstance(e, sfctss.events.PacketHoldingEvent):
                yield e.inner_packet
    
    p_finder = check_remaining_packets()
    for p in p_finder:
        sfctss.sanity.print_sim_snapshot(context.sim)
        raise NameError(f'we found a packet in the simulation, but there shouldn\'t be any packet {p}')


@then('success rate is in the range of "{expected_success_rate}" allow delta {delta}')
def step_impl(context, expected_success_rate, delta):
    expected = float(expected_success_rate)
    context.sim.calculate_simple_statistics()
    actual: float = context.sim.stats['success_rate']
    sure.expect(actual).equal(expected, epsilon=float(delta))


@then('reject rate is in the range of "{expected_reject_rate}" allow delta {delta}')
def step_impl(context, expected_reject_rate, delta):
    expected = float(expected_reject_rate)
    context.sim.calculate_simple_statistics()
    actual: float = context.sim.stats['reject_rate']
    sure.expect(actual).equal(expected, epsilon=float(delta))


@then('service quality is in the range of "{expected_service_quality}" allow delta {delta}')
def step_impl(context, expected_service_quality, delta):
    expected = float(expected_service_quality)
    context.sim.calculate_simple_statistics()
    actual: float = context.sim.stats['service_quality']
    sure.expect(actual).equal(expected, epsilon=float(delta))


@step('we have "{number_of_sfi_types:d}" sfi types and SFF-SFI connections use latency class "{latency}"')
def step_impl(context, number_of_sfi_types, latency):
    sfctss.model.SFI.init_data_structure(sim=context.sim,
                                         number_of_sf_types=number_of_sfi_types,
                                         latency_provider_sff_sfi=int(latency))
    for i in range(number_of_sfi_types):
        sfctss.model.SFI.setup_sf_processing_rate_per_1s(context.sim, i, context.sim_conf['sfi_rate'])


@step('we have latency class "{clazz}" of latency "{latency}"')
def step_impl(context, clazz, latency):
    latency = int(latency)
    sfctss.model.SFF.setup_latency_distribution(context.sim, id=int(clazz), values=[latency for _ in range(10)])


@step('we connect all sff with each other using latency class "{latency}"')
def step_impl(context, latency):
    latency = int(latency)
    for i, sff_a in enumerate(context.all_sff):
        for sff_b in context.all_sff[i + 1:]:
            sfctss.model.SFF.setup_connection(context.sim, sff_a.id, sff_b.id, context.sim_conf['sff_bw_capacity'], latency,
                                              bidirectional=True)


@then('the idle time of the servers is on avg below "{idle_time_avg}"%')
def step_impl(context, idle_time_avg):
    idle_time_avg = float(idle_time_avg)
    if context.sim.stats is None:
        context.sim.calculate_simple_statistics()
    sure.expect(context.sim.stats['server_idle_time_ratio']).lower_than(idle_time_avg)


@given('we set config "{key}" to "{value}"')
def step_impl(context, key, value):
    if key not in context.sim_conf:
        raise NameError(f'unkown config "{key}"')
    if isinstance(context.sim_conf[key], int):
        context.sim_conf[key] = int(value)
    elif isinstance(context.sim_conf[key], float):
        context.sim_conf[key] = float(value)
    elif isinstance(context.sim_conf[key], str):
        context.sim_conf[key] = str(value)
    elif isinstance(context.sim_conf[key], bool):
        context.sim_conf[key] = bool(value)
    else:
        raise NameError(f'unknown config type: "{type(context.sim_conf[key])}"')


@step("we activate debug mode")
def step_impl(context):
    context.sim.DEBUG = True


@step('we have for traffic class "{traffic_class}" a static rate of "{packets:d}" packet every "{interval:d}" time steps at ingress "{ingress:d}"')
def step_impl(context, traffic_class, packets, interval, ingress):
    start_time = 0
    sff = [e for e in context.all_sff if e.id == int(ingress)]
    if len(sff) != 1:
        raise NameError(f"unknown sff id {ingress}")
    sff = sff[0]
    interval = int(interval)
    packets = int(packets)
    end = context.sim_conf["workload_start_new_flows_till"]
    
    for tc, deadline in context.traffic_classes:
        tc_name = "-".join(map(str, tc))
        if tc_name == traffic_class:
            flow = sfctss.model.Flow(sim=context.sim, sf_type_chain=tc,
                                     qos_max_delay=deadline,
                                     desired_egress_ssf_id=sff.id,
                                     ingress_sff_id=sff.id,
                                     start_time=start_time)
            
            last_time = flow.start_time
            while last_time < end:
                last_time += interval
                for _ in range(packets):
                    context.sim.schedule_event(sfctss.model.Packet.create_wrap_in_event(
                        time_ingress=int(last_time),
                        flow=flow,
                        transmission_size=1))
            
            return
    raise NameError(f"unknown traffic class {traffic_class}")
