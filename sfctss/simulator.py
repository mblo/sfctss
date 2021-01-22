#!/usr/bin/env python3
# coding=utf-8

import datetime
import gc
import pprint
import random
import re
import time
import traceback
from typing import Generator

import numpy as np

from . import __version__, __copyright__
from .events import EventList, BaseEvent, PacketHoldingEvent


class SchedulingFailure(Exception):
    def __init__(self, message):
        super().__init__(message)


class Sim(object):
    # hooks
    reset_global_fields = []
    simulation_done_hooks = []
    simulation_start_hooks = []
    stop_sim_catch_all_packets_hooks = []
    simulation_one_liner_hooks = []
    workload_over_hooks = []
    ui_update_hook = None
    
    @classmethod
    def register_reset_global_fields(cls, clazz):
        cls.reset_global_fields.append(clazz)
        print(f'::register class {clazz}')
        return clazz
    
    @classmethod
    def register_simulation_done_hook(cls, func):
        cls.simulation_done_hooks.append(func)
        return func
    
    @classmethod
    def register_workload_over_hook(cls, func):
        cls.workload_over_hooks.append(func)
        return func
    
    @classmethod
    def register_simulation_start_hook(cls, func):
        cls.simulation_start_hooks.append(func)
        return func
    
    @classmethod
    def register_sim_oneliner_text_provider(cls, func):
        cls.simulation_one_liner_hooks.append(func)
        return func
    
    # these hooks provide iterator access to all packets that are somewhere stored at internal data structures
    @classmethod
    def register_stop_sim_catch_all_packets_hooks(cls, func):
        cls.stop_sim_catch_all_packets_hooks.append(func)
        return func
    
    @classmethod
    def register_ui_update_hook(cls, f):
        if cls.ui_update_hook is not None:
            print('WARNING someone overwrites ui update hook')
        cls.ui_update_hook = f
        return f
    
    def __init__(self, seed):
        self.packet_generator: Generator = None
        self.workload_end_time: int = 0
        self.last_packet_ingress_time = 0
        self.ui_shows_full_state = True
        self.start_time = None
        self.stats = None
        self.current_event = None
        self.PACKET_ID_TO_DEBUG: int = None
        self.STOP_SIMULATION_IF_SCHEDULER_WAS_UNSUCCESSFUL: bool = False
        self.TRACE_PACKET_PATH: bool = False
        self.KEEP_LIST_OF_ALL_PACKETS: bool = False
        self.SERVER_CPU_POLICY_DYNAMIC_INTERVAL: int = 1000000
        self.DEBUG: bool = False
        self.SERVER_CPU_SHARE_GRANULARITY: int = 10000
        self.run: bool = False
        self.currentTime: int = 0
        self.event_list: EventList = EventList(sim=self)
        self.ignore_all_future_schedule_event_attempts: bool = False
        self.lastRelevantTime = 0
        self.printing_progress_previous_ticks = 0
        self.printing_progress_previous_time = 0
        self.random = random.Random(seed)
        self.random_state_workload_python = None
        self.random_state_workload_numpy = None
        self.packed_generator_is_done = False
        
        print(f"# SFC TSS - Traffic Scheduling Simulator")
        print(f"# Version {__version__}")
        
        # initialize global fields
        class Props:
            def __init__(self):
                for property_class in Sim.reset_global_fields:
                    properties = property_class()
                    try:
                        name = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)', properties.__class__.__qualname__)[-2]
                        name_of_props = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name).lower()
                    except IndexError as e:
                        print(f'error while resetting properties for {property_class}, '
                              f'{e}: {traceback.format_exc()}')
                        raise e
                    # print(f'::reset properties for "sim.{name_of_props}"')
                    setattr(self, name_of_props, properties)
            
            def __str__(self):
                return pprint.pformat(vars(self))
        
        self.props = Props()
    
    def schedule_event(self, event: BaseEvent):  # schedules an event -> simply add it to the list of events
        if self.ignore_all_future_schedule_event_attempts:
            return
        self.event_list.enqueue_event(event)
    
    def debug_get_sorted_event_list(self):
        return sorted(self.event_list.debug_get_all_remaining(), key=lambda x: x.time)
    
    def calculate_simple_statistics(self):
        successfully_delivered = (
                self.props.packet.statsPacketsSuccessfulProcessed - self.props.packet.counter_packet_after_workload_end_in_system_no_timeout)
        idle_time = 0
        for s in self.props.server.all_servers:
            idle_time += s.stats_idle_time
            if s.is_free():
                idle_time += self.currentTime - s.stats_last_time_idle
        idle_time_ratio = 0 if self.currentTime == 0 else round(
            100 * (idle_time / len(self.props.server.all_servers)) / self.currentTime, 4)
        
        self.stats = {'total_scheduling_attempts':
                          sum([s.scheduler.scheduling_attempts for s in self.props.sff.allSFFs.values()]),
                      'success_rate': self.props.packet.statsPacketsSuccessfulProcessed /
                                      self.props.packet.statsPacketsTotalCount,
                      'reject_rate': self.props.packet.statsPacketsRejectedSchedule /
                                     self.props.packet.statsPacketsTotalCount,
                      'service_quality': 0.0 if successfully_delivered == 0 else
                      1 - self.props.packet.statsRatiosQos / successfully_delivered,
                      'server_idle_time': idle_time,
                      'server_idle_time_ratio': idle_time_ratio
                      }
    
    def print_sim_snapshot(self, per_sfi_stats: bool = False):
        from sfctss import sanity
        print("-" * 20)
        print(f"Sim Snapshot at time:{self.currentTime}µs ({datetime.datetime.now()})")
        sanity.print_sim_snapshot(self, per_sfi_stats=per_sfi_stats)
        print("-" * 20)
        self.event_list.print_snapshot()
        print('#' * 20)
    
    def run_one_step(self, print_status=False, do_not_stop=False):
        if not self.run:
            self.run = True
        
        if self.event_list.len() > 0:
            e = self.event_list.pop_next()
            while (e.ignoreWhenFinished and not do_not_stop) and self.event_list.len() > 0:
                e = self.event_list.pop_next()
            self.current_event = e
            if e.ignoreWhenFinished and not do_not_stop:
                return False
            assert e.time >= self.currentTime
            self.currentTime = e.time  # update time to the time of the current event
            if print_status:
                print(f'-- @{self.currentTime}')
            e.process_event()
            return True
        else:
            return False
    
    def run_sim(self, max_sim_time: int = -1, show_progress: bool = True,
                interactive=False, ui=False,
                stop_simulation_when_workload_is_over=False):  # method to run the simulator
        assert not self.run
        assert not (ui and show_progress)
        if self.packet_generator is None:
            self.packed_generator_is_done = True
        self.run = True
        # we need to check this, in case we have only one SFF, so there is no
        # link setup
        for f in self.simulation_start_hooks:
            f(self)
        ticks = 0
        self.start_time = time.time()
        peek_last = self.event_list.peek_last()
        if self.workload_end_time == 0:
            self.workload_end_time = peek_last.time
        self.event_list.print_snapshot()
        print("start simulation @{0}µs till max SimTime:{1}".format(
            str(self.start_time), str(max_sim_time)))
        
        still_during_workload = True
        
        if self.currentTime != 0:
            raise NameError("this Implementation uses many class instances, \
            hence, your cannot run multiple simulations w/o restarting python")
        
        if ui:
            from . import ui
        
        try:
            # main loop
            
            while self.event_list.get_number_of_relevant_events() > 0 and (
                    max_sim_time == -1 or max_sim_time >= self.currentTime):
                ticks += 1  # count ticks for statistics
                if show_progress:
                    if ticks % 4096 == 0:
                        self.update_sim_status_oneliner(ticks=ticks)
                if ui:
                    if ticks % 4096 == 0:
                        self.update_ui(ticks)
                        if interactive:
                            input("#" * 40 + "\nPress Enter to continue...")
                
                e = self.event_list.pop_next()
                assert e != self.current_event
                
                if e.time > self.workload_end_time and still_during_workload and stop_simulation_when_workload_is_over:
                    # put the event back
                    self.event_list.enqueue_event(e)
                    
                    # we are now done with the workload
                    # inform all statistic writer that workload is over
                    self.ignore_all_future_schedule_event_attempts = True
                    
                    # check all packets which are still in the system,
                    # and for each, check the current time in system,
                    # if higher than deadline, consider as drop,
                    #   drop, we fire p.drop, so that we get all statistics
                    # if lower, consider as successful delivery
                    #   no drop, but we cannot account into the detailed statistics.
                    #   so we simply decrease the number of packets in system,
                    #   increase successful by 1, but we do not fire
                    #   p.done, so that the avg counter are not affected
                    
                    def check_remaining_packets():
                        for callback in self.stop_sim_catch_all_packets_hooks:
                            for packet in callback(self):
                                yield packet
                        
                        while self.event_list.len() > 0:
                            event = self.event_list.pop_next()
                            if isinstance(event, PacketHoldingEvent):
                                event.update_packet_time_tracking()
                                yield event.inner_packet
                    
                    p_finder = check_remaining_packets()
                    for p in p_finder:
                        p.handle_stop_simulation()
                    
                    for f in self.workload_over_hooks:
                        f(self)
                    
                    self.update_sim_status_oneliner(ticks, newline=True)
                    print(
                        "simulator reaches end of workload @{0}µs stop simulation ".format(
                            self.currentTime))
                    
                    break
                
                self.current_event = e
                self.currentTime = e.time  # update time to the time of the current event
                if not e.ignoreWhenFinished:
                    self.lastRelevantTime = self.currentTime
                
                if self.DEBUG:
                    print(f"Simulation tick @time {self.currentTime} with event {e}")
                
                e.process_event()  # process this event, regardless of which event it is
                
                if self.event_list.get_number_of_relevant_events() == 0:
                    self.update_sim_status_oneliner(ticks, newline=True)
                    print(
                        "simulator is done @{0}µs because of empty event stack of relevant events ".format(
                            self.currentTime))
                    
                    break
                
                if interactive and not ui:
                    self.print_sim_snapshot()
                    input("#" * 40 + "\nPress Enter to continue...")
            
            self.ignore_all_future_schedule_event_attempts = True
            
            if self.event_list.get_number_of_relevant_events() > 0:
                print(f"\nsimulator stops @{self.currentTime} because of simulation " +
                      f"reached given max sim time {max_sim_time}")
            
            # process all events which are still in the list of events, but do not belong to relevant events (e.g.
            # statistics)
            if self.event_list.len() > 0 and self.event_list.get_number_of_relevant_events() == 0:
                print(f"Finally process all pending statistic events")
                while self.event_list.len() > 0:
                    self.event_list.pop_next().process_event()
            
            for f in self.simulation_done_hooks:
                f(self)
        
        except BaseException as e:
            print(
                "#" *
                40 +
                "\n" +
                "#" *
                40 +
                f"\nsomething went wrong @{self.currentTime}, {self.event_list.len()} events "
                f"try to write all statistics to disk\n")
            self.print_sim_snapshot()
            raise e
        finally:
            pass
        
        end_time = time.time()
        time_passed = (end_time - self.start_time)
        performance = ticks / time_passed
        print("Simulation done with a total of {0} ticks, ~{1} ticks/s, {2}x speedup ({3}s simulated in {4}s)".format(
            str(ticks), str(int(round(performance, 0))), str(
                round(self.lastRelevantTime / time_passed / 1000000, 2)),
            str(self.lastRelevantTime / 1000000), str(time_passed)))
        
        self.calculate_simple_statistics()
        print(self.stats)
        
        # clear events that are not important anymore
        # e.g., a statistic event might still be scheduled
        # we have to remove these events to check if there are some "packet"
        # events in the queue
        remaining_events = [e for e in self.event_list.debug_get_all_remaining() if not e.ignoreWhenFinished]
        
        if len(remaining_events) > 0:
            if stop_simulation_when_workload_is_over:
                print(f"simulation has still {self.event_list.get_number_of_relevant_events()} enqueued events," +
                      f" but we stopped the simulation")
            else:
                print("simulation has still {0} enqueued events".format(
                    str(len(remaining_events))))
    
    def update_sim_status_oneliner(self, ticks, newline=False):
        progress = 100 * self.currentTime // self.workload_end_time \
            if self.currentTime <= self.workload_end_time else -1
        time_passed = round(time.time() - self.start_time, 1) + 0.1
        performance = ticks // time_passed
        
        performance_recent = int((ticks - self.printing_progress_previous_ticks) //
                                 (time.time() - self.printing_progress_previous_time))
        
        bar = (10 - int(progress / 10)) if progress >= 0 else 0
        progress_text = f"{progress}%" if progress >= 0 else "workload finished"
        print(f"\r[{'#' * (10 - bar) + ' ' * bar}] {progress_text} "
              f"@{self.currentTime}µs/"
              f"{self.event_list.peek_last().time if self.event_list.get_number_of_relevant_events() > 0 else '/'} "
              f"({performance_recent} t/s avg: {performance} t/s in {int(time_passed)} s) "
              f"{' '.join([provider(self) for provider in self.simulation_one_liner_hooks])}",
              end="\n" if newline else '\r')
        self.printing_progress_previous_ticks = ticks
        self.printing_progress_previous_time = time.time()
    
    def update_ui(self, ticks, split_server_table=True):
        if self.currentTime == 0:
            return
        Sim.ui_update_hook(self, ticks, split_server_table)
    
    def register_packet_generator(self, packet_generator, fetch_all=False):
        assert self.packet_generator is None
        self.packet_generator = packet_generator
        
        self.workload_end_time = packet_generator.get_expected_workload_time()
        try:
            print(f"Ask packet generator for a slice of up to {EventList.length_of_a_slice} packets")
            for _ in range(EventList.length_of_a_slice):
                self.event_list.add_event_from_the_back(self.packet_generator.__next__())
            
            while fetch_all:
                self.event_list.add_event_from_the_back(self.packet_generator.__next__())
            
            # freezing random states when done
            self.random_state_workload_python = random.getstate()
            self.random_state_workload_numpy = np.random.get_state()
        except StopIteration:
            self.packed_generator_is_done = True
            print("packet gen is done")
        self.workload_end_time = max(self.workload_end_time, self.event_list.last_time_of_relevant_event)
    
    def ask_packet_generator_for_more_events(self, minimum_number_of_new_events: int,
                                             minimum_future_time: int):
        
        if not self.packed_generator_is_done and self.packet_generator is not None:
            try:
                random.setstate(self.random_state_workload_python)
                np.random.set_state(self.random_state_workload_numpy)
                
                print(f"\rloading @{self.currentTime} ", end="\r")
                
                gc.collect()
                
                time_before = self.event_list.last_time_of_relevant_event
                for _ in range(minimum_number_of_new_events):
                    self.event_list.add_event_from_the_back(self.packet_generator.__next__())
                expected_time = time_before + minimum_future_time
                while self.event_list.last_time_of_relevant_event < expected_time:
                    self.event_list.add_event_from_the_back(self.packet_generator.__next__())
            
            except StopIteration:
                self.packed_generator_is_done = True
                # print("packet gen is done")
            finally:
                self.random_state_workload_python = random.getstate()
                self.random_state_workload_numpy = np.random.get_state()
                
                self.workload_end_time = max(self.workload_end_time, self.event_list.last_time_of_relevant_event)
