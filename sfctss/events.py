#!/usr/bin/env python3
# coding=utf-8
from sortedcontainers import SortedKeyList


class BaseEvent(object):
    def __init__(self, at_time):  # absolute time of event (from start of simulation)
        # we don't allow events with floating time, since there might be a precision error -> may affect
        # deterministic property int, because float may introduce rounding errors and is more expensive
        self.time = int(at_time)
        self.ignoreWhenFinished = False
    
    def process_event(self):
        pass
    
    def __str__(self):
        return f"{self.__class__.__name__}({self.time})"
    
    def __repr__(self):
        return str(self)


class PacketHoldingEvent(BaseEvent):
    
    def __init__(self, at_time, inner_packet):
        super().__init__(at_time)
        self.inner_packet = inner_packet
    
    def update_packet_time_tracking(self):
        raise NameError("this need to be implemented")


class EventList(object):
    length_of_a_slice = 800000
    
    def __init__(self, sim):
        self.current_list: SortedKeyList[BaseEvent] = SortedKeyList(key=lambda e: e.time)
        self.sim = sim
        self.number_of_events = 0
        self.number_of_relevant_events = 0
        self.last_popped_time = None
        self.last_time_of_relevant_event = None
    
    def debug_get_all_remaining(self):
        return self.current_list[:]
    
    def debug_access(self, relative_to_next_event) -> BaseEvent:
        assert relative_to_next_event >= 0
        return self.current_list[relative_to_next_event]
    
    # this is used when filling up the backlog with new events from the workload
    def add_event_from_the_back(self, event):
        self.enqueue_event(event)
    
    def enqueue_event(self, event: BaseEvent):
        self.number_of_events += 1
        if not event.ignoreWhenFinished:
            self.number_of_relevant_events += 1
            if self.last_time_of_relevant_event is None or self.last_time_of_relevant_event < event.time:
                self.last_time_of_relevant_event = event.time
        
        self.current_list.add(event)
    
    def len(self):
        return self.number_of_events
    
    def get_number_of_relevant_events(self):
        return self.number_of_relevant_events
    
    def pop_next(self) -> BaseEvent:
        item = self.current_list.pop(0)
        
        self.number_of_events -= 1
        if not item.ignoreWhenFinished:
            self.number_of_relevant_events -= 1
        assert self.last_popped_time is None or item.time >= self.last_popped_time
        self.last_popped_time = item.time
        
        if not self.sim.packed_generator_is_done and self.last_time_of_relevant_event - 500000 < self.last_popped_time:
            self.sim.ask_packet_generator_for_more_events(EventList.length_of_a_slice, 800000)
        
        return item
    
    def print_snapshot(self):
        print(f"Number of pending events: {self.number_of_events}")
        if len(self.current_list) > 0:
            for e in self.current_list[:5]:
                print(f"    {e}, {e.time}")
            print("...")
            for e in self.current_list[-5:]:
                print(f"    {e}, {e.time}")
    
    def peek_last(self):
        return self.current_list[-1]
