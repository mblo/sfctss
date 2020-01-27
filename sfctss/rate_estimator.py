#!/usr/bin/env python3
# coding=utf-8
from array import array
from typing import List, Dict

from .simulator import Sim
from .events import BaseEvent


class RateEstimator(object):
    @Sim.register_reset_global_fields
    class Props:
        def __init__(self):
            self.all_estimators: Dict[int, List['RateEstimator']] = dict()
    
    def __init__(self, sim: Sim, period: int = 500000):
        self.period = int(period)
        self.sim = sim
        estimator_props = sim.props.rate_estimator
        assert self.period > 0
        
        if self.period not in estimator_props.all_estimators:
            estimator_props.all_estimators[self.period] = []
            sim.schedule_event(RateEstimatorUpdateEvent(self))
        estimator_props.all_estimators[self.period].append(self)
    
    def packet_arrival(self):
        pass
    
    def get_estimated_rate(self):
        pass
    
    def update_rate(self):
        pass


class RateEstimatorUpdateEvent(BaseEvent):
    
    def __init__(self, estimator: RateEstimator):
        super().__init__(estimator.sim.currentTime + estimator.period)
        self.estimator = estimator
        self.ignoreWhenFinished = True
    
    def process_event(self):
        for rate_estimator in self.estimator.sim.props.rate_estimator.all_estimators[self.estimator.period]:
            rate_estimator.update_rate()
        self.estimator.sim.schedule_event(RateEstimatorUpdateEvent(estimator=self.estimator))


class EWMA(RateEstimator):
    
    def __init__(self, sim: Sim, alpha: float = 0.06, period: int = 5000, buckets: int = 20):
        super().__init__(sim, period=period)
        self.expected_size = buckets
        self.value = 0
        self.alpha = alpha
        assert 1 > self.alpha > 0
        
        assert 0 < self.expected_size < 100000
        self.buckets = array('I', [0])
        self.pos_current_bucket = 0
    
    def packet_arrival(self):
        self.value += 1
    
    def get_estimated_rate(self):
        v = float(self.buckets[self.pos_current_bucket])
        length = len(self.buckets)
        next_pos = (self.pos_current_bucket + 1) % length
        while next_pos != self.pos_current_bucket:
            v = v * (1 - self.alpha) + self.alpha * float(self.buckets[next_pos])
            next_pos = (next_pos + 1) % length
        return v / (self.period / 1000000)
    
    def update_rate(self):
        if len(self.buckets) < self.expected_size:
            self.buckets.append(self.value)
            self.pos_current_bucket = len(self.buckets) - 1
        else:
            self.pos_current_bucket = (self.pos_current_bucket + 1) % len(self.buckets)
            self.buckets[self.pos_current_bucket] = self.value
        self.value = 0


class DRE(RateEstimator):
    """Discounting Rate Estimator (DRE)
    should result in: X is proportional to the rate of traffic
    more precisely, if the traffic rate is R, then X ≈ R · τ, where τ = Tdre/α
    https://people.csail.mit.edu/alizadeh/papers/conga-techreport.pdf"""
    
    def __init__(self, sim: Sim, alpha: float = 0.125, period: int = 500000):
        super().__init__(sim, period)
        self.value = 0
        self.alpha = alpha
        assert 1 > self.alpha > 0
        
        self.tau = (1000000 / self.period) / self.alpha
    
    def packet_arrival(self):
        self.value += 1
    
    def update_rate(self):
        self.value *= (1 - self.alpha)
    
    def get_dre(self):
        return self.value
    
    def get_estimated_rate(self):
        return self.value / self.tau
    
    def get_congestion_metric(self, capacity):
        return self.value / (self.tau * capacity)
    
    def get_tau(self):
        return self.tau
