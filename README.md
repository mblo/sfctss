![Package & Test 3.6](https://github.com/mblo/sfctss/workflows/Package%20and%20Test%203.6/badge.svg)
![Package & Test 3.7](https://github.com/mblo/sfctss/workflows/Package%20and%20Test%203.7/badge.svg)
![Package & Test 3.8](https://github.com/mblo/sfctss/workflows/Package%20and%20Test%203.8/badge.svg)
![Package & Test PyPy3](https://github.com/mblo/sfctss/workflows/Package%20and%20Test%20PyPy3/badge.svg)

# SFC TSS - Traffic Scheduling Simulator

**SFC TSS** - **S**ervice **F**unction **C**hain (SFC) **t**raffic **s**cheduling **s**imulator - is an Apache2 licensed packet-level discrete event simulator.
SFC TSS simulates the SFC traffic scheduling problem as described in our paper ["Letting off STEAM: Distributed Runtime Traffic Scheduling for Service Function Chaining"](https://linwang.info/docs/infocom20-steam.pdf).
We refer to this paper for more information on [RFC 7665](https://tools.ietf.org/html/rfc7665) and the features of the simulator.
SFC TSS simulates scenarios in compliance with [RFC 7665](https://tools.ietf.org/html/rfc7665) including link latencies, packet handling at the various SFC components like SFFs, SFIs and server.

If you use SFC TSS in your research, please cite our paper:

```
@inproceedings{bloecher2020steam,
  title={Letting off STEAM: Distributed Runtime Traffic Scheduling for Service Function Chaining},
  author={Blöcher, Marcel and Khalili, Ramin and Wang, Lin and Eugster, Patrick},
  booktitle={IEEE INFOCOM 2020 - IEEE Conference on Computer Communications},
  pages={824-833},
  doi = {10.1109/INFOCOM41043.2020.9155404},
  year={2020}
}
```

# Getting Started


## Install

To setup your environment use either [PyPy3](https://pypy.org) or [Python3](https://www.python.org).
We **highly** recommend you to use PyPy3.


```
pip install sfctss
```

Tested with Ubuntu 19.10 / Mac OS 10.15.

## Experiment setup

SFC TSS provides the essential parts of a SFC traffic simulation and provides many options to configure an experiment. 

A minimal configuration requires the following steps

```Python
import numpy as np
import random

import sfctss
from sfctss.scheduler.examples import LoadUnawareRoundRobinScheduler

rand = random.Random()
rand.seed(42) # seed the experiment

sim = sfctss.simulator.Sim(seed=rand.randint(0,1000000))


# create a link latency distribution that is used to connect between SFFs-SFIs
LATENCY_SFF_SFI = 1
sfctss.model.SFF.setup_latency_distribution(sim=sim, 
                                            id=LATENCY_SFF_SFI, 
                                            values=np.random.poisson(500, 5000)) # 3000µs

# create a link latency distribution that is used to connect between SFFs-SFFs
LATENCY_SFF_SFF = 2
sfctss.model.SFF.setup_latency_distribution(sim=sim, 
                                            id=LATENCY_SFF_SFF,
                                            values=np.random.poisson(3000, 5000)) # 3000µs

# initialize data structures, configure number of sf types
sfctss.model.SFI.init_data_structure(sim=sim, 
                                     number_of_sf_types=1, 
                                     latency_provider_sff_sfi=LATENCY_SFF_SFI)

# at least one SFF with a scheduler instance
scheduler_a = LoadUnawareRoundRobinScheduler(sim=sim,
                                             incremental=True, # schedule one step of a chain per scheduling attempt
                                             oracle=True) # scheduler has a global view (all sites)
sff_a = sfctss.model.SFF(sim=sim, 
                         scheduler=scheduler_a)

# at least one Server with a SFI that is connected to the SFF
server = sfctss.model.Server(sim=sim, 
                             processing_cap=120, 
                             cpu_policy=sfctss.model.ServerCpuPolicy.one_at_a_time)
server.add_sfi(of_type=1, 
               with_sff_id=sff_a.id)

# do the same for ssf_b ...
scheduler_b = None # ...
sff_b = None # ...

# configure connections between SFFs
sfctss.model.SFF.setup_connection(sim=sim, 
                                  source_id=sff_a.id, 
                                  destination_id=sff_b.id,
                                  bw_cap=100000,
                                  latency_provider=LATENCY_SFF_SFF,
                                  bidirectional=True)           

# configure processing speed of sf types
# the rate gives the number of packets a sfi of this sf type can process in 1 s when using 1 cpu share
sfctss.model.SFI.setup_sf_processing_rate_per_1s(sim=sim, 
                                                 of_type=1, 
                                                 with_mu=100)

# create at least one packet generator (which could also replay a pcap)
wl_config = sfctss.workload.SyntheticWorkloadGenerator.get_default_config()
wl_gen = sfctss.workload.SyntheticWorkloadGenerator(sim=sim,
                                                    workload_rand=rand,
                                                    config=wl_config)
sim.register_packet_generator(packet_generator=wl_gen,
                              fetch_all=False)

# finally, start simulation
sim.run_sim(show_progress=True, # print progress on bash
            interactive=False, # no interactive mode
            max_sim_time=1500000, # we stop after 1.5s
            ui=False, # do not show bash ui
            stop_simulation_when_workload_is_over=True) # stop when max_sim_time is done or when workload is done 

```

We refer to the example `example/main.py` for an example how to use SFC TSS.

You may also want to create your own scheduler. Simply subclass `BaseScheduler`.
Check example schedulers in `sfctss.scheduler.examples` for more information. 

You may also want to create a custom workload provider like a pcap replay. Your workload provider must implement `WorkloadGenerator`.

## Full Example

Start running the example experiment 

```Bash
./example/main.py --show-progress
```

or with more debugging output or activated statistics dumps

```Bash
# show progress, write csv logs, active some of the statistics
./example/main.py --show-progres --write-statistics output --statistics-server --statistics-polling-sfi --statistics-latency-cdf-buckets 50

# with more verbose bash ui
./example/main.py --show-ui
 
# debugging mode
./example/main.py -v --interactive 
```

The example provides more options...

```
usage: main.py [-h] [-v] [--sim-time SIM_TIME] [--no-workload-reloading] [--dry] [--show-progress] [--interactive] [--show-ui]
               [--write-statistics STATISTICS_FILENAME] [--statistics-overview] [--statistics-packets] [--statistics-server] [--statistics-polling-sfi]
               [--statistics-polling-sff] [--statistics-polling-server] [--statistics-polling-overview]
               [--statistics-polling-interval STATISTICS_POLLING_INTERVAL] [--statistics-latency-cdf-buckets STATISTICS_PACKETS_CDF_BUCKETS]
               [--dump-full-workload]

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         verbose output
  --sim-time SIM_TIME   set simulation time (in ns)
  --no-workload-reloading
                        if set, load full workload before simulation starts
  --dry                 do not run the simulation, but test everything if it is functional
  --show-progress       shows the progress during running the simulation
  --interactive         run each simulation tick one after another
  --show-ui             shows a simple bash-ui when running the simulation
  --write-statistics STATISTICS_FILENAME
                        if filename is set, activate statistics
  --statistics-overview
                        activate overview statistics
  --statistics-packets  activate packet statistics
  --statistics-server   activate server statistics
  --statistics-polling-sfi
                        activate sfi polling statistics
  --statistics-polling-sff
                        activate sff polling statistics
  --statistics-polling-server
                        activate server polling statistics
  --statistics-polling-overview
                        activate overview polling statistics
  --statistics-polling-interval STATISTICS_POLLING_INTERVAL
                        set statistics polling interval (in ns)
  --statistics-latency-cdf-buckets STATISTICS_PACKETS_CDF_BUCKETS
                        activate cdf of packet latencies; set # of buckets for cdf, e.g., 50
  --dump-full-workload  dumps full workload (full packet dump)
```


# Manual Installation / Contribute

Run one of the following lines 

```Bash
./bootstrap-deps-pypy.sh # recommended option
./bootstrap-deps.sh # fallback with standard Python
```

to setup your environment either with [PyPy3](https://pypy.org) or [Python3](https://www.python.org).
We **highly** recommend you to use PyPy3.

Simply load your python environment by calling one of the following lines

```Bash
source env-pypy/bin/activate
source env/bin/activate
```

We are happy for all kind of contributions, including bug fixes and additional features.


# Acknowledgement

This work has been co-funded by the Federal Ministry of Education and Research ([BMBF](https://www.bmbf.de)) Software Campus grant 01IS17050, the German Research Foundation ([DFG](https://www.dfg.de)) as part of the projects B2 and C7 in the Collaborative Research Center (CRC) 1053 “MAKI” and DFG grant 392046569 (61761136014 for NSFC), and the EU H2020 program under grant ICT-815279 “5G-VINNI” and ERC grant FP7-617805 “LiveSoft”. 
