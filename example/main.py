#!/usr/bin/env python3
# coding=utf-8
import argparse

import config
from topology import run

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="verbose output")
    
    parser.add_argument("--sim-time", default=10000000, type=int, dest="sim_time",
                        help="set simulation time (in ns)")
    
    parser.add_argument("--no-workload-reloading", action="store_true",
                        help="if set, load full workload before simulation starts")
    parser.add_argument("--dry", action="store_true", default=False,
                        help="do not run the simulation, but test everything if it is functional")
    parser.add_argument("--show-progress", action='store_true', default=False,
                        help="shows the progress during running the simulation")
    parser.add_argument("--interactive", action='store_true', default=False,
                        help="run each simulation tick one after another")
    parser.add_argument("--show-ui", action='store_true', default=False,
                        help="shows a simple bash-ui when running the simulation")
    
    parser.add_argument("--write-statistics", type=str, dest="statistics_filename",
                        help="if filename is set, activate statistics")
    
    parser.add_argument("--statistics-overview", action='store_true', default=False, dest="statistics_overview",
                        help="activate overview statistics")
    parser.add_argument("--statistics-packets", action='store_true', default=False, dest="statistics_packets",
                        help="activate packet statistics")
    parser.add_argument("--statistics-server", action='store_true', default=False, dest="statistics_server",
                        help="activate server statistics")
    
    parser.add_argument("--statistics-polling-sfi", action='store_true', default=False, dest="statistics_polling_sfi",
                        help="activate sfi polling statistics")
    parser.add_argument("--statistics-polling-sff", action='store_true', default=False, dest="statistics_polling_sff",
                        help="activate sff polling statistics")
    parser.add_argument("--statistics-polling-server", action='store_true', default=False, dest="statistics_polling_server",
                        help="activate server polling statistics")
    parser.add_argument("--statistics-polling-overview", action='store_true', default=False, dest="statistics_polling_overview",
                        help="activate overview polling statistics")
    
    parser.add_argument("--statistics-polling-interval", default=50000, type=int, dest="statistics_polling_interval",
                        help="set statistics polling interval (in ns)")
    
    parser.add_argument("--statistics-latency-cdf-buckets", type=int, dest="statistics_packets_cdf_buckets",
                        help="activate cdf of packet latencies; set # of buckets for cdf, e.g., 50")
    
    parser.add_argument("--dump-full-workload", action='store_true', default=False,
                        help="dumps full workload (full packet dump)")
    
    args = parser.parse_args()
    
    run(config=config.template_default_parameters(sites=3),
        stop_simulation_after=args.sim_time,
        debug=args.verbose,
        show_ui=args.show_ui,
        show_ui_full=args.show_ui,
        show_progress=args.show_progress,
        run_interactive=args.interactive,
        no_workload_reloading=args.no_workload_reloading,
        dry_run=args.dry,
        statistics_packet_cdfs=args.statistics_packets_cdf_buckets,
        statistics_filename=None if args.statistics_filename is None else args.statistics_filename,
        statistics_overview=args.statistics_overview,
        statistics_packets=args.statistics_packets,
        statistics_server=args.statistics_server,
        statistics_workload=args.dump_full_workload,
        statistics_polling=args.statistics_polling_interval,
        statistics_polling_sfi=args.statistics_polling_sfi,
        statistics_polling_sff=args.statistics_polling_sff,
        statistics_polling_server=args.statistics_polling_server,
        statistics_polling_overview=args.statistics_polling_overview,
        )
