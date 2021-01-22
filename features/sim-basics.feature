Feature: Simulator Basic Tests

  Background: a valid simulator setup
    Given an empty simulator setup

  Scenario Outline: very simple setup, single sff with <scheduler>, 3 SFIs, using for each SF type <server> SFIs each on its own server
    Given we have "1" SFFs using scheduler "<scheduler>"
    And we have latency class "0" of latency "0"
    And we have "3" sfi types and SFF-SFI connections use latency class "0"
    And we have "<server>" SFIs of type "0" running on "<server>" servers and "do not" share the server
    And we have "<server>" SFIs of type "1" running on "<server>" servers and "do not" share the server
    And we have "<server>" SFIs of type "2" running on "<server>" servers and "do not" share the server
    And we have a traffic class "0" with latency "1000000000" ns
    And we have a traffic class "1" with latency "1000000000" ns
    And we have a traffic class "2" with latency "1000000000" ns
    And we have a traffic class "0-1-2" with latency "1000000000" ns
    And we have for each traffic class "2" flows each with "10" packets
    When we let the simulation run till all processing is done
    Then no packet is still in the simulator
    Then success rate is in the range of "<expected_success_rate>" allow delta 0.1
    Then service quality is in the range of "<expected_service_quality>" allow delta 0.1
    Then the idle time of the servers is on avg below "<expected_max_idle_ratio>"%
    Then reject rate is in the range of "0" allow delta 0.03

    Examples: single SFI, single server
      | scheduler    | server | expected_success_rate | expected_service_quality | expected_max_idle_ratio |
      | Static       | 1      | 1                     | 0.9                      | 5.2                     |
      | GreedyOracle | 1      | 1                     | 0.9                      | 5.2                     |
      | GreedyLocal  | 1      | 1                     | 0.9                      | 5.2                     |
      | MPP          | 1      | 1                     | 0.9                      | 5.2                     |
      | DMPP         | 1      | 1                     | 0.9                      | 5.2                     |


    Examples: two SFIs per SF
      | scheduler    | server | expected_success_rate | expected_service_quality | expected_max_idle_ratio |
      | Static       | 2      | 1                     | 0.9                      | 29.6                    |
      | GreedyOracle | 2      | 1                     | 0.9                      | 17.2                    |
      | GreedyLocal  | 2      | 1                     | 0.9                      | 17.2                    |
      | MPP          | 2      | 1                     | 0.9                      | 9.7                     |
      | DMPP         | 2      | 1                     | 0.9                      | 9.7                     |

  Scenario Outline: start a med-size simulation with <scheduler> using <server> servers, and sff latency of <latency>
    Given we set config "server_capacity" to "100"
#    And we activate debug mode
    And we set config "sfi_rate" to "40"
    And we have "5" SFFs using scheduler "<scheduler>"
    And we have latency class "0" of latency "<latency>"
    And we connect all sff with each other using latency class "0"
    And we have "4" sfi types and SFF-SFI connections use latency class "0"
    And we have "3" SFIs of type "1" running on "<server>" servers and "do" share the server
    And we have "2" SFIs of type "2" running on "<server>" servers and "do" share the server
    And we have "3" SFIs of type "3" running on "<server>" servers and "do" share the server
    And we have a traffic class "1-2-3" with latency "1500000" ns
    And we have a traffic class "1-3" with latency "1500000" ns
    And we have a traffic class "3" with latency "500000" ns
    And we have for each traffic class "10" flows each with "10" packets
    And we have for each traffic class "100" flows each with "200" packets
    When we let the simulation run till all processing is done
    Then the idle time of the servers is on avg below "<expected_max_idle_ratio>"%
    Then no packet is still in the simulator
    Then success rate is in the range of "<expected_success_rate>" allow delta 0.02
    Then service quality is in the range of "<expected_service_quality>" allow delta 0.02
    Then reject rate is in the range of "0" allow delta 0.02

    Examples: Sparse resources
      | scheduler    | server | expected_success_rate | expected_service_quality | latency | expected_max_idle_ratio |
      | Static       | 3      | 0.10                  | 0.41                     | 100     | 0.03                    |
      | GreedyOracle | 3      | 0.11                  | 0.4                      | 100     | 0.06                    |
      | GreedyLocal  | 3      | 0.10                  | 0.41                     | 100     | 0.53                    |
      | MPP          | 3      | 0.13                  | 0.42                     | 100     | 0.49                    |
      | DMPP         | 3      | 0.10                  | 0.42                     | 100     | 0.1                     |

    Examples: more resources
      | scheduler    | server | expected_success_rate | expected_service_quality | latency | expected_max_idle_ratio |
      | Static       | 6      | 0.16                  | 0.42                     | 100     | 5.7                     |
      | GreedyOracle | 6      | 0.18                  | 0.41                     | 100     | 0.6                     |
      | GreedyLocal  | 6      | 0.16                  | 0.42                     | 100     | 6                       |
      | MPP          | 6      | 0.28                  | 0.39                     | 100     | 0.43                    |
      | DMPP         | 6      | 0.20                  | 0.39                     | 100     | 3                       |

    Examples: more resources and no latency
      | scheduler    | server | expected_success_rate | expected_service_quality | latency | expected_max_idle_ratio |
      | Static       | 6      | 0.16                  | 0.42                     | 0       | 5.7                     |
      | GreedyOracle | 6      | 0.18                  | 0.41                     | 0       | 0.5                     |
      | GreedyLocal  | 6      | 0.16                  | 0.42                     | 0       | 6.1                     |
      | MPP          | 6      | 0.27                  | 0.39                     | 0       | 0.09                    |
      | DMPP         | 6      | 0.23                  | 0.41                     | 0       | 3.7                     |

    Examples: over provisioned
      | scheduler    | server | expected_success_rate | expected_service_quality | latency | expected_max_idle_ratio |
      | Static       | 25     | 0.30                  | 0.39                     | 100     | 69                      |
      | GreedyOracle | 25     | 0.30                  | 0.39                     | 100     | 69                      |
      | GreedyLocal  | 25     | 0.25                  | 0.44                     | 100     | 76                      |
      | MPP          | 25     | 0.30                  | 0.39                     | 100     | 69                      |
      | DMPP         | 25     | 0.25                  | 0.44                     | 100     | 76                      |


  Scenario Outline: start a simulation with different latencies and more chains with <scheduler> using <server> servers, and within site latency of <within_site_latency>, inter site latency <inter_site_latency>, but
    Given we set config "server_capacity" to "100"
#    And we activate debug mode
    And we set config "sfi_rate" to "300"
    And we set config "allow_up_to_x_packets_underway_per_server" to "<packet_on_wire>"
    And we have "6" SFFs using scheduler "<scheduler>"
    And we have latency class "0" of latency "<within_site_latency>"
    And we have latency class "1" of latency "<inter_site_latency>"
    And we connect all sff with each other using latency class "1"
    And we have "5" sfi types and SFF-SFI connections use latency class "0"
    And we have "<server>" SFIs of type "1" running on "<server>" servers and "do" share the server
    And we have "<server>" SFIs of type "2" running on "<server>" servers and "do" share the server
    And we have "<server>" SFIs of type "3" running on "<server>" servers and "do" share the server
    And we have "<server>" SFIs of type "4" running on "<server>" servers and "do" share the server
    And we have a traffic class "1-2-3-4" with latency "300000" ns
    And we have a traffic class "1-3" with latency "200000" ns
    And we have a traffic class "1" with latency "80000" ns
    And we have a traffic class "2" with latency "80000" ns
    And we have a traffic class "3-4-1-2" with latency "300000" ns
    # also add a chain with a SF type of no running SFI
    And we have a traffic class "1-5" with latency "100000" ns
    And we have a traffic class "5-1" with latency "100000" ns
    And we have for each traffic class "10" flows each with "10" packets
    And we have for each traffic class "30" flows each with "200" packets
    When we let the simulation run till all processing is done
    Then no packet is still in the simulator
    Then success rate is in the range of "<expected_success_rate>" allow delta 0.02
    Then service quality is in the range of "<expected_service_quality>" allow delta 0.02
    Then the idle time of the servers is on avg below "<expected_max_idle_ratio>"%
    Then reject rate is in the range of "<expected_reject_rate>" allow delta 0.02

    Examples: no latency
      | scheduler    | server | expected_success_rate | expected_reject_rate | expected_service_quality | packet_on_wire | within_site_latency | inter_site_latency | expected_max_idle_ratio |
      | Static       | 10     | .55                   | .14                  | .44                      | 1              | 0                   | 0                  | 1                       |
      | GreedyOracle | 10     | .55                   | .14                  | .44                      | 1              | 0                   | 0                  | 1                       |
      | GreedyLocal  | 10     | .44                   | .18                  | .47                      | 1              | 0                   | 0                  | 23                      |
      | MPP          | 10     | .70                   | .28                  | .52                      | 1              | 0                   | 0                  | 1                       |
      | DMPP         | 10     | .45                   | .28                  | .52                      | 1              | 0                   | 0                  | 30                      |

    Examples: with SFI-SFF latency
      | scheduler    | server | expected_success_rate | expected_reject_rate | expected_service_quality | packet_on_wire | within_site_latency | inter_site_latency | expected_max_idle_ratio |
      | Static       | 10     | .56                   | .15                  | .38                      | 2500           | 5000                | 0                  | 9                       |
      | GreedyOracle | 10     | .52                   | .16                  | .44                      | 2500           | 5000                | 0                  | 20                      |
      | GreedyLocal  | 10     | .41                   | .21                  | .42                      | 2500           | 5000                | 0                  | 37                      |
      | MPP          | 10     | .56                   | .15                  | .39                      | 2500           | 5000                | 0                  | 7                       |
      | DMPP         | 10     | .50                   | .21                  | .38                      | 2500           | 5000                | 0                  | 22                      |

    Examples: with SFF-SFF latency
      | scheduler    | server | expected_success_rate | expected_reject_rate | expected_service_quality | packet_on_wire | within_site_latency | inter_site_latency | expected_max_idle_ratio |
      | Static       | 10     | .35                   | .16                  | .22                      | 5              | 0                   | 50000              | 36                      |
      | GreedyOracle | 10     | .40                   | .19                  | .28                      | 5              | 0                   | 50000              | 33                      |
      | GreedyLocal  | 10     | .39                   | .18                  | .29                      | 5              | 0                   | 50000              | 35                      |
      | MPP          | 10     | .33                   | .15                  | .10                      | 2500           | 0                   | 50000              | 39                      |
      | DMPP         | 10     | .41                   | .19                  | .30                      | 2500           | 0                   | 50000              | 30                      |

    Examples: with SFI-SFF and SFF-SFF latency
      | scheduler    | server | expected_success_rate | expected_reject_rate | expected_service_quality | packet_on_wire | within_site_latency | inter_site_latency | expected_max_idle_ratio |
      | Static       | 10     | .28                   | .16                  | .17                      | 2500           | 5000                | 50000              | 41                      |
      | GreedyOracle | 10     | .31                   | .21                  | .26                      | 2500           | 5000                | 50000              | 43                      |
      | GreedyLocal  | 10     | .33                   | .19                  | .24                      | 2500           | 5000                | 50000              | 39                      |
      | MPP          | 10     | .15                   | .14                  | .07                      | 2500           | 5000                | 50000              | 46                      |
      | DMPP         | 10     | .39                   | .20                  | .24                      | 2500           | 5000                | 50000              | 32                      |
