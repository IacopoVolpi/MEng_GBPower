# SPDX-FileCopyrightText: : 2024 Lukas Franken
#
# SPDX-License-Identifier: MIT



rule build_base:
    input:
        europe_day_ahead_prices="data/europe_day_ahead_prices_GBP.csv",
        flow_constraints=lambda wildcards: (
            f"data/prerun/flow_constraints_{wildcards.day[:4]}.csv"
            if wildcards.day[:4] != "2025" else
            f"data/prerun/flow_constraints_2024.csv"
        ),
    output:
        date_register="data/base/{day}/settlement_period_register.csv",
        boundary_flow_constraints="data/base/{day}/boundary_flow_constraints.csv",
        physical_notifications="data/base/{day}/physical_notifications.csv",
        maximum_export_limits="data/base/{day}/maximum_export_limits.csv",
        offers="data/base/{day}/offers.csv",
        bids="data/base/{day}/bids.csv",
        nemo_powerflow="data/base/{day}/nemo_powerflow.csv",
        day_ahead_prices="data/base/{day}/day_ahead_prices.csv",
        europe_day_ahead_prices="data/base/{day}/europe_day_ahead_prices.csv",
        europe_generation="data/base/{day}/europe_generation.csv",
    resources:
        mem_mb=4000,
    log:
        "../logs/base/{day}.log",
    conda:
        "../envs/environment.yaml",
    script:
        "../scripts/build_base.py"


rule IV_build_submitted_bids_offers:
    output:
        submitted_bids="data/base/{day}/submitted_bids.csv",
        submitted_offers="data/base/{day}/submitted_offers.csv",
    log:
        "../logs/submitted_bids_offers/{day}.log"
    resources:
        mem_mb=2000
    script:
        "../scripts/build_all_bm_actions.py"
