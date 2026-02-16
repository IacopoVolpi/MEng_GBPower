# SPDX-FileCopyrightText: : 2024 Lukas Franken
#
# SPDX-License-Identifier: MIT


rule summarize_bmu_revenues:
    input:
        bids="data/base/{day}/bids.csv",
        offers="data/base/{day}/offers.csv",
        cfd_strike_prices="data/prerun/cfd_strike_prices.csv",
        roc_values="data/prerun/roc_values.csv",
        network_nodal="results/{day}/network_{ic}_s_nodal_solved.nc",
        network_national="results/{day}/network_{ic}_s_national_solved.nc",
        network_national_redispatch="results/{day}/network_{ic}_s_national_solved_redispatch.nc",
        network_zonal="results/{day}/network_{ic}_s_zonal_solved.nc",
        network_zonal_redispatch="results/{day}/network_{ic}_s_zonal_solved_redispatch.nc",
        default_balancing_prices=lambda wildcards: 'data/prerun/balancing_prices/{year}-week{week}.csv'.format(
            year=datetime.strptime(wildcards.day, '%Y-%m-%d').year,
            week=str(datetime.strptime(wildcards.day, '%Y-%m-%d').isocalendar()[1]).zfill(2)
        ),
    output:
        bmu_revenues_nodal="results/{day}/bmu_revenues_{ic}_nodal.csv",
        bmu_revenues_zonal="results/{day}/bmu_revenues_{ic}_zonal.csv",
        bmu_revenues_national="results/{day}/bmu_revenues_{ic}_national.csv",
        bmu_dispatch_nodal="results/{day}/bmu_dispatch_{ic}_nodal.csv",
        bmu_dispatch_zonal="results/{day}/bmu_dispatch_{ic}_zonal.csv",
        bmu_dispatch_national="results/{day}/bmu_dispatch_{ic}_national.csv",
        bmu_revenues_detailed_national="results/{day}/bmu_revenues_detailed_{ic}_national.csv",
        bmu_revenues_detailed_nodal="results/{day}/bmu_revenues_detailed_{ic}_nodal.csv",
        bmu_revenues_detailed_zonal="results/{day}/bmu_revenues_detailed_{ic}_zonal.csv",
        gb_total_load="results/{day}/gb_total_load_{ic}.csv",
    resources:
        mem_mb=1500,
    log:
        "../logs/system_cost/{day}_{ic}.log",
    conda:
        "../envs/environment.yaml",
    script:
        "../scripts/summarize_bmu_revenues.py"


rule summarize_system_cost:
    input:
        bids="data/base/{day}/bids.csv",
        offers="data/base/{day}/offers.csv",
        cfd_strike_prices="data/prerun/cfd_strike_prices.csv",
        roc_values="data/prerun/roc_values.csv",
        network_nodal="results/{day}/network_{ic}_s_nodal_solved.nc",
        network_national="results/{day}/network_{ic}_s_national_solved.nc",
        network_national_redispatch="results/{day}/network_{ic}_s_national_solved_redispatch.nc",
        network_zonal="results/{day}/network_{ic}_s_zonal_solved.nc",
        network_zonal_redispatch="results/{day}/network_{ic}_s_zonal_solved_redispatch.nc",
        bmu_revenues_nodal="results/{day}/bmu_revenues_{ic}_nodal.csv",
        bmu_revenues_zonal="results/{day}/bmu_revenues_{ic}_zonal.csv",
        bmu_revenues_national="results/{day}/bmu_revenues_{ic}_national.csv",
    output:
        marginal_prices="results/{day}/marginal_prices_{ic}.csv",
        system_cost_summary="results/{day}/system_cost_summary_{ic}.csv",
    resources:
        mem_mb=1500,
    log:
        "../logs/system_cost/{day}_{ic}.log",
    conda:
        "../envs/environment.yaml",
    script:
        "../scripts/summarize_system_cost.py"


rule summarize_frontend_data:
    input:
        bmu_revenues_zonal="results/{day}/bmu_revenues_{ic}_zonal.csv",
        bmu_revenues_national="results/{day}/bmu_revenues_{ic}_national.csv",
        nat_who="results/{day}/network_flex_s_national_solved.nc",
        nat_bal="results/{day}/network_flex_s_national_solved_redispatch.nc",
        zon_who="results/{day}/network_flex_s_zonal_solved.nc",
        zon_bal="results/{day}/network_flex_s_zonal_solved_redispatch.nc",
        roc_values="data/prerun/roc_values.csv",
        cfd_strike_prices="data/prerun/cfd_strike_prices.csv",
        system_cost_summary="results/{day}/system_cost_summary_{ic}.csv",
        gb_shape="data/gb_shape.geojson",
        bids="data/base/{day}/bids.csv",
        offers="data/base/{day}/offers.csv",
        default_balancing_prices=lambda wildcards: 'data/prerun/balancing_prices/{year}-week{week}.csv'.format(
            year=datetime.strptime(wildcards.day, '%Y-%m-%d').year,
            week=str(datetime.strptime(wildcards.day, '%Y-%m-%d').isocalendar()[1]).zfill(2)
        ),
    output:
        frontend_revenues="frontend/{day}/revenues_{ic}.csv",
        frontend_dispatch="frontend/{day}/dispatch_{ic}.csv",
        frontend_dispatch_intercon="frontend/{day}/dispatch_flex_{ic}_intercon.csv",
        frontend_marginal_costs="frontend/{day}/marginal_costs_{ic}.csv",
        frontend_thermal_dispatch="frontend/{day}/thermal_dispatch_{ic}.csv",
    resources:
        mem_mb=1500,
    log:
        "../logs/frontend_data/{day}_{ic}.log",
    conda:
        "../envs/environment.yaml",
    script:
        "../scripts/summarize_frontend_data.py"

rule IV_distribute_balancing_volumes:
    input:
        network_redispatch="results/{day}/network_{ic}_s_national_solved_redispatch.nc",
        transmission_boundaries="data/transmission_boundaries.yaml",
        boundary_flow_constraints="data/base/{day}/boundary_flow_constraints.csv",
        bids="data/base/{day}/bids.csv",
        bmus="data/prerun/prepared_bmus.csv",
    output:
        per_constraint_balancing="results/{day}/IV_balancing_volume_per_constraint_{ic}.csv",
        detailed_breakdown="results/{day}/IV_per_constraint_balancing_breakdown_{ic}.csv",
    log:
        "../logs/distribute_balancing/{day}_{ic}.log"
    resources:
        mem_mb=2000,
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/IV_distribute_balancing_volumes.py"

rule IV_alternative_distribute_balancing_volumes:
    input:
        network_nodal="results/{day}/network_{ic}_s_nodal_solved.nc",
        network_wholesale="results/{day}/network_{ic}_s_national_solved.nc",
        transmission_boundaries="data/transmission_boundaries.yaml",
    output:
        per_constraint_balancing="results/{day}/IV_alternative_balancing_volume_per_constraint_{ic}.csv",
        detailed_breakdown="results/{day}/IV_alternative_per_constraint_breakdown_{ic}.csv",
    log:
        "../logs/wholesale_flows/{day}_{ic}.log"
    resources:
        mem_mb=1000,
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/IV_alternative_distribute_balancing_volumes.py"


rule IV_2_distribute_balancing_volume:
    input:
        network_wholesale="results/{day}/network_{ic}_s_national_solved.nc",
        network_redispatch="results/{day}/network_{ic}_s_national_solved_redispatch.nc",
        bmu_classification="data/prerun/bmu_constraint_classification.csv",
    output:
        aggregated_by_zone="results/{day}/IV_2_dispatch_changes_by_zone_{ic}.csv",
        detailed_by_generator="results/{day}/IV_2_dispatch_changes_by_generator_{ic}.csv",
        aggregated_by_zone_and_type="results/{day}/IV_2_dispatch_changes_by_zone_and_type_{ic}.csv",
    log:
        "../logs/dispatch_changes/{day}_{ic}.log"
    resources:
        mem_mb=1500,
    conda:
        "../envs/environment.yaml"
    script:
        "../scripts/IV_2_distribute_balancing_volume.py"

rule IV_clear_balancing_market:
    input:
        network_wholesale="results/{day}/network_{ic}_s_national_solved.nc",
        network_redispatch="results/{day}/network_{ic}_s_national_solved_redispatch.nc",
        bmu_classification="data/prerun/bmu_constraint_classification.csv",
        submitted_bids="data/base/{day}/submitted_bids.csv",
        submitted_offers="data/base/{day}/submitted_offers.csv",
    output:
        settlement_summary="results/{day}/IV_clearing_settlement_summary_{ic}.csv",
        accepted_actions="results/{day}/IV_clearing_accepted_actions_{ic}.csv",
        uncleared_summary="results/{day}/IV_clearing_uncleared_summary_{ic}.csv",
    resources:
        mem_mb=2000,
    script:
        "../scripts/IV_clear_balancing_market.py"