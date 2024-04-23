
/*
Consolidate metrics and reports for Sales Contest.
Sales representatives are allocated with Bounty reward points by each qualify activities. 
The first N person who reach certain Bounty Points will be rewarded with a prize. 

Thus, data need to track by date when the qualify event(activity) happens;
and also track by dealerships level -- this will provide clarity when QA data accuracy.
*/




CREATE TABLE Sales.Sales_Contest_Dashboard AS (
--==========================================================================
--Sales Contest Metrics
--RSM
--Metric #7: New E5+ A-Dealer (E5+: originated 5+ deals in a month)
--Feb Current Year (E5+) and Last Year (not E5+) -> count the dealership
--Mar Current Year (E5+) and Last Year (not E5+) -> count the dealership again

t_RSM_M7 AS
(
    SELECT
        'RSM' AS team,
        7 AS metric_id,
        'New E5+ A-Dealer' AS metric_name,

        rsm_territory_regions__c AS region,
        dealership_id,

        EXTRACT(MONTH FROM month) AS mm,

        MIN(CASE WHEN EXTRACT(YEAR FROM month) = 2024 THEN e5plus_date END) AS date, --First_E5plus_Date
        MAX(CASE WHEN EXTRACT(YEAR FROM month) = 2023 THEN month_tot ELSE 0 END) AS py_mm_fund,
        MAX(CASE WHEN EXTRACT(YEAR FROM month) = 2024 THEN month_tot ELSE 0 END) AS cy_mm_fund
    FROM
    (
        SELECT
            a.rsm_territory_regions__c,
            dealership_id,
            CAST(DATE_Trunc('month', funding_pending_date) AS DATE) AS month,
            funding_pending_date,

            SUM(count) OVER (PARTITION BY Dealership_id, Month ORDER BY funding_pending_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sum,
            SUM(count) OVER (PARTITION BY Dealership_id, Month) AS month_tot,

            CASE WHEN cum_sum >= 5 THEN Funding_Pending_Date END AS e5plus_Date

        FROM Sales.business_insights_data_mart AS a
        INNER JOIN sfdc.raw_sfdc_account_dyly_curr AS b
            ON a.dealership_id = b.dealership_id__c
        WHERE b.powersports_sales_tier__c LIKE 'A%'
            AND COALESCE(LOWER(vehicle_type), 'na') NOT IN ('mower', 'trailer', 'tractor', 'rv', 'automobile', 'na')  --incl PS veh type and 'Other'
            AND (
                    funding_pending_date BETWEEN '2024-02-01' AND '2024-03-31'
                    OR funding_pending_date BETWEEN '2023-02-01' AND '2023-03-31'
                )
            AND is_funded = 'Yes'
        ORDER BY 1,2,3,4
    ) AS t1
    GROUP BY 1,2,3,4,5,6
    HAVING py_mm_fund < 5 AND cy_mm_fund >=5
    ORDER BY 4,5,6
),

-----------------------------------------------------------------------------------
--RSM
--Metric #4: Active a New Flex Dealer
t_RSM_M4 AS
(
    SELECT
        'RSM' AS team,
        4 AS metric_id,
        'Active Flex Dealer' AS metric_name,

        rsm_territory_regions__c AS region,
        dealership_id,
        MIN(submitted_at) AS date
    FROM Sales.business_insights_data_mart_all_apps
    WHERE source_client = 'direct_to_consumer_flex'
        AND COALESCE(LOWER(vehicle_type), 'na') NOT IN ('mower', 'trailer', 'tractor', 'rv', 'automobile', 'na')  --incl PS veh type and 'Other'
        AND submitted_at <= '2024-03-31'
    GROUP BY 1,2,3,4,5
    HAVING date >= '2024-02-01'
),

-----------------------------------------------------------------------------------
--RSM
--Metric #5: New Active A-Dealer (no app since 1/1/2023)
--Regardless of vehicle type

t_RSM_M5 AS
(
    SELECT
        'RSM' AS team,
        5 AS metric_id,
        'New Active A-Dealer' AS metric_name,

        b.rsm_territory_regions__c AS region,
        dealership_id,
        MIN(submitted_at) AS date
    FROM Sales.business_insights_data_mart_all_apps AS a
    INNER JOIN sfdc.raw_sfdc_account_dyly_curr AS b
        ON a.dealership_id = b.dealership_id__c
    WHERE submitted_at BETWEEN '2023-01-01' AND '2024-03-31'
        AND app_type IN ('Dealer', 'lightspeed')
        AND b.powersports_sales_tier__c LIKE 'A%'
    GROUP BY 1,2,3,4,5
    HAVING date >= '2024-02-01'
),

-----------------------------------------------------------------------------------
--RSM
--Metric #6: A-Dealer Enrollment (excl if Prev Owner had enrolled)

t_RSM_M6 AS
(
    SELECT
        'RSM' AS team,
        6 AS metric_id,
        'A-Dealer Enrollment' AS metric_name,

        a.rsm_territory_regions__c AS region,
        a.dealership_id__c AS dealership_id,
        CAST(a.enrolled_time_stamp__c AS Date) AS date
    FROM sfdc.raw_sfdc_account_dyly_curr AS a
    LEFT JOIN
        (
            SELECT
                Dealership_id__c, enrolled_time_stamp__c
            FROM sfdc.raw_sfdc_account_dyly_curr
        ) AS prev
        ON a.previous_octane_dealer_id__c = prev.dealership_id__c

    WHERE a.vertical_type__c = 'Powersports'
        AND a.powersports_sales_tier__c LIKE 'A%'
        AND prev.enrolled_time_stamp__c IS NULL
        AND a.enrolled_time_stamp__c BETWEEN '2024-02-01' AND '2024-03-31'
),



--==========================================================================
--BDR
--Metric #2: Enrollment (*excl if prev owner has enrolled)

WITH t_BDR_M2 AS
(
    SELECT
        'BDR' AS team,
        2 AS metric_id,
        'Enrollment' AS metric_name,

        a.rsm_territory_regions__c AS region,
        a.dealership_id__c AS dealership_id,
        CAST(a.enrolled_time_stamp__c AS Date) AS date
    FROM sfdc.raw_sfdc_account_dyly_curr AS a
    LEFT JOIN
        (
            SELECT Dealership_id__c, enrolled_time_stamp__c
            FROM sfdc.raw_sfdc_account_dyly_curr
        ) AS prev
        ON a.previous_octane_dealer_id__c = prev.dealership_id__c

    WHERE a.vertical_type__c = 'Powersports'
        AND prev.enrolled_time_stamp__c IS NULL
        AND a.enrolled_time_stamp__c BETWEEN '2024-02-01' AND '2024-03-31'
),

-----------------------------------------------------------------------------------
--BDR
--Metric #3: New E1+

t_BDR_M3 AS
(
    SELECT
        'BDR' AS team,
        3 AS metric_id,
        'New E1+' AS metric_name,

        rsm_territory_regions__c AS region,
        dealership_id,
        MIN(funding_pending_date) AS date
    FROM Sales.business_insights_data_mart
    WHERE COALESCE(LOWER(vehicle_type), 'na') NOT IN ('mower', 'trailer', 'tractor', 'rv', 'automobile', 'na')  --incl PS veh type and 'Other'
        AND funding_pending_date BETWEEN '2023-01-01' AND '2024-03-31'
        AND is_funded = 'Yes'
    GROUP BY 1,2,3,4,5
    HAVING date >= '2024-02-01'
),

-----------------------------------------------------------------------------------
--BDR
--Metric #4: OP Level Up

t_BDR_M4 AS
(
    SELECT
        'BDR' AS team,
        4 AS metric_id,
        'OP Level Up' AS metric_name,

        b.rsm_territory_regions__c AS region,
        a.dealership_id__c AS dealership_id,

        CAST(DATE_ADD('MONTH', 1, CAST(partition_date AS DATE)) - 1 AS DATE) AS date,

        CASE
            WHEN full_status_prior NOT IN ('All-Star', 'MVP') AND full_status IN ('All-Star', 'MVP') THEN 1
            WHEN full_status_prior NOT IN ('MVP') AND full_status IN ('MVP') THEN 1
            ELSE 0
        END AS OP_levelup_ind
    FROM Sales.loyalty_cohort_all_info_monthly AS a
    LEFT JOIN sfdc.raw_sfdc_account_dyly_curr AS b
            ON a.dealership_id__c = b.dealership_id__c
    WHERE partition_date IN ('2024-02-01', '2024-03-01')
        AND (OP_Levelup_Ind = 1)
),

-----------------------------------------------------------------------------------
--BDR
--Metric #5: Prequal Integration

t_BDR_M5 AS
(
    SELECT
        'BDR' AS team,
        5 AS metric_id,
        'Prequal Integration' AS metric_name,

        rsm_territory_regions__c AS region,
        dealership_id__c AS dealership_id,
        CAST(prequal_integrated_enrolled_date__c AS Date) AS date
    FROM sfdc.raw_sfdc_account_dyly_curr
    WHERE vertical_type__c = 'Powersports'
        AND prequal_integrated_enrolled_date__c IS NOT NULL
        AND CAST(prequal_integrated_enrolled_date__c AS Date) BETWEEN '2024-02-01' AND '2024-03-31'
),

-----------------------------------------------------------------------------------
--BDR
--Metric #6: New E3+ (not E3+ since 1/1/2023)

t_BDR_M6 AS
(
    SELECT
        'BDR' AS team,
        6 AS metric_id,
        'New E3+' AS metric_name,

        rsm_territory_regions__c AS region,
        dealership_id,

        MIN(CASE WHEN funding_pending_date >= '2024-02-01' AND rn = 3 THEN funding_pending_date END) AS date, --First_E3plus_Date
        MAX(CASE WHEN funding_pending_date < '2024-02-01' THEN month_tot ELSE 0 END) AS prior_max_fund,
        MAX(CASE WHEN funding_pending_date >= '2024-02-01' THEN month_tot ELSE 0 END) AS curr_max_fund
    FROM
    (
        SELECT
            rsm_territory_regions__c,
            dealership_id,
            funding_pending_date,

            ROW_NUMBER() OVER (PARTITION BY Dealership_id, DATE_Trunc('month', funding_pending_date) ORDER BY funding_pending_date) AS rn,
            SUM(count) OVER (PARTITION BY Dealership_id, DATE_Trunc('month', funding_pending_date)) AS month_tot

        FROM Sales.business_insights_data_mart
        WHERE COALESCE(LOWER(vehicle_type), 'na') NOT IN ('mower', 'trailer', 'tractor', 'rv', 'automobile', 'na')  --incl PS veh type and 'Other'
            AND funding_pending_date BETWEEN '2023-01-01' AND '2024-03-31'
            AND is_funded = 'Yes'
    ) AS t1
    GROUP BY 1,2,3,4,5
    HAVING Prior_max_fund < 3
        AND Curr_max_fund >=3
),

-----------------------------------------------------------------------------------
--BDR
--Metric #7: Enrollment to E5+ (excl if prev owner had enrolled)

t_BDR_M7 AS
(
    SELECT
        'BDR' AS team,
        7 AS metric_id,
        'Enrollment to E5+' AS metric_name,

        rsm_territory_regions__c AS region,
        dealership_id,
        MIN(funding_pending_date) AS date
    FROM
    (
        SELECT
            a.rsm_territory_regions__c,
            dealership_id,
            funding_pending_date,

            row_number() OVER (PARTITION BY Dealership_id, CAST(DATE_Trunc('month', funding_pending_date) AS DATE) ORDER BY funding_pending_date) AS rn

        FROM Sales.business_insights_data_mart AS a
        INNER JOIN sfdc.raw_sfdc_account_dyly_curr AS b
            ON a.dealership_id = b.dealership_id__c

        LEFT JOIN
            (
                SELECT Dealership_id__c, enrolled_time_stamp__c
                FROM sfdc.raw_sfdc_account_dyly_curr
            ) AS prev
            ON b.previous_octane_dealer_id__c = prev.dealership_id__c

        WHERE b.enrolled_time_stamp__c >= '2024-01-01'
            AND prev.enrolled_time_stamp__c IS NULL
            AND COALESCE(LOWER(vehicle_type), 'na') NOT IN ('mower', 'trailer', 'tractor', 'rv', 'automobile', 'na')  --incl PS veh type and 'Other'
            AND funding_pending_date BETWEEN '2024-02-01' AND '2024-03-31'
            AND is_funded = 'Yes'
    ) AS t1
    WHERE rn = 5
    GROUP BY 1,2,3,4,5
),

-----------------------------------------------------------------------------------
--BDR
--Metric #9: New Active B-Dealer (no app since 1/1/2023)

t_BDR_M9 AS
(
    SELECT
        'BDR' AS team,
        9 AS metric_id,
        'New Active B-Dealer' AS metric_name,

        b.rsm_territory_regions__c AS region,
        dealership_id,
        MIN(submitted_at) AS date
    FROM Sales.business_insights_data_mart_all_apps AS a
    INNER JOIN sfdc.raw_sfdc_account_dyly_curr AS b
        ON a.dealership_id = b.dealership_id__c
    WHERE COALESCE(LOWER(vehicle_type), 'na') NOT IN ('mower', 'trailer', 'tractor', 'rv', 'automobile', 'na')  --incl PS veh type and 'Other'
        AND submitted_at BETWEEN '2023-01-01' AND '2024-03-31'
        AND app_type IN ('Dealer', 'lightspeed')
        AND b.powersports_sales_tier__c LIKE 'B%'
    GROUP BY 1,2,3,4,5
    HAVING date >= '2024-02-01'
),


--==========================================================================


t_BI_M_Regn AS
(
    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_BDR_M2

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_BDR_M3

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_BDR_M4

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_BDR_M5

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_BDR_M6

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_BDR_M7

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_BDR_M9

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_RSM_M4

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_RSM_M5

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_RSM_M6

    UNION ALL

    SELECT team, metric_ID, metric_name, region, dealership_id, date, 1 AS metric_volume
    FROM t_RSM_M7
),

t1 AS
(
    SELECT
        'BI Report: by Dealership_ID' AS Report_Type,
        b.sales_name,

        a.team,
        a.metric_id,
        a.metric_name,
        a.region,
        CASE WHEN sales_name = 'xxx' THEN 'South' ELSE a.region END AS region2,
        a.dealership_id,
        a.date,
        a.metric_volume,

        NULL AS note

    FROM t_BI_M_Regn AS a
    LEFT JOIN Sales.sales_contest_24q1_regnrep AS b
        ON a.region = b.region
        AND a.team = b.team

    UNION ALL

    SELECT
        'Sales Report: by Person' AS Report_Type,
        a.sales_name,
        a.team,
        a.metric_id,
        a.metric_name,
        b.region as region,
        CASE WHEN a.sales_name = 'xxx' THEN 'South' ELSE b.region END AS region2,
        999999999 AS dealership_id,
        a.date,
        a.metric_volume,
        a.note

    FROM Sales.sales_contest_24q1_salesrpt AS a
    LEFT JOIN (SELECT * FROM Sales.sales_contest_24q1_regnrep WHERE sales_name != 'Carla Harmon') AS b
        ON a.sales_name = b.sales_name
)


SELECT
    t1.Report_Type,
    t1.sales_name,
    t1.team,
    t1.metric_id,
    t1.metric_name,
    t1.region,
    t1.region2,
    t1.date,
    CASE WHEN t1.Report_Type = 'Sales Report: by Person' THEN NULL ELSE t1.dealership_id END AS dealership_id,
    sf.name as dealership_name,
    COALESCE(t1.metric_volume, 0) AS metric_volume,
    CASE WHEN COALESCE(t1.metric_volume, 0) > 0 THEN 1 ELSE 0 END AS metric_volume_ind,
    a.bounty_per_unit,
    t1.metric_volume * a.bounty_per_unit AS bounty,
    t1.note,

    SUM(bounty) OVER (PARTITION BY t1.sales_name ORDER BY t1.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sum,

    CASE WHEN cum_sum >= 250000 THEN t1.date END AS date_250K,
    CASE WHEN cum_sum >= 500000 THEN t1.date END AS date_500K,
    CASE WHEN cum_sum >= 750000 THEN t1.date END AS date_750K,
    CASE WHEN cum_sum >= 1000000 THEN t1.date END AS date_1000K


FROM t1
LEFT JOIN Sales.sales_contest_24q1_bounty AS a
    ON t1.metric_id = a.metric_id
    AND t1.team = a.team

LEFT JOIN sfdc.raw_sfdc_account_dyly_curr as sf
    ON t1.dealership_id = sf.dealership_id__c

WHERE Sales_NAME IS NOT NULL
    AND date <= '2024-03-31'

);




