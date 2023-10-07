
/*----------------------------------------------------------------------------------------------------------------------
Question: What is the impact of a new product feature?
Background:
    Dealers can enroll in the new feature anytime (thus,some dealers with the new feature, some don't);
    Dealers are different sizes and with different growth rates.
Approach:
    Try to compare Dealers with new feature to their similar (size & growth rate) peers who don't have the feature
    Compare % lift (Funded App or Funded Origination) of enrolled dealers from their Peers since enrollment.

Data Model:
    Monthly performance of the new feature dealers (cohort by enrollment months) and their normalized peers.
    % lift calculation and visualization is built on Tableau, which looks at both by calendar month or by Month on Book.
------------------------------------------------------------------------------------------------------------------------

Peer Selection Criteria:

Peers are selected for each prequal integrated dealership, based on the following criteria:
    (1) M0 funding: funding on the prequal enrolled month
    (2) MoM growth rate: growth rate from previous month to prequal enrolled month.
**The goal with these criteria is to select dealerships with similar funding and growth trend
(criteria is not too strict, so that there would be a good sample size of the prequal dealers with peers.)

Qualify Peers for a Preuqal Dealership:
Peers are selected from non-preqaul integrated Powersport dealerships, satisfying the following:
(1) M0 funding within 25% of the prequal dealer
    (M0: the preuqal enrolled month of the prequal dealer, peer's funding at the same month);
(2) MoM growth rate within 10% of the prequal dealer;

Based on conditions (1) and (2), if a prequal dealer has more than 10 peers, then apply a narrower condition on (2):
(2a) MoM growth rate within 5% of the prequal dealer.

Based on conditions (1) and (2), if a prequal dealer has less than 4 peers, then apply a broader condition on (2):
(2b) MoM growth rate within 50% of the prequal dealer.

As a result, each prequal dealership could have a set of Peer dealership(s) assigned
(some prequal dealerships may not have any identify peer, and these will be excluded from the analysis).

Peer Normalization
For each prequal dealership, the peers’ volumes are summarized and divided by a scaler (S0),
such that the total peers’ volume is the same as the prequal dealer’s volume at M0.
Future month’s volumes are also summarized by month and then divided by this same scaler, S0.

    At M0 (prequal enrolled month),
    S0 = SUM(funding of all Peers) / Funding of the prequal dealer

As a result, there would be one synthetic peer per prequal dealership.
----------------------------------------------------------------------------------------------------------------------*/


CREATE TABLE PreqDealers_n_NormalizedPeer AS (

    --Calendar_Month table is used in temp table, t8a, to populate each calender month since Prequal enrollment for each prequal dealer.
    WITH Calendar_Month AS
    (
        SELECT
            DISTINCT CAST(date_trunc('month', funding_pending_date) AS DATE) AS Calendar_MM
        FROM Sales.business_insights_data_mart
        WHERE funding_pending_date <= DATEADD(DAY, -1, CAST(DATE_TRUNC('MONTH', Current_Timestamp) AS DATE))  --recent completed calendar month
    ),


    --Prequal Dealers and their enroll date and criteria time range determination
    --Peer Selection criteria will look at MO funding, and MOM growth from MP1(prior 1 month) to M0
    t1 AS
    (
        SELECT
            Dealership_id__c,
            vertical_type__c,
            status__c,

            CAST(prequal_integrated_enrolled_date__c AS DATE) AS Prequal_Int_Enrl_Date,
            CAST(DATE_Trunc('month', cast(prequal_integrated_enrolled_date__c AS DATE)) AS DATE) AS M0,

            CAST(DATEADD(MONTH, -1, M0) AS DATE) AS MP1

        FROM sfdc.raw_sfdc_account_dyly_curr
        WHERE Dealership_id__c IS NOT NULL
            AND prequal_integrated_enrolled_date__c IS NOT NULL
            AND CAST(prequal_integrated_enrolled_date__c AS DATE) <= DATEADD(DAY, -1, CAST(DATE_TRUNC('MONTH', Current_Timestamp) AS DATE))  --recent completed calendar month
            AND prequal_integrated_unenrolled_date__c IS NULL
    ),

    --Prequal Dealers monthly Funding and Orig for their Prequal Enrl month, and Prior 1 month before Prequal Enrl
    t2 AS (
        SELECT
            t1.dealership_id__C,
            t1.M0,
            t1.MP1,

            CAST(DATE_Trunc('month', a.funding_pending_date) AS DATE) AS Funding_MM,

            CASE WHEN Funding_MM = t1.M0 THEN 1 ELSE 0 END AS M0_ind,
            CASE WHEN Funding_MM = t1.MP1 THEN 1 ELSE 0 END AS MP1_ind,

            SUM(count) AS Funding,
            SUM(total_financed) AS Orig

        FROM Sales.business_insights_data_mart AS a
        INNER JOIN t1
            ON a.dealership_id = t1.Dealership_id__c

        WHERE a.funding_pending_date IS NOT NULL
            AND LOWER(is_funded) = 'yes'
            AND ( M0_ind = 1 OR MP1_ind = 1 )

        GROUP BY 1,2,3,4,5,6
        ORDER BY 1,2,3,4,5,6
    ),

    --Prequal Dealers and their criteria for peer selection:
    t3 AS (
        SELECT
            t1.Dealership_id__c,

            t1.Prequal_Int_Enrl_Date,
            t1.M0,

            SUM(CASE WHEN M0_Ind = 1 THEN Orig ELSE 0 END) AS M0_Orig,

            SUM(CASE WHEN M0_Ind = 1 THEN Funding ELSE 0 END) AS M0_Fund,
            SUM(CASE WHEN MP1_Ind = 1 THEN Funding ELSE 0 END) AS MP1_Fund,

            CASE WHEN M0_Fund > 0 THEN 1 ELSE 0 END AS w_M0_Fund_Ind,
            CASE WHEN MP1_Fund > 0 THEN 1 ELSE 0 END AS w_MP1_Fund_Ind,

            CASE WHEN M0_Fund > 0 THEN M0_Fund * (1-0.25) END AS M0_Fund_Min,
            CASE WHEN M0_Fund > 0 THEN M0_Fund * (1+0.25) END AS M0_Fund_Max,

            CASE WHEN MP1_Fund > 0 THEN M0_Fund*1.0 / MP1_fund - 1 END AS MP1_Fund_MoM,

            CASE WHEN MP1_Fund_MoM > 0 THEN MP1_Fund_MoM * 0.9
                 WHEN MP1_Fund_MoM < 0 THEN MP1_Fund_MoM * 1.1
                 WHEN MP1_Fund_MoM = 0 THEN -0.1
            END AS MP1_Fund_MoM_Min_1,

            CASE WHEN MP1_Fund_MoM > 0 THEN MP1_Fund_MoM * 1.1
                 WHEN MP1_Fund_MoM < 0 THEN MP1_Fund_MoM * 0.9
                 WHEN MP1_Fund_MoM = 0 THEN 0.1
            END AS MP1_Fund_MoM_Max_1,

            CASE WHEN MP1_Fund_MoM > 0 THEN MP1_Fund_MoM * 0.5
                 WHEN MP1_Fund_MoM < 0 THEN MP1_Fund_MoM * 1.5
                 WHEN MP1_Fund_MoM = 0 THEN -0.5
            END AS MP1_Fund_MoM_Min_2L,

            CASE WHEN MP1_Fund_MoM > 0 THEN MP1_Fund_MoM * 1.5
                 WHEN MP1_Fund_MoM < 0 THEN MP1_Fund_MoM * 0.5
                 WHEN MP1_Fund_MoM = 0 THEN 0.5
            END AS MP1_Fund_MoM_Max_2L,

            CASE WHEN MP1_Fund_MoM > 0 THEN MP1_Fund_MoM * 0.95
                 WHEN MP1_Fund_MoM < 0 THEN MP1_Fund_MoM * 1.05
                 WHEN MP1_Fund_MoM = 0 THEN -0.05
            END AS MP1_Fund_MoM_Min_2H,

            CASE WHEN MP1_Fund_MoM > 0 THEN MP1_Fund_MoM * 1.05
                 WHEN MP1_Fund_MoM < 0 THEN MP1_Fund_MoM * 0.95
                 WHEN MP1_Fund_MoM = 0 THEN 0.05
            END AS MP1_Fund_MoM_Max_2H

        FROM t1
        LEFT JOIN t2
            ON t1.dealership_id__C = t2.dealership_id__c

        GROUP BY 1,2,3
    ),

    --Non Prequal Dealers and their measurements for each Prequal Integrated Cohort
    t4 AS (
        SELECT
            a.dealership_id,
            b.M0,
            b.MP1,

            SUM(CASE WHEN DATE_Trunc('month', a.funding_pending_date) = b.M0 THEN a.total_financed ELSE 0 END) AS M0_Orig,

            SUM(CASE WHEN DATE_Trunc('month', a.funding_pending_date) = b.M0 THEN a.count ELSE 0 END) AS M0_Fund,
            SUM(CASE WHEN DATE_Trunc('month', a.funding_pending_date) = b.MP1 THEN a.count ELSE 0 END) AS MP1_Fund,

            CASE WHEN MP1_Fund > 0 THEN M0_Fund*1.0 / MP1_fund - 1 END AS MP1_Fund_MoM

        FROM Sales.business_insights_data_mart AS a
        LEFT JOIN sfdc.raw_sfdc_account_dyly_curr AS sf
            ON a.dealership_id = sf.dealership_id__c

        CROSS JOIN (SELECT DISTINCT M0, MP1 /*, MP2*/ FROM t1) AS b

        WHERE a.funding_pending_date IS NOT NULL
            AND LOWER(a.is_funded) = 'yes'
            ----Peers are Non Prequal Integrated PS Dealerships----
            AND a.dealership_id NOT IN
             (
                SELECT DISTINCT Dealership_id__c
                FROM sfdc.raw_sfdc_account_dyly_curr
                WHERE prequal_integrated_enrolled_date__c IS NOT NULL
             )

        GROUP BY 1,2,3
        ORDER BY 1,2
    ),

    --Prequal Dealers and their peer flags on all non-prequal dealers
    t5 AS
    (
        SELECT
               a.*,
               SUM(Peer_Sel_2_Ind) OVER (PARTITION BY M0, Dealership_id__c) AS Peer_Cnt_Sel_2,
               Peer_Cnt_Sel_2 AS Peer_Cnt_Final
        FROM
        (
            SELECT
                t3.*,

                t4.dealership_id AS Peer_Dealership_id,
                t4.M0_Orig AS Peer_M0_Orig,
                t4.M0_Fund AS Peer_M0_Fund,
                t4.MP1_Fund AS Peer_MP1_Fund,
                t4.MP1_Fund_MoM AS Peer_MP1_Fund_MoM,

                CASE WHEN t4.M0_Fund BETWEEN t3.M0_Fund_Min AND t3.M0_Fund_Max THEN 1 ELSE 0 END AS within_M0_Fund_Ind,
                CASE WHEN t4.MP1_Fund_MoM BETWEEN t3.MP1_Fund_MoM_Min_1 AND t3.MP1_Fund_MoM_Max_1 THEN 1 ELSE 0 END AS within_MP1_Fund_MoM_1,
                CASE WHEN t4.MP1_Fund_MoM BETWEEN t3.MP1_Fund_MoM_Min_2L AND t3.MP1_Fund_MoM_Max_2L THEN 1 ELSE 0 END AS within_MP1_Fund_MoM_2L,
                CASE WHEN t4.MP1_Fund_MoM BETWEEN t3.MP1_Fund_MoM_Min_2H AND t3.MP1_Fund_MoM_Max_2H THEN 1 ELSE 0 END AS within_MP1_Fund_MoM_2H,

                CASE WHEN within_M0_Fund_Ind = 1 AND within_MP1_Fund_MoM_1 = 1 THEN 1 ELSE 0 END AS Peer_Sel_1_Ind,

                SUM(Peer_Sel_1_Ind) OVER (PARTITION BY t3.M0, t3.Dealership_id__c) AS Peer_Cnt_Sel_1,

                CASE WHEN Peer_Cnt_Sel_1 < 4 AND within_M0_Fund_Ind = 1 AND within_MP1_Fund_MoM_2L = 1 THEN 1
                     WHEN Peer_Cnt_Sel_1 > 10 AND within_M0_Fund_Ind = 1 AND within_MP1_Fund_MoM_2H = 1 THEN 1
                     WHEN Peer_Cnt_Sel_1 BETWEEN 4 AND 10 AND within_M0_Fund_Ind = 1 AND within_MP1_Fund_MoM_1 = 1 THEN 1
                     ELSE 0
                END AS Peer_Sel_2_Ind,

                Peer_Sel_2_Ind AS Peer_Sel_Final_Ind

            FROM t3
            LEFT JOIN t4
                ON t3.M0 = t4.M0
        ) as a
    ),

    --Prequal Dealers summary from t5
    t6 AS
    (
        SELECT
            M0,
            Dealership_id__c,
            M0_Orig,
            M0_Fund,
            MP1_Fund,
			
            MAX(Peer_Cnt_Final) AS Peer_Cnt,
            CASE WHEN Peer_Cnt > 0 THEN 1 ELSE 0 END AS w_Peer_Ind,
            SUM(CASE WHEN Peer_Sel_Final_Ind = 1 THEN Peer_M0_Orig  END) AS Peer_tot_M0_Orig,
            SUM(CASE WHEN Peer_Sel_Final_Ind = 1 THEN Peer_M0_Fund  END) AS Peer_tot_M0_Fund,
            CASE WHEN w_Peer_Ind = 1 AND M0_Orig > 0 THEN Peer_tot_M0_Orig * 1.0 / M0_Orig END AS Norm_Ratio_Orig,
            CASE WHEN w_Peer_Ind = 1 AND M0_Fund > 0 THEN Peer_tot_M0_Fund * 1.0 / M0_Fund END AS Norm_Ratio_Fund
        FROM t5
        GROUP BY 1,2,3,4,5
        ORDER BY 1,2,3,4,5
    ),

    --each Peers' monthly funding and origination for each Prequal Dealer (m0, dealership_id__c)
    t7a AS
    (
        SELECT
            a.M0,
            a.Dealership_id__c,
            a.Peer_dealership_id,

            CAST(DATE_Trunc('month', b.funding_pending_date) AS DATE) AS Funding_MM,

            SUM(count) AS Funded_Apps,
            SUM(total_financed) AS Orig_Amt

        FROM
        (
            SELECT M0, Dealership_id__c, Peer_Dealership_id
            FROM t5
            WHERE Peer_Sel_Final_Ind = 1
        ) AS a

        LEFT JOIN Sales.business_insights_data_mart AS b
           ON a.peer_dealership_id = b.dealership_id

        WHERE  DATEDIFF(Month, a.m0, funding_MM) >= 0
           AND LOWER(is_funded) = 'yes'

        GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4
    ),

    --Normalized Peers_total funding and origination by funding month
    t7b AS
    (
        SELECT
            t7a.m0,
            t7a.dealership_id__c,
            Funding_MM,
            t6.Norm_Ratio_fund,
            t6.Norm_Ratio_Orig,
            SUM(Funded_Apps) AS Funded,
            SUM(Orig_Amt) AS Orig,

            Funded / t6.Norm_Ratio_fund AS Norm_Funded,
            Orig / t6.Norm_Ratio_Orig AS Norm_Orig

        FROM t7a
        LEFT JOIN t6
           ON t7a.m0 = t6.M0
           AND t7a.dealership_id__c = t6.dealership_id__c
        GROUP BY 1,2,3,4,5
        ORDER BY 1,2,3,4,5
    ),

    --Prequal Dealers with each Calendar Month from Enrl date to recent completed calendar month
    t8a AS
    (
        SELECT M0, Dealership_id__c, w_Peer_Ind, Peer_Cnt, a.Calendar_MM
        FROM t6
        CROSS JOIN Calendar_Month AS a
        WHERE DATEDIFF(Month, m0, Calendar_MM) >= 0
        ORDER BY 1,2,3,4,5
    ),

    --Prequal Dealers and their monthly funding and orig by each Calendar month from Enrl Date to recent completed calendar month
    t8b AS
    (
        SELECT
            t8a.M0,
            t8a.Dealership_id__c,
            t8a.w_Peer_Ind,
            t8a.Peer_Cnt,
            t8a.Calendar_MM,

            COALESCE(a.Funded, 0) AS Funded_app,
            COALESCE(a.Orig, 0) AS Orig_amt

        FROM t8a
        LEFT JOIN
        (
            SELECT
                dealership_id,
                CAST(DATE_Trunc('month', funding_pending_date) AS DATE) AS Funding_MM,
                SUM(count) AS Funded,
                SUM(total_financed) AS Orig
            FROM Sales.business_insights_data_mart
            WHERE LOWER(is_funded) = 'yes'
            GROUP BY 1,2
            ORDER BY 1,2
        ) AS a
        ON t8a.dealership_id__c = a.dealership_id
           AND t8a.Calendar_MM = a.Funding_MM
        ORDER BY 1,2,3,4
    ),

    --Monthly Perf of all Prequal Dealers and their normalized Peers' perf
    t AS
    (
        SELECT
            t8b.M0,
            a.M0_PrequalD_Cnt,
            a.M0_PrequalD_wPeer_Cnt,

            t8b.Dealership_id__c,
            t8b.w_Peer_Ind,
            t8b.Peer_Cnt,
            t8b.Calendar_MM,

            DATEDIFF(Month, t8b.m0, calendar_MM) AS MM,

            t8b.Funded_app,
            t8b.Orig_amt,

            t7b.Norm_Funded,
            t7b.Norm_Orig

        FROM t8b
        LEFT JOIN t7b
            ON t8b.m0 = t7b.M0
            AND t8b.dealership_id__c = t7b.dealership_id__C
            AND t8b.Calendar_MM = t7b.funding_mm

        LEFT JOIN
        (
            SELECT
                M0,
                count(distinct dealership_id__c) AS M0_PrequalD_Cnt,
                SUM(Case when w_peer_ind = 1 THEN 1 ELSE 0 END) AS M0_PrequalD_wPeer_Cnt
            FROM t6
            GROUP BY 1
        ) AS a
            ON t8b.m0= a.m0

        ORDER BY 1,2,3,4,5,6,7
    ),

    --Include addt'l info for the prequal dealers:
    --SF: powersports_sales_tier__c, dealership name
    t_final AS
    (
        SELECT
            M0,
            MAX(MM) OVER (PARTITION BY M0) AS M0_Tenure,
            M0_PrequalD_Cnt,
            M0_PrequalD_wPeer_Cnt,

            t.Dealership_id__c AS Dealership_ID,
            sf.name AS Dealership_Name,
            w_Peer_Ind,
            Peer_Cnt,

            CASE
                WHEN sf.powersports_sales_tier__c LIKE '%A1%' THEN 'A1'
                WHEN sf.powersports_sales_tier__c LIKE '%A2%' THEN 'A2'
                WHEN sf.powersports_sales_tier__c LIKE '%A3%' THEN 'A3'
                WHEN sf.powersports_sales_tier__c LIKE '%B1%' THEN 'B1'
                WHEN sf.powersports_sales_tier__c LIKE '%B2%' THEN 'B2'
                WHEN sf.powersports_sales_tier__c LIKE '%B3%' THEN 'B3'
                ELSE sf.powersports_sales_tier__c
            END AS Dealership_Tier,

            Calendar_MM,
            MM,

            Funded_app AS Funding,
            Orig_amt AS Orig,
            Norm_Funded AS Funding_Peer,
            Norm_Orig AS Orig_Peer
        FROM t
        LEFT JOIN sfdc.raw_sfdc_account_dyly_curr AS sf
            ON t.dealership_id__c = sf.dealership_id__c

        ORDER BY 3,1,2,4,10,11
    )

    SELECT *
    FROM t_final
)