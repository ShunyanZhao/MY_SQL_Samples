

/*----------------------------------------------------------------------------------------------------------------------------------
--Get Dealer login activity from DealerUser_dyly_hist table
--Reports are snapshots ON First_Rpt_Date TO Last_Rpt_Date, with increment by x months (Rpt_Incre_MM)
--Result table: DealerAct_Rpts
-----------------------------------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE PROCEDURE Get_DealerAct_Rpts(First_Rpt_Date DATE, Last_Rpt_Date DATE, Rpt_Incre_MM INT)
LANGUAGE plpgsql
AS $$

DECLARE i DATE := First_Rpt_Date;

BEGIN
  DROP TABLE IF EXISTS DealerAct_Rpts;
  RAISE INFO 'i %', i;

  CREATE TABLE DealerAct_Rpts AS
      (
        SELECT
            i AS Report_Date,
            partition_date,
            Dealership_id,

            MAX(last_login) AS Last_Login_anyuser,
            MAX(CASE WHEN LOWER(Dealer_User_Type) = 'default' THEN last_login END) AS Last_Login_Defult,
            MAX(CASE WHEN LOWER(Dealer_User_Type) = 'administrator' THEN last_login END) AS Last_Login_Admin,
            MAX(CASE WHEN LOWER(Dealer_User_Type) = 'finance manager' THEN last_login END) AS Last_Login_FinMgr,

            i - Last_Login_AnyUser::DATE AS Day_sin_lastlog_AnyUser,
            i - Last_Login_Defult::DATE AS Day_sin_lastlog_Defult,
            i - Last_Login_Admin::DATE AS Day_sin_lastlog_Admin,
            i - Last_Login_FinMgr::DATE AS Day_sin_lastlog_FinMgr

        FROM DealerUser_dyly_hist
        WHERE CAST(partition_date AS DATE) = i
            AND Dealership_id IS NOT NULL
        GROUP BY 1,2,3
      ) ;

  LOOP
    i := CAST(DATEADD(MONTH, Rpt_Incre_MM, i) AS DATE);

    RAISE INFO 'i %', i;

    INSERT INTO DealerAct_Rpts
        SELECT
            i AS Report_Date,
            partition_date,
            Dealership_id,

            MAX(last_login) AS Last_Login_anyuser,
            MAX(CASE WHEN LOWER(Dealer_User_Type) = 'default' THEN last_login END) AS Last_Login_Defult,
            MAX(CASE WHEN LOWER(Dealer_User_Type) = 'administrator' THEN last_login END) AS Last_Login_Admin,
            MAX(CASE WHEN LOWER(Dealer_User_Type) = 'finance manager' THEN last_login END) AS Last_Login_FinMgr,

            i - Last_Login_AnyUser::DATE AS Day_sin_lastlog_AnyUser,
            i - Last_Login_Defult::DATE AS Day_sin_lastlog_Defult,
            i - Last_Login_Admin::DATE AS Day_sin_lastlog_Admin,
            i - Last_Login_FinMgr::DATE AS Day_sin_lastlog_FinMgr

        FROM DealerUser_dyly_hist
        WHERE CAST(partition_date AS DATE) = i
            AND Dealership_id IS NOT NULL
        GROUP BY 1,2,3;

    EXIT  WHEN (i >= Last_Rpt_Date);
  END LOOP;
END;
$$;

----------------------------------------------------------------------------------------------------------------------------------
--Result table, DealerAct_Rpts, contains Dealerusers activity on 2021-01-01, 2021-03-01, 2021-05-01,..., 2022-09-01.
CALL Get_DealerAct_Rpts(CAST('2021-01-01' AS DATE) , CAST('2022-09-01' AS DATE), 2);
----------------------------------------------------------------------------------------------------------------------------------

--DROP PROCEDURE Get_DealerAct_Rpts(First_Rpt_Date DATE, Last_Rpt_Date DATE, Rpt_Incre_MM INT)

