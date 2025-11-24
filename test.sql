CREATE OR REPLACE PACKAGE BODY SYSADM.INV_RECON_PROC_NEW IS

  PROCEDURE PREPARE_OPERATOR_DATA(v_last_rundt IN TIMESTAMP,
                                  ERROR_CODE   OUT NUMBER) AS
  
    v_fresh_inv     NUMBER := 0;
    v_partial_inv   NUMBER := 0;
    v_rerun_inv     NUMBER := 0;
    v_final_inv_cnt NUMBER := 0;
    v_logmsg        VARCHAR2(2000);
    v_procName      VARCHAR2(50);
	v_temp_vrun_dt TIMESTAMP;
  
  BEGIN
  
    error_code := 200;
	
	v_temp_vrun_dt := TO_TIMESTAMP('2025-06-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'); --temp hardcoded
	
	
  
    DELETE FROM T_SET_INVOICE_RECON;
    DELETE FROM S_INV_RECON_LDR_DTL;
  
    v_procName := 'PREPARE_OPERATOR_DATA';
    v_logmsg   := 'START OF PROCEDURE PREPARE_OPERATOR_DATA';
    auditloggerProcedure(v_procName, v_logmsg);
  
    v_fresh_inv := 0;
  
    FOR ACTRN IN (SELECT distinct ICT_OPERATOR,
                                  PRODUCT_GROUP,
                                  TRAFFIC_PERIOD,
                                  INVOICE_NUMBER,
                                  CASH_FLOW,
                                  STATUS
                    FROM VW_ACCOUNT_TRANSACTIONS VAT
                   WHERE VAT.ACCOUNT_TRANSACTION = 'DOCUMENT_RECEIVED'
                     AND VAT.CONDITION = 'ACTUAL'
                     and VAT.PRODUCT != 'MMS'
                     AND VAT.STATUS in ('CREATED',
                                        'POSTED',
                                        'POSTED AND LOCKED',
                                        'FAILED POSTING',
                                        'FAILED REVERSAL',
                                        'OPEN',
                                        'APPROVED') --- hasan-27/05/2024-Teams
        AND  (  VAT.LAST_MODIFICATION_DAT > v_last_rundt  OR CREATED_ON > v_last_rundt )
                  --AND ( ICT_OPERATOR = 'SWB'  and traffic_period = '2021/07/01 - 2021/07/31' and product_group = 'DATA' )  
                  ) LOOP
    
      INSERT INTO S_INV_RECON_LDR_DTL
        (BATCH_ID,
         FRANCHISE,
         OPERATOR,
         BILLING_PERIOD,
         TRAFFIC_PERIOD,
         INVOICE_NUMBER,
         BILLING_METHOD,
         STATEMENT_DIRECTION,
         PRODUCT_GROUP,
         COMPONENT,
         PRODUCT,
         INV_PRODUCT,
         CASH_FLOW,
         CURRENCY,
         CALL_COUNT,
         CHARGED_USAGE,
         NET_AMOUNT_ORIG,
         TAXES,
         GROSS_AMOUNT,
         DESCRIPTION,
         RATE,
         INV_STATUS,
         NET_AMOUNT,
         INV_RCVD_DATE)
        SELECT DISTINCT 1,
                        ELS.FRANCHISE,
                        ELS.OPERATOR,
                        ELS.BILLING_PERIOD,
                        ELS.TRAFFIC_PERIOD,
                        ELS.INVOICE_NUMBER,
                        ELS.BILLING_METHOD,
                        ELS.STATEMENT_DIRECTION,
                        ELS.PRODUCT_GROUP,
                        ELS.COMPONENT,
                        ELS.PRODUCT,
                        ELS.PRODUCT,
                        ELS.CASH_FLOW,
                        ELS.CURRENCY,
                        ELS.CALL_COUNT,
                        ELS.CHARGED_USAGE,
                        ELS.NET_AMOUNT,
                        ELS.TAXES,
                        ELS.GROSS_AMOUNT,
                        REPLACE(REPLACE(ORIGINORDEST, CHR(13), ' '),
                                CHR(10),
                                ' ') DESCRIPTION,
                        round(ELS.RATE, 6),
                        ACTRN.STATUS,
                        (ELS.RATE * ELS.CHARGED_USAGE) as NET_AMOUNT,
                        INV_RCVD_DATE
          FROM ERP_LOADED_STATEMENTS ELS
         WHERE OPERATOR = ACTRN.ICT_OPERATOR
           AND PRODUCT_GROUP = ACTRN.PRODUCT_GROUP
           AND TRAFFIC_PERIOD = ACTRN.TRAFFIC_PERIOD
           AND INVOICE_NUMBER = ACTRN.INVOICE_NUMBER
           AND CASH_FLOW = ACTRN.CASH_FLOW;
    
      v_fresh_inv := v_fresh_inv + 1;
    
    END LOOP;
  
    -------- Omitting Freshly Loaded Invoices  which are not in CREATED status But Recon Already Done For Them   -----------------
  
    IF (v_fresh_inv > 0) THEN
    
      v_logmsg := 'Omitting Freshly Loaded Invoices  which are not in CREATED status But Recon Already Done For Them';
      auditloggerProcedure(v_procName, v_logmsg);
    
      DELETE FROM S_INV_RECON_LDR_DTL
       WHERE (OPERATOR, PRODUCT_GROUP, TRAFFIC_PERIOD, INVOICE_NUMBER,
              CASH_FLOW, BILLING_METHOD, CURRENCY, STATEMENT_DIRECTION,
              NVL(INV_STATUS, ' ')) IN
             (SELECT DISTINCT LDR.OPERATOR,
                              LDR.PRODUCT_GROUP,
                              LDR.TRAFFIC_PERIOD,
                              LDR.INVOICE_NUMBER,
                              LDR.CASH_FLOW,
                              LDR.BILLING_METHOD,
                              LDR.CURRENCY,
                              LDR.STATEMENT_DIRECTION,
                              NVL(LDR.INV_STATUS, ' ')
                FROM S_INV_RECON_LDR_DTL  LDR,
                     T_SET_INV_RECON_HDR  HDR,
                     VW_MST_OPR_PRD_COMBO MST
               WHERE HDR.MST_OPR_CODE = MST.MST_CODE
                 AND LDR.OPERATOR = MST.CHLD_CODE
                 AND HDR.PRODUCT_GROUP = MST.PRD_GRP
                 AND HDR.TRAFFIC_PERIOD = LDR.TRAFFIC_PERIOD
                 AND HDR.BILLING_METHOD = LDR.BILLING_METHOD
                 AND HDR.STATEMENT_DIRECTION = LDR.STATEMENT_DIRECTION
                 AND HDR.PRODUCT_GROUP = LDR.PRODUCT_GROUP
                 AND HDR.CASH_FLOW = LDR.CASH_FLOW
                 AND HDR.CURRENCY = LDR.CURRENCY
                 AND INSTR(HDR.INVOICE_NUMBER, LDR.INVOICE_NUMBER) > 0
                 AND NVL(LDR.INV_STATUS, ' ') NOT IN ('CREATED', 'OPEN'));
    
    END IF;
  
    v_fresh_inv := 0;
    SELECT COUNT(1) into v_fresh_inv FROM S_INV_RECON_LDR_DTL;
  
    ------ CR2-Point3-Partial Invoices Reloading and  Merging -------
  
    v_partial_inv := 0;
  
    IF (v_fresh_inv >= 1) THEN
    
      v_logmsg := 'LOADING INVOICE/DECL DATA FROM PREVIOUS BATCHES FOR MERGING and  RECON';
      auditloggerProcedure(v_procName, v_logmsg);
    
      INSERT INTO S_INV_RECON_LDR_DTL
        (BATCH_ID,
         FRANCHISE,
         OPERATOR,
         BILLING_PERIOD,
         TRAFFIC_PERIOD,
         INVOICE_NUMBER,
         BILLING_METHOD,
         STATEMENT_DIRECTION,
         PRODUCT_GROUP,
         COMPONENT,
         PRODUCT,
         INV_PRODUCT,
         CASH_FLOW,
         CURRENCY,
         CALL_COUNT,
         CHARGED_USAGE,
         NET_AMOUNT_ORIG,
         TAXES,
         GROSS_AMOUNT,
         DESCRIPTION,
         RATE,
         NET_AMOUNT,
         INV_RCVD_DATE)
        WITH PRV_LDR AS
         (SELECT DISTINCT ICT_OPERATOR,
                          PRODUCT_GROUP,
                          TRAFFIC_PERIOD,
                          INVOICE_NUMBER,
                          CASH_FLOW,
                          BILLING_METHOD,
                          CURRENCY,
                          STATEMENT_DIRECTION
            FROM VW_ACCOUNT_TRANSACTIONS
           WHERE account_transaction = 'DOCUMENT_RECEIVED'
             AND CONDITION = 'ACTUAL'
             AND status in ('OPEN',
                            'CREATED',
                            'APPROVED',
                            'PENDING ERP VERIFICATION',
                            'POSTED',
                            'POSTED AND LOCKED',
                            'FAILED POSTING')
             AND LAST_MODIFICATION_DAT < v_temp_vrun_dt
             AND (ICT_OPERATOR, PRODUCT_GROUP, TRAFFIC_PERIOD, CASH_FLOW,
                  BILLING_METHOD, CURRENCY, STATEMENT_DIRECTION) IN
                 (SELECT DISTINCT MST.CHLD_CODE,
                                  PRODUCT_GROUP,
                                  TRAFFIC_PERIOD,
                                  CASH_FLOW,
                                  BILLING_METHOD,
                                  CURRENCY,
                                  STATEMENT_DIRECTION
                    FROM S_INV_RECON_LDR_DTL  LDR,
                         VW_MST_OPR_PRD_COMBO MST,
                         VW_MST_OPR_PRD_COMBO CHLD
                   WHERE MST.MST_CODE = CHLD.MST_CODE
                     AND CHLD.CHLD_CODE = LDR.OPERATOR))
        SELECT DISTINCT 1,
                        ELS.FRANCHISE,
                        ELS.OPERATOR,
                        ELS.BILLING_PERIOD,
                        ELS.TRAFFIC_PERIOD,
                        ELS.INVOICE_NUMBER,
                        ELS.BILLING_METHOD,
                        ELS.STATEMENT_DIRECTION,
                        ELS.PRODUCT_GROUP,
                        ELS.COMPONENT,
                        ELS.PRODUCT,
                        ELS.PRODUCT,
                        ELS.CASH_FLOW,
                        ELS.CURRENCY,
                        ELS.CALL_COUNT,
                        ELS.CHARGED_USAGE,
                        ELS.NET_AMOUNT,
                        ELS.TAXES,
                        ELS.GROSS_AMOUNT,
                        REPLACE(REPLACE(ORIGINORDEST, CHR(13), ' '),
                                CHR(10),
                                ' ') DESCRIPTION,
                        round(ELS.RATE, 6),
                        (ELS.RATE * ELS.CHARGED_USAGE) as NET_AMOUNT,
                        INV_RCVD_DATE
          FROM ERP_LOADED_STATEMENTS ELS
         WHERE (OPERATOR, PRODUCT_GROUP, TRAFFIC_PERIOD, INVOICE_NUMBER,
                CASH_FLOW, BILLING_METHOD, CURRENCY, STATEMENT_DIRECTION) IN
               (SELECT DISTINCT ICT_OPERATOR,
                                PRODUCT_GROUP,
                                TRAFFIC_PERIOD,
                                INVOICE_NUMBER,
                                CASH_FLOW,
                                BILLING_METHOD,
                                CURRENCY,
                                STATEMENT_DIRECTION
                  FROM PRV_LDR);
    
      v_partial_inv := SQL%ROWCOUNT;
    
    END IF;
  
    INSERT INTO S_INV_RECON_LDR_DTL
      (BATCH_ID,
       FRANCHISE,
       OPERATOR,
       BILLING_PERIOD,
       TRAFFIC_PERIOD,
       INVOICE_NUMBER,
       BILLING_METHOD,
       STATEMENT_DIRECTION,
       PRODUCT_GROUP,
       COMPONENT,
       PRODUCT,
       INV_PRODUCT,
       CASH_FLOW,
       CURRENCY,
       CALL_COUNT,
       CHARGED_USAGE,
       NET_AMOUNT_ORIG,
       TAXES,
       GROSS_AMOUNT,
       DESCRIPTION,
       RATE,
       NET_AMOUNT,
       INV_RCVD_DATE)
      SELECT DISTINCT 1,
                      ELS.FRANCHISE,
                      ELS.OPERATOR,
                      ELS.BILLING_PERIOD,
                      ELS.TRAFFIC_PERIOD,
                      ELS.INVOICE_NUMBER,
                      ELS.BILLING_METHOD,
                      ELS.STATEMENT_DIRECTION,
                      ELS.PRODUCT_GROUP,
                      ELS.COMPONENT,
                      ELS.PRODUCT,
                      ELS.PRODUCT,
                      ELS.CASH_FLOW,
                      ELS.CURRENCY,
                      ELS.CALL_COUNT,
                      ELS.CHARGED_USAGE,
                      ELS.NET_AMOUNT,
                      ELS.TAXES,
                      ELS.GROSS_AMOUNT,
                      REPLACE(REPLACE(ORIGINORDEST, CHR(13), ' '),
                              CHR(10),
                              ' ') DESCRIPTION,
                      round(ELS.RATE, 6),
                      (ELS.RATE * ELS.CHARGED_USAGE) as NET_AMOUNT,
                      INV_RCVD_DATE
        FROM ERP_LOADED_STATEMENTS ELS
       WHERE (OPERATOR, PRODUCT_GROUP, TRAFFIC_PERIOD, INVOICE_NUMBER,
              CASH_FLOW) in
             (SELECT distinct ICT_OPERATOR,
                              PRODUCT_GROUP,
                              TRAFFIC_PERIOD,
                              INVOICE_NUMBER,
                              CASH_FLOW
                FROM VW_ACCOUNT_TRANSACTIONS VAT
               WHERE VAT.ACCOUNT_TRANSACTION = 'DOCUMENT_RECEIVED'
                 AND VAT.CONDITION = 'ACTUAL' --- below status line added on 15-apr-21  for hasan 
                 AND VAT.STATUS in ('OPEN',
                                    'CREATED',
                                    'APPROVED',
                                    'PENDING ERP VERIFICATION',
                                    'POSTED',
                                    'POSTED AND LOCKED',
                                    'FAILED POSTING')
                 AND (OPERATOR, PRODUCT_GROUP, TRAFFIC_PERIOD, CASH_FLOW,
                      CURRENCY, BILLING_METHOD, STATEMENT_DIRECTION) IN
                     (SELECT distinct MST.CHLD_CODE,
                                      HDR.PRODUCT_GROUP,
                                      HDR.TRAFFIC_PERIOD,
                                      HDR.CASH_FLOW,
                                      HDR.CURRENCY,
                                      HDR.BILLING_METHOD,
                                      HDR.STATEMENT_DIRECTION
                        FROM T_SET_INV_RECON_HDR  HDR,
                             VW_MST_OPR_PRD_COMBO MST
                       WHERE HDR.MST_OPR_CODE = MST.MST_CODE
                         AND HDR.USER_ACTION = 'ReRun'
                         AND HDR.ACTION_STATUS = 'RUNNING'));
  
    /*----- Chg-25 - If No Matching or Valid Invoices Found For Any Recon , set DISP_STATUS as CANCELLED for them With Auditing 
         -- Reason may be any of the Below Recon combo mismatch  :
               OPERATOR,PRODUCT_GROUP,TRAFFIC_PERIOD,CASH_FLOW,CURRENCY,BILLING_METHOD,STATEMENT_DIRECTION
            OR None of the invoices For the Matching Combo might be in below status : 
               Account_Trans = DOCUMENT_RECEIVED / Condition = ACTUAL / Inv Status = CREATED/APPROVED/PENDING ERP VERIFICATION/POSTED/POSTED AND LOCKED/FAILED POSTING
    --------------*/
    v_logmsg := 'BEFORE SELECT OF RECON CANCELLED AS NO VALID INVOICE FOUND IN FAM';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_INV_RECON_AUDIT
      (ID,
       SOURCE,
       HDR_ID,
       USERNAME,
       USER_FULL_NAME,
       USER_ACTION,
       USER_COMMENT,
       ACTION_TIMESTATMP)
      SELECT T_SET_INV_RECON_AUDIT_SEQ.NEXTVAL ID,
             'INV_RECON_HDR' SOURCE,
             HDR_ID,
             'SYSTEM' USERNAME,
             'INV_RECON_PROCESS' USER_FULL_NAME,
             USER_ACTION,
             'RECON CANCELLED AS NO VALID INVOICE FOUND IN FAM' USER_COMMENT,
             SYSTIMESTAMP ACTION_TIMESTATMP
        FROM T_SET_INV_RECON_HDR HD
       WHERE HD.USER_ACTION = 'ReRun'
         AND HD.ACTION_STATUS = 'RUNNING'
         AND HD.HDR_ID NOT IN
             (SELECT DISTINCT HDR.HDR_ID
                FROM T_SET_INV_RECON_HDR HDR, VW_ACCOUNT_TRANSACTIONS FM
               WHERE HDR.USER_ACTION = 'ReRun'
                 AND HDR.ACTION_STATUS = 'RUNNING'
                 AND FM.PRODUCT_GROUP = HDR.PRODUCT_GROUP
                 AND FM.TRAFFIC_PERIOD = HDR.TRAFFIC_PERIOD
                 AND FM.CASH_FLOW = HDR.CASH_FLOW
                 AND FM.CURRENCY = HDR.CURRENCY
                 AND FM.OPERATOR = HDR.MST_OPR_CODE
                 AND FM.BILLING_METHOD = HDR.BILLING_METHOD
                 AND FM.STATEMENT_DIRECTION = HDR.STATEMENT_DIRECTION
                 AND FM.ACCOUNT_TRANSACTION = 'DOCUMENT_RECEIVED'
                 AND FM.CONDITION = 'ACTUAL' --- below status line added on 15-apr-21  for hasan
                 AND FM.STATUS in ('CREATED',
                                   'APPROVED',
                                   'PENDING ERP VERIFICATION',
                                   'POSTED',
                                   'POSTED AND LOCKED',
                                   'FAILED POSTING',
                                   'OPEN',
                                   'APPROVED'));
  
    v_logmsg := 'BEFORE SELECT OF T_SET_INV_RECON_HDR &  S_INV_RECON_LDR_DTL';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE T_SET_INV_RECON_HDR HDR
       SET ACTION_STATUS      = 'NEW',
           USER_ACTION        = 'Cancel',
           LAST_MODIFIED_BY   = 'SYSTEM',
           LAST_MODIFIED_DATE = SYSDATE
    --- Chg-33 : **** below replaced with above, recon Cancelled and not going thru for swapping prevly approved recons
    ---  SET    ACTION_STATUS = 'COMPLETED' , DISP_STATUS   = 'CANCELLED' , DISP_STATUS_DT = SYSDATE  ,
    ---         LAST_MODIFIED_BY = 'SYSTEM' , LAST_MODIFIED_DATE = SYSDATE 
     WHERE HDR.USER_ACTION = 'ReRun'
       AND HDR.ACTION_STATUS = 'RUNNING'
       AND HDR_ID NOT IN
           (SELECT DISTINCT HDR.HDR_ID
              FROM T_SET_INV_RECON_HDR HDR, VW_ACCOUNT_TRANSACTIONS FM
             WHERE HDR.USER_ACTION = 'ReRun'
               AND HDR.ACTION_STATUS = 'RUNNING'
               AND FM.PRODUCT_GROUP = HDR.PRODUCT_GROUP
               AND FM.TRAFFIC_PERIOD = HDR.TRAFFIC_PERIOD
               AND FM.CASH_FLOW = HDR.CASH_FLOW
               AND FM.CURRENCY = HDR.CURRENCY
               AND FM.OPERATOR = HDR.MST_OPR_CODE
               AND FM.BILLING_METHOD = HDR.BILLING_METHOD
               AND FM.STATEMENT_DIRECTION = HDR.STATEMENT_DIRECTION
               AND FM.ACCOUNT_TRANSACTION = 'DOCUMENT_RECEIVED'
               AND FM.CONDITION = 'ACTUAL'
               AND FM.STATUS in ('CREATED',
                                 'APPROVED',
                                 'PENDING ERP VERIFICATION',
                                 'POSTED',
                                 'POSTED AND LOCKED',
                                 'FAILED POSTING',
                                 'OPEN',
                                 'APPROVED'));
			COMMIT;
			
	SELECT COUNT(1) into v_final_inv_cnt  FROM  S_INV_RECON_LDR_DTL ;
  
    IF (v_final_inv_cnt = 0) THEN
    
      v_logmsg   := '*******  NO INVOICE SELECTED FROM FRESH/RERUN/PREV SOURCES FOR RECON. Quitting ******* ';
      ERROR_CODE := 300;
      auditloggerProcedure(v_procName, v_logmsg);
      RETURN;
    END IF;
	
	
  
  EXCEPTION
    WHEN OTHERS THEN
      v_logmsg := 'ERR_MSG-' || sqlerrm;
      DBMS_OUTPUT.PUT_LINE('V_outVal: ' || sqlerrm);
      auditloggerProcedure(v_procName, v_logmsg);
      error_code := 100;
      RETURN;
    
  END;

  PROCEDURE ENRICH_OPERATOR_DATA(ERROR_CODE OUT NUMBER) AS
  
    vs_cons_dest   VARCHAR2(3990) := '';
    vn_dest_length number := 0;
    vs_symbol      VARCHAR2(3) := '';
    vs_cty_dest    VARCHAR2(200) := '';
    v_logmsg       VARCHAR2(2000);
    v_procName     VARCHAR2(50);
  
  BEGIN
    error_code := 200;
  
    v_procName := 'ENRICH_OPERATOR_DATA';
    v_logmsg   := 'START OF PROCEDURE ENRICH_OPERATOR_DATA';
    auditloggerProcedure(v_procName, v_logmsg);
  
    DELETE FROM S_INV_RECON_LDR_SUM;
  
    update S_INV_RECON_LDR_DTL INV
       SET DIFF_TYPE = 'E', DIFF_DESCR = 'INVALID OPERATOR CODE'
    WHERE OPERATOR NOT IN (SELECT ID FROM ITUPRDI.ORGANISATION)
       AND DIFF_TYPE IS NULL;
  
    DELETE FROM S_INV_RECON_LDR_DTL WHERE PRODUCT = 'MMS';
  
    UPDATE S_INV_RECON_LDR_DTL INV
       SET NET_AMOUNT = DECODE(PRODUCT_GROUP,
                               'TELE',
                               RATE * CHARGED_USAGE,
                               RATE * CALL_COUNT);
  
    v_logmsg := 'LOADING MASTER OPR DATA FROM VW_MST_OPR_PRD_COMBO TO S_INV_RECON_LDR_DTL';
    auditloggerProcedure(v_procName, v_logmsg);
  
    MERGE INTO S_INV_RECON_LDR_DTL T1
    USING (SELECT CHLD_CODE AS BIL_OPR, MST_CODE AS MST_OPR, PRD_GRP
             FROM VW_MST_OPR_PRD_COMBO
            ORDER BY 1) T2
    ON (T1.OPERATOR = T2.BIL_OPR AND T1.PRODUCT_GROUP = T2.PRD_GRP)
    WHEN MATCHED THEN
      UPDATE SET T1.MST_OPR_CODE = T2.MST_OPR;
  
    UPDATE S_INV_RECON_LDR_DTL T1
       SET T1.MST_OPR_CODE = OPERATOR
     WHERE T1.MST_OPR_CODE IS NULL;
  
    v_logmsg := 'CONFIGURING PROFILE RECORDS FOR NEW OPRS USING **ETISALATs RECON**';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_OPR_RECON_PROFILE
      (ID,
       MSTR_OPR_NAME,
       MST_OPR_CODE,
       ACTIVE_FLAG,
       MASTER_FLAG,
       TIMEZONE_FLAG,
       PROD_GROUP,
       CASH_FLOW,
       BILLING_METHOD,
       PROD_FLAG,
       SUB_DEL_FLAG,
       ORIG_DEST_FLAG,
       CTRY_OPER_FLAG,
       THRESH_TYPE_TQ,
       THRESH_VALUE_TQ,
       THRESH_PERC_TQ,
       THRESH_TYPE_RQ,
       THRESH_VALUE_RQ,
       THRESH_PERC_RQ,
       THRESH_OVRIDE,
       CREATED_BY,
       CREATED_DATE,
       LAST_MODIFIED_BY,
       LAST_MODIFIED_DATE,
       CURRENCY,
       TEMPLATE_ID,
       MAX_FILE_ROWS,
       MAX_FILE_SIZE_MB,
       REPORT_THRESH_PERC,
       REPORT_THRESH_VALUE,
       TIMEZONE_OPERATOR)
      WITH OPR_PRFL AS
       (SELECT *
          FROM T_SET_OPR_RECON_PROFILE
         WHERE MST_OPR_CODE = 'ZZZ'
           AND MSTR_OPR_NAME = 'TEMPLATE-ETI')
      SELECT T_SET_OPR_RECON_PROFILE_SEQ.NEXTVAL ID,
             MST.NAME                            MSTR_OPR_NAME,
             OPR.*
           FROM ITUPRDI.ORGANISATION MST,
             (SELECT DISTINCT LDR.MST_OPR_CODE,
                              PRF.ACTIVE_FLAG,
                              PRF.MASTER_FLAG,
                              PRF.TIMEZONE_FLAG,
                              PRF.PROD_GROUP,
                              PRF.CASH_FLOW,
                              PRF.BILLING_METHOD,
                              PRF.PROD_FLAG,
                              PRF.SUB_DEL_FLAG,
                              PRF.ORIG_DEST_FLAG,
                              PRF.CTRY_OPER_FLAG,
                              PRF.THRESH_TYPE_TQ,
                              PRF.THRESH_VALUE_TQ,
                              PRF.THRESH_PERC_TQ,
                              PRF.THRESH_TYPE_RQ,
                              PRF.THRESH_VALUE_RQ,
                              PRF.THRESH_PERC_RQ,
                              PRF.THRESH_OVRIDE,
                              PRF.CREATED_BY,
                              SYSDATE                 CREATED_DATE,
                              PRF.LAST_MODIFIED_BY,
                              SYSDATE                 LAST_MODIFIED_DATE,
                              PRF.CURRENCY,
                              PRF.TEMPLATE_ID,
                              PRF.MAX_FILE_ROWS,
                              PRF.MAX_FILE_SIZE_MB,
                              PRF.REPORT_THRESH_PERC,
                              PRF.REPORT_THRESH_VALUE,
                              PRF.TIMEZONE_OPERATOR
                FROM S_INV_RECON_LDR_DTL LDR, OPR_PRFL PRF
               WHERE PRF.PROD_GROUP = LDR.PRODUCT_GROUP
                 AND PRF.CASH_FLOW = LDR.CASH_FLOW
                 AND PRF.BILLING_METHOD = LDR.BILLING_METHOD
                 AND PRF.CURRENCY = LDR.CURRENCY
                 AND (LDR.MST_OPR_CODE, LDR.PRODUCT_GROUP, LDR.CASH_FLOW,
                      LDR.BILLING_METHOD, LDR.CURRENCY) NOT IN
                     (SELECT DISTINCT MST_OPR_CODE,
                                      PROD_GROUP,
                                      CASH_FLOW,
                                      BILLING_METHOD,
                                      CURRENCY
                        FROM T_SET_OPR_RECON_PROFILE)) OPR
       WHERE OPR.MST_OPR_CODE = MST.ID;
  
    v_logmsg := 'CONFIGURING PROFILE RECORDS FOR NEW OPRS USING **OPERATORs** ';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_OPR_RECON_PROFILE
      (ID,
       MSTR_OPR_NAME,
       MST_OPR_CODE,
       ACTIVE_FLAG,
       MASTER_FLAG,
       TIMEZONE_FLAG,
       PROD_GROUP,
       CASH_FLOW,
       BILLING_METHOD,
       PROD_FLAG,
       SUB_DEL_FLAG,
       ORIG_DEST_FLAG,
       CTRY_OPER_FLAG,
       THRESH_TYPE_TQ,
       THRESH_VALUE_TQ,
       THRESH_PERC_TQ,
       THRESH_TYPE_RQ,
       THRESH_VALUE_RQ,
       THRESH_PERC_RQ,
       THRESH_OVRIDE,
       CREATED_BY,
       LAST_MODIFIED_BY,
       CURRENCY,
       TEMPLATE_ID,
       MAX_FILE_ROWS,
       MAX_FILE_SIZE_MB,
       REPORT_THRESH_PERC,
       REPORT_THRESH_VALUE,
       TIMEZONE_OPERATOR,
       CREATED_DATE,
       LAST_MODIFIED_DATE)
      WITH OPR_PRFL AS
       (SELECT *
          FROM T_SET_OPR_RECON_PROFILE
         WHERE MST_OPR_CODE = 'ZZZ'
           AND MSTR_OPR_NAME = 'TEMPLATE-OPR'),
      LDR_OPRS AS
       (SELECT DISTINCT MST_OPR_CODE, PRODUCT_GROUP FROM S_INV_RECON_LDR_DTL)
      SELECT T_SET_OPR_RECON_PROFILE_SEQ.NEXTVAL ID,
             MST.NAME                            MSTR_OPR_NAME,
             LDR.MST_OPR_CODE,
             ACTIVE_FLAG,
             MASTER_FLAG,
             TIMEZONE_FLAG,
             PROD_GROUP,
             CASH_FLOW,
             BILLING_METHOD,
             PROD_FLAG,
             SUB_DEL_FLAG,
             ORIG_DEST_FLAG,
             CTRY_OPER_FLAG,
             THRESH_TYPE_TQ,
             THRESH_VALUE_TQ,
             THRESH_PERC_TQ,
             THRESH_TYPE_RQ,
             THRESH_VALUE_RQ,
             THRESH_PERC_RQ,
             THRESH_OVRIDE,
             CREATED_BY,
             LAST_MODIFIED_BY,
             CURRENCY,
             TEMPLATE_ID,
             MAX_FILE_ROWS,
             MAX_FILE_SIZE_MB,
             REPORT_THRESH_PERC,
             REPORT_THRESH_VALUE,
             TIMEZONE_OPERATOR,
             SYSDATE                             CREATED_DATE,
             SYSDATE                             LAST_MODIFIED_DATE
           FROM ITUPRDI.ORGANISATION MST, LDR_OPRS LDR, OPR_PRFL PRF
       WHERE (LDR.MST_OPR_CODE, PRF.PROD_GROUP, PRF.CASH_FLOW,
              PRF.BILLING_METHOD, PRF.CURRENCY) NOT IN
             (SELECT DISTINCT MST_OPR_CODE,
                              PROD_GROUP,
                              CASH_FLOW,
                              BILLING_METHOD,
                              CURRENCY
                FROM T_SET_OPR_RECON_PROFILE)
         AND MST.ID = LDR.MST_OPR_CODE
         AND PRF.PROD_GROUP = LDR.PRODUCT_GROUP;
  
    v_logmsg := 'SETTING  PARAMETER FLAGS FROM OPERATOR PROFILE INTO THE LOADER DATA';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE S_INV_RECON_LDR_DTL INV
       SET (ACTIVE_FLAG,
            MASTER_FLAG,
            TIMEZONE_FLAG,
            SUB_DEL_FLAG,
            ORIG_DEST_FLAG,
            CTRY_OPER_FLAG,
            THRESH_TYPE_TQ,
            THRESH_VALUE_TQ,
            THRESH_PERC_TQ,
            THRESH_TYPE_RQ,
            THRESH_VALUE_RQ,
            THRESH_PERC_RQ,
            THRESH_OVRIDE,
            PROD_FLAG,
            PRODUCT) =
           (SELECT DISTINCT ACTIVE_FLAG,
                            MASTER_FLAG,
                            TIMEZONE_FLAG,
                            SUB_DEL_FLAG,
                            ORIG_DEST_FLAG,
                            CTRY_OPER_FLAG,
                            THRESH_TYPE_TQ,
                            THRESH_VALUE_TQ,
                            THRESH_PERC_TQ,
                            THRESH_TYPE_RQ,
                            THRESH_VALUE_RQ,
                            THRESH_PERC_RQ,
                            THRESH_OVRIDE,
                            PROD_FLAG,
                            DECODE(PROD_FLAG, 'Y', PRODUCT, 'PRD')
              FROM T_SET_OPR_RECON_PROFILE PRF
             WHERE PRF.MST_OPR_CODE = INV.MST_OPR_CODE
               AND INV.PRODUCT_GROUP = PRF.PROD_GROUP
               AND INV.CASH_FLOW = PRF.CASH_FLOW
               AND INV.BILLING_METHOD = PRF.BILLING_METHOD
               AND INV.CURRENCY = PRF.CURRENCY)
     WHERE EXISTS (SELECT DISTINCT 1
              FROM T_SET_OPR_RECON_PROFILE PRF
             WHERE PRF.MST_OPR_CODE = INV.MST_OPR_CODE
               AND INV.PRODUCT_GROUP = PRF.PROD_GROUP
               AND INV.CASH_FLOW = PRF.CASH_FLOW
               AND INV.BILLING_METHOD = PRF.BILLING_METHOD
               AND INV.CURRENCY = PRF.CURRENCY);
  
    IF (SQL%ROWCOUNT <= 0) THEN
    
      v_logmsg   := '..........ERROR- NO PROFILE DATA FOUND FOR ANY OPERATORS *******  ';
      ERROR_CODE := 300;
      auditloggerProcedure(v_procName, v_logmsg);
      RETURN;
    
    END IF;
  
    update S_INV_RECON_LDR_DTL INV
       SET DIFF_TYPE = 'E', DIFF_DESCR = 'RECON IS DIS-ABLED'
    WHERE ACTIVE_FLAG != 'A' AND DIFF_TYPE IS NULL;
       AND DIFF_TYPE IS NULL;
  
    v_logmsg := 'SETTING DESTINATION NAME USING NEW DEST_MAPPING TABLE';
    auditloggerProcedure(v_procName, v_logmsg);
    BEGIN
      MERGE INTO S_INV_RECON_LDR_DTL T1
      USING (SELECT DISTINCT PRODUCT_GROUP,
                             OPR_CODE,
                             trim(OPR_NAME) OPR_NAME,
                             trim(OPR_DEST_NAME) OPR_DEST_NAME, ---- DEC30
                             INTEC_OPR_CODE,
                             INTEC_DEST_CODE,
                             trim(INTEC_DEST_NAME) INTEC_DEST_NAME,
                             UPPER(trim(INTEC_COUNTRY_NAME)) INTEC_COUNTRY_NAME -- Dec28 
               FROM T_IPMX_DEST_MAPPING
              WHERE MAPPING_STATUS = 'MAPPED'
                AND OPR_DEST_CODE = 'V10' --- Chg-32 --> temp use. 
                AND INTEC_DEST_NAME IS NOT NULL
                AND OPR_CODE in
                    (SELECT DISTINCT mst_opr_code FROM S_INV_RECON_LDR_DTL)) T2
      ON (trim(T1.DESCRIPTION) = trim(T2.OPR_DEST_NAME) AND T1.MST_OPR_CODE = T2.OPR_CODE AND T1.PRODUCT_GROUP = T2.PRODUCT_GROUP) ------ 10/12/2020
      WHEN MATCHED THEN
        UPDATE
           SET T1.ORIG_DEST_NAME = T2.INTEC_DEST_NAME,
               T1.COUNTRY_NAME   = T2.INTEC_COUNTRY_NAME
         WHERE ((PRODUCT_GROUP = 'TELE' AND
               TO_DATE(SUBSTR(TRAFFIC_PERIOD, 1, 10), 'YYYY/MM/DD') <
               '01-AUG-2025' -- Chg-32  
               ) OR PRODUCT_GROUP = 'DATA')
           AND T1.ORIG_DEST_NAME IS NULL;
    
    END;
  
    BEGIN
      MERGE INTO S_INV_RECON_LDR_DTL L
      USING (SELECT FRANCHISE,
                    PROD_GROUP,
                    OPERATOR_CODE,
                    COUNTRY_NAME,
                    DESTN_NAME
               FROM VW_ROUTE_CNTRY_DESTN) D
      ON (D.OPERATOR_CODE = L.MST_OPR_CODE AND TRIM(D.DESTN_NAME) = TRIM(L.DESCRIPTION))
      WHEN MATCHED THEN
        UPDATE
           SET COUNTRY_NAME = D.COUNTRY_NAME, ORIG_DEST_NAME = D.DESTN_NAME --- = DESCRIPTION   
         WHERE ORIG_DEST_NAME IS NULL
           AND DIFF_TYPE IS NULL
           AND PRODUCT_GROUP = 'TELE'
           AND TO_DATE(SUBSTR(TRAFFIC_PERIOD, 1, 10), 'YYYY/MM/DD') >=
               '01-AUG-2025'; -- Chg-32
    
    END;
    MERGE INTO S_INV_RECON_LDR_DTL T1
    USING (SELECT DISTINCT PRODUCT_GROUP,
                           OPR_CODE,
                           trim(OPR_NAME) OPR_NAME,
                           trim(OPR_DEST_NAME) OPR_DEST_NAME, ---- DEC30
                           INTEC_OPR_CODE,
                           INTEC_DEST_CODE,
                           trim(INTEC_DEST_NAME) INTEC_DEST_NAME,
                           UPPER(trim(INTEC_COUNTRY_NAME)) INTEC_COUNTRY_NAME -- Dec28 
             FROM T_IPMX_DEST_MAPPING
            WHERE MAPPING_STATUS = 'MAPPED'
              AND OPR_DEST_CODE IS NULL --- Chg-32 --> this field will be null from portal 
              AND INTEC_DEST_NAME IS NOT NULL
              AND OPR_CODE in
                  (SELECT DISTINCT mst_opr_code FROM S_INV_RECON_LDR_DTL)) T2
    ON (trim(T1.DESCRIPTION) = trim(T2.OPR_DEST_NAME) AND T1.MST_OPR_CODE = T2.OPR_CODE AND T1.PRODUCT_GROUP = T2.PRODUCT_GROUP) ------ 10/12/2020
    WHEN MATCHED THEN
      UPDATE
         SET T1.ORIG_DEST_NAME = T2.INTEC_DEST_NAME,
             T1.COUNTRY_NAME   = T2.INTEC_COUNTRY_NAME
       WHERE ((PRODUCT_GROUP = 'TELE' AND
             TO_DATE(SUBSTR(TRAFFIC_PERIOD, 1, 10), 'YYYY/MM/DD') >=
             '01-AUG-2025' -- Chg-32
             ) OR PRODUCT_GROUP = 'DATA')
         AND T1.ORIG_DEST_NAME IS NULL; -- not updated in prev qry 
  
    UPDATE S_INV_RECON_LDR_DTL
       SET DIFF_TYPE = 'E', DIFF_DESCR = 'NO DEST MAPPING'
     WHERE ORIG_DEST_NAME IS NULL
       AND DIFF_TYPE IS NULL;
  
    v_logmsg := 'LOADING GROUPED LOADER DATA INTO INVOICE SUMMARY SESSION TABLE';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
      FOR CTY IN (select FRANCHISE,
                         MST_OPR_CODE,
                         MAX(OPERATOR) OPERATOR,
                         TRAFFIC_PERIOD,
                         PRODUCT_GROUP,
                         PRODUCT,
                         INV_PRODUCT,
                         COUNTRY_NAME,
                         ORIG_DEST_NAME,
                         RATE,
                         ACTIVE_FLAG,
                         MASTER_FLAG,
                         TIMEZONE_FLAG,
                         SUB_DEL_FLAG,
                         ORIG_DEST_FLAG,
                         CTRY_OPER_FLAG,
                         THRESH_TYPE_TQ,
                         THRESH_VALUE_TQ,
                         THRESH_PERC_TQ,
                         THRESH_TYPE_RQ,
                         THRESH_VALUE_RQ,
                         THRESH_PERC_RQ,
                         PROD_FLAG,
                         THRESH_OVRIDE,
                         BILLING_METHOD,
                         STATEMENT_DIRECTION,
                         COMPONENT,
                         CASH_FLOW,
                         CURRENCY,
                         'D' RECORD_TYPE,
                         MAX(BATCH_ID) BATCH_ID,
                         MAX(BILLING_PERIOD) BILLING_PERIOD,
                         SUM(CALL_COUNT) CALL_COUNT,
                         SUM(CHARGED_USAGE) CHARGED_USAGE,
                         SUM(NET_AMOUNT) NET_AMOUNT,
                         MAX(COUNTRY_NAME) COUNTRY,
                         MAX(INVOICE_NUMBER) INVOICE_NUMBER,
                         SUM(NET_AMOUNT_ORIG) NET_AMOUNT_ORIG,
                         MAX(INV_RCVD_DATE) INV_RCVD_DATE
                    FROM S_INV_RECON_LDR_DTL
                   WHERE DIFF_TYPE IS NULL
                   GROUP BY FRANCHISE,
                            MST_OPR_CODE,
                            TRAFFIC_PERIOD,
                            PRODUCT_GROUP,
                            PRODUCT,
                            INV_PRODUCT,
                            COUNTRY_NAME,
                            ORIG_DEST_NAME,
                            RATE,
                            BILLING_METHOD,
                            STATEMENT_DIRECTION,
                            COMPONENT,
                            CASH_FLOW,
                            CURRENCY,
                            ACTIVE_FLAG,
                            MASTER_FLAG,
                            TIMEZONE_FLAG,
                            SUB_DEL_FLAG,
                            ORIG_DEST_FLAG,
                            CTRY_OPER_FLAG,
                            THRESH_TYPE_TQ,
                            THRESH_VALUE_TQ,
                            THRESH_PERC_TQ,
                            THRESH_TYPE_RQ,
                            THRESH_VALUE_RQ,
                            THRESH_PERC_RQ,
                            PROD_FLAG,
                            THRESH_OVRIDE) LOOP
      
        vs_cons_dest   := '';
        vn_dest_length := 0;
        vs_symbol      := ''; -- TO AVOID ADDING IN THE FIRST PLACE
      
        BEGIN
          FOR DSCR IN (SELECT DISTINCT DESCRIPTION
                         FROM S_INV_RECON_LDR_DTL
                        WHERE DIFF_TYPE IS NULL
                          AND DESCRIPTION IS NOT NULL
                          AND MST_OPR_CODE = CTY.MST_OPR_CODE
                          AND TRAFFIC_PERIOD = CTY.TRAFFIC_PERIOD
                          AND PRODUCT_GROUP = CTY.PRODUCT_GROUP
                          AND PRODUCT = CTY.PRODUCT
                          AND INV_PRODUCT = CTY.INV_PRODUCT
                          AND COUNTRY_NAME = CTY.COUNTRY_NAME
                          AND ORIG_DEST_NAME = CTY.ORIG_DEST_NAME
                          AND RATE = CTY.RATE
                          AND BILLING_METHOD = CTY.BILLING_METHOD
                          AND STATEMENT_DIRECTION = CTY.STATEMENT_DIRECTION
                          AND COMPONENT = CTY.COMPONENT
                          AND CASH_FLOW = CTY.CASH_FLOW
                          AND CURRENCY = CTY.CURRENCY) LOOP
            BEGIN
            
              vs_cty_dest := DSCR.DESCRIPTION;
            
              BEGIN
                SELECT trim(vs_cons_dest) || vs_symbol || trim(vs_cty_dest),
                       LENGTH(trim(vs_cons_dest))
                  INTO vs_cons_dest, vn_dest_length
                  FROM DUAL
                 WHERE vs_cty_dest IS NOT NULL;
              
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  NULL;
              END;
            
              IF (vn_dest_length > 3500) THEN
                DBMS_OUTPUT.PUT_LINE('WARNING = DESTN EXCEEDED THE LENGTH .DATA STRIPPED UPTO 4000 Chars ..........');
                EXIT; ----- to save data with descr when reached max size 
              END IF;
            
              vs_symbol := ' + '; --- to add from 2nd record 
            
            END;
          
          END LOOP;
        
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            NULL;
          
        END; -- DESTN LOOP
      
        BEGIN
          INSERT INTO S_INV_RECON_LDR_SUM
            (FRANCHISE,
             MST_OPR_CODE,
             OPERATOR,
             TRAFFIC_PERIOD,
             PRODUCT_GROUP,
             PRODUCT,
             INV_PRODUCT,
             COUNTRY_NAME,
             ORIG_DEST_NAME,
             RATE,
             ACTIVE_FLAG,
             MASTER_FLAG,
             TIMEZONE_FLAG,
             SUB_DEL_FLAG,
             ORIG_DEST_FLAG,
             CTRY_OPER_FLAG,
             THRESH_TYPE_TQ,
             THRESH_VALUE_TQ,
             THRESH_PERC_TQ,
             THRESH_TYPE_RQ,
             THRESH_VALUE_RQ,
             THRESH_PERC_RQ,
             PROD_FLAG,
             THRESH_OVRIDE,
             BATCH_ID,
             BILLING_PERIOD,
             BILLING_METHOD,
             STATEMENT_DIRECTION,
             COMPONENT,
             CASH_FLOW,
             CURRENCY,
             CALL_COUNT,
             CHARGED_USAGE,
             NET_AMOUNT,
             RECORD_TYPE,
             COUNTRY,
             INVOICE_NUMBER,
             NET_AMOUNT_ORIG,
             DESCRIPTION,
             INV_RCVD_DATE)
          VALUES
            (CTY.FRANCHISE,
             CTY.MST_OPR_CODE,
             CTY.OPERATOR,
             CTY.TRAFFIC_PERIOD,
             CTY.PRODUCT_GROUP,
             CTY.PRODUCT,
             CTY.INV_PRODUCT,
             CTY.COUNTRY_NAME,
             CTY.ORIG_DEST_NAME,
             CTY.RATE,
             CTY.ACTIVE_FLAG,
             CTY.MASTER_FLAG,
             CTY.TIMEZONE_FLAG,
             CTY.SUB_DEL_FLAG,
             CTY.ORIG_DEST_FLAG,
             CTY.CTRY_OPER_FLAG,
             CTY.THRESH_TYPE_TQ,
             CTY.THRESH_VALUE_TQ,
             CTY.THRESH_PERC_TQ,
             CTY.THRESH_TYPE_RQ,
             CTY.THRESH_VALUE_RQ,
             CTY.THRESH_PERC_RQ,
             CTY.PROD_FLAG,
             CTY.THRESH_OVRIDE,
             CTY.BATCH_ID,
             CTY.BILLING_PERIOD,
             CTY.BILLING_METHOD,
             CTY.STATEMENT_DIRECTION,
             CTY.COMPONENT,
             CTY.CASH_FLOW,
             CTY.CURRENCY,
             CTY.CALL_COUNT,
             CTY. CHARGED_USAGE,
             CTY.NET_AMOUNT,
             CTY.RECORD_TYPE,
             CTY.COUNTRY,
             CTY.INVOICE_NUMBER,
             CTY.NET_AMOUNT_ORIG,
             vs_cons_dest,
             CTY.INV_RCVD_DATE);
        
        END;
      
      END LOOP; -- CTY LOOP
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
      
    END;
  
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      v_logmsg := 'ERR_MSG-' || sqlerrm;
      auditloggerProcedure(v_procName, v_logmsg);
      error_code := 100;
      RETURN;
  END;

  PROCEDURE INTEC_DATA_PREPARE(ERROR_CODE OUT NUMBER) AS
  
    v_ldr_seq_no  number(5) := 0;
    vn_finsum_chk NUMBER := 0;
  
    v_logmsg   VARCHAR2(2000);
    v_procName VARCHAR2(50);
  
  BEGIN
    error_code := 200;
    v_procName := 'INTEC_DATA_PREPARE';
    v_logmsg   := 'START OF INTEC_DATA_PREPARE PROCEDURE';
    auditloggerProcedure(v_procName, v_logmsg);
  
    v_logmsg := 'LOADING CURSOR DATA INTO NEW S_FIN_SUM_CURSOR_TBL';
    auditloggerProcedure(v_procName, v_logmsg);
  
    DELETE FROM S_FINANCIAL_SUMMARY;
    DELETE FROM S_INV_RECON_INTEC_DTL;
    DELETE FROM S_INV_RECON_INTEC_SUM;
  
    BEGIN
    
      select count(1)
        into vn_finsum_chk
        from financial_summary
       where billing_period in
             (select id from billing_period where fed = '01-JUL-2024')
         AND ROWNUM < 10;
    
      INSERT INTO S_FIN_SUM_CURSOR_TBL
        (FRANCHISE,
         MST_OPR_CODE,
         CHLD_OPR,
         TRAFFIC_PERIOD,
         PRODUCT_GROUP,
         PROD_FLAG,
         TIMEZONE_FLAG,
         CASH_FLOW,
         BILLING_METHOD,
         STATEMENT_DIRECTION,
         CURRENCY,
         SUB_DEL_FLAG,
         COMPONENT,
         ORIG_DEST_FLAG)
        SELECT DISTINCT FRANCHISE,
                        MST_OPR_CODE,
                        CHLD_CODE as CHLD_OPR,
                        TRAFFIC_PERIOD,
                        PRODUCT_GROUP,
                        PROD_FLAG,
                        TIMEZONE_FLAG,
                        CASH_FLOW,
                        BILLING_METHOD,
                        STATEMENT_DIRECTION,
                        CURRENCY,
                        SUB_DEL_FLAG,
                        COMPONENT,
                        ORIG_DEST_FLAG
          FROM (SELECT CHLD_CODE, MST_CODE, PRD_GRP
                  FROM VW_MST_OPR_PRD_COMBO ---- view 
                 WHERE (MST_CODE, PRD_GRP) IN
                       (SELECT DISTINCT MST_OPR_CODE, PRODUCT_GROUP
                          FROM S_INV_RECON_LDR_DTL)) MST,
               (SELECT DISTINCT FRANCHISE,
                                MST_OPR_CODE,
                                TRAFFIC_PERIOD,
                                PRODUCT_GROUP,
                                PROD_FLAG,
                                TIMEZONE_FLAG,
                                CASH_FLOW,
                                BILLING_METHOD,
                                STATEMENT_DIRECTION,
                                CURRENCY,
                                SUB_DEL_FLAG,
                                COMPONENT,
                                ORIG_DEST_FLAG
                  FROM S_INV_RECON_LDR_DTL INV) INVO
         WHERE MST.MST_CODE = INVO.MST_OPR_CODE
           AND MST.PRD_GRP = INVO.PRODUCT_GROUP;
    
    END;
    --some operator does not have master code, hence considering child Codes
  
    v_logmsg := 'EXTRACT DETAILS FROM FIN SUMMARY and  TIME_ZONE TABLES INTO S_FINANCIAL_SUMMARY';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      DECLARE
        V_TRF_PRD_ID NUMBER(10) := 0;
        V_FIN_CNT    NUMBER(10) := 0;
        V_TZN_CNT    NUMBER(10) := 0;
        V_TRF_STDT   DATE;
      
        CURSOR FINSUM IS
          SELECT * FROM S_FIN_SUM_CURSOR_TBL;
      
        FINREC FINSUM%ROWTYPE;
      
      BEGIN
      
        OPEN FINSUM;
      
        LOOP
        
          FETCH FINSUM
            INTO FINREC;
        
          EXIT WHEN FINSUM%NOTFOUND;
        
          BEGIN
            SELECT max(ID), max(FED)
              into V_TRF_PRD_ID, V_TRF_STDT
              FROM BILLING_PERIOD -- VIEW V10 + V11 
             WHERE FK_ORGA_OPER = FINREC.CHLD_OPR
               AND FK_PGRP = FINREC.PRODUCT_GROUP
               AND FK_BMET = FINREC.BILLING_METHOD
               AND FK_SDIR = FINREC.STATEMENT_DIRECTION
               AND NAME = FINREC.TRAFFIC_PERIOD;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              NULL;
          END;
        
          IF (FINREC.PRODUCT_GROUP = 'DATA') -- Chg-32
           THEN
          
            BEGIN
            
              INSERT INTO S_FINANCIAL_SUMMARY
                (FRANCHISE,
                 GROUP_OPERATOR,
                 BILLING_OPERATOR,
                 AGREEMENT_NAAG_ANUM,
                 AGREEMENT_NAAG_BNUM,
                 AMOUNT,
                 TAXES,
                 BILLING_PERIOD,
                 BILLING_METHOD,
                 CALL_COUNT,
                 CASH_FLOW,
                 STATEMENT_DIRECTION,
                 CURRENCY,
                 CHARGED_USAGE,
                 BILLED_PRODUCT,
                 PRODUCT_GROUP,
                 RATING_COMPONENT,
                 RECON_NAAG_ANUM,
                 RECON_NAAG_BNUM,
                 TRAFFIC_PERIOD,
                 RATE,
                 BUSINESS_HIERARCHY,
                 RECORD_TYPE,
                 DISCOUNT_TEMPLATE)
                SELECT FIN.FRANCHISE,
                       FINREC.MST_OPR_CODE AS GROUP_OPERATOR,
                       BILLING_OPERATOR,
                       decode(FINREC.ORIG_DEST_FLAG,
                              'O',
                              FIN.AGREEMENT_NAAG_ANUM,
                              NULL) AGREEMENT_NAAG_ANUM,
                       decode(FINREC.ORIG_DEST_FLAG,
                              'D',
                              FIN.AGREEMENT_NAAG_BNUM,
                              NULL) AGREEMENT_NAAG_BNUM,
                       round(FIN.AMOUNT, 2) AMOUNT,
                       FIN.TAX_AMOUNT,
                       FIN.BILLING_PERIOD,
                       FIN.BILLING_METHOD,
                       FIN.CALL_COUNT,
                       FIN.CASH_FLOW,
                       FIN.STATEMENT_DIRECTION,
                       FIN.CURRENCY,
                       FIN.CHARGED_USAGE / 60 as CHARGED_USAGE,
                       DECODE(FINREC.PROD_FLAG,
                              'Y',
                              FIN.BILLED_PRODUCT,
                              'PRD') AS BILLED_PRODUCT, -- IF PROD TO MATCH, TAKE FROM FIN_SUM, ELSE FROM INV
                       FIN.PRODUCT_GROUP,
                       FINREC.COMPONENT AS RATING_COMPONENT,
                       decode(FINREC.ORIG_DEST_FLAG,
                              'O',
                              FIN.RECON_NAAG_ANUM,
                              NULL) RECON_NAAG_ANUM,
                       decode(FINREC.ORIG_DEST_FLAG,
                              'D',
                              FIN.RECON_NAAG_BNUM,
                              NULL) RECON_NAAG_BNUM,
                       FINREC.TRAFFIC_PERIOD,
                       DECODE(FIN.PRODUCT_GROUP,
                              'TELE',
                              FIN.UNIT_COST_USED,
                              FIN.FLAT_RATE_CHARGE) RATE,
                       FIN.BUSINESS_HIERARCHY,
                       'D' RECORD_TYPE,
                       FIN.DISCOUNT_TEMPLATE
                  FROM FINANCIAL_SUMMARY FIN
                 WHERE FIN.BILLING_OPERATOR = FINREC.CHLD_OPR
                   AND FIN.TRAFFIC_PERIOD = V_TRF_PRD_ID
                   AND FIN.PRODUCT_GROUP = FINREC.PRODUCT_GROUP
                   AND FIN.CASH_FLOW = FINREC.CASH_FLOW
                   AND FIN.BILLING_METHOD = FINREC.BILLING_METHOD
                   AND FIN.STATEMENT_DIRECTION = FINREC.STATEMENT_DIRECTION
                   AND FIN.CURRENCY = FINREC.CURRENCY --- CR2/point-5
                   AND FIN.AMOUNT != 0
                   AND FIN.PRODUCT_GROUP = 'DATA'
                   AND ((FINREC.SUB_DEL_FLAG = 'S' AND
                       FIN.BILLED_PRODUCT = 'SMSHUB') OR
                       (FINREC.SUB_DEL_FLAG = 'D' AND
                       FIN.BILLED_PRODUCT LIKE 'SMS%' AND
                       FIN.BILLED_PRODUCT != 'SMSHUB'));
            
              V_FIN_CNT := V_FIN_CNT + SQL%ROWCOUNT;
            
            END;
          
            DBMS_OUTPUT.PUT_LINE('..........COUNT FROM V10-DATA GRP   FINANCIAL SUMMARY TABLE *******' ||
                                 SQL%ROWCOUNT);
          
          ELSE
            -- ******* TELE  *******
          
            BEGIN
              INSERT INTO S_FINANCIAL_SUMMARY
                (FRANCHISE,
                 GROUP_OPERATOR,
                 BILLING_OPERATOR,
                 AGREEMENT_NAAG_ANUM,
                 AGREEMENT_NAAG_BNUM,
                 AMOUNT,
                 TAXES,
                 BILLING_PERIOD,
                 BILLING_METHOD,
                 CALL_COUNT,
                 CASH_FLOW,
                 STATEMENT_DIRECTION,
                 CURRENCY,
                 CHARGED_USAGE,
                 BILLED_PRODUCT,
                 PRODUCT_GROUP,
                 RATING_COMPONENT,
                 RECON_NAAG_ANUM,
                 RECON_NAAG_BNUM,
                 TRAFFIC_PERIOD,
                 RATE,
                 BUSINESS_HIERARCHY,
                 RECORD_TYPE,
                 DISCOUNT_TEMPLATE,
                 ORIG_DEST_NAME)
                SELECT FIN.FRANCHISE,
                       FINREC.MST_OPR_CODE AS GROUP_OPERATOR,
                       BILLING_OPERATOR,
                       decode(FINREC.ORIG_DEST_FLAG,
                              'O',
                              FIN.AGREEMENT_NAAG_ANUM,
                              NULL) AGREEMENT_NAAG_ANUM,
                       decode(FINREC.ORIG_DEST_FLAG,
                              'D',
                              FIN.AGREEMENT_NAAG_BNUM,
                              NULL) AGREEMENT_NAAG_BNUM,
                       round(FIN.AMOUNT, 2) AMOUNT,
                       FIN.TAX_AMOUNT,
                       FIN.BILLING_PERIOD,
                       FIN.BILLING_METHOD,
                       FIN.CALL_COUNT,
                       FIN.CASH_FLOW,
                       FIN.STATEMENT_DIRECTION,
                       FIN.CURRENCY,
                       FIN.CHARGED_USAGE / 60 as CHARGED_USAGE,
                       DECODE(FINREC.PROD_FLAG,
                              'Y',
                              FIN.BILLED_PRODUCT,
                              'PRD') AS BILLED_PRODUCT, -- IF PROD TO MATCH, TAKE FROM FIN_SUM, ELSE FROM INV
                       FIN.PRODUCT_GROUP,
                       FINREC.COMPONENT AS RATING_COMPONENT, -- HASAN 20/01/2020 - DONT MATCH
                       decode(FINREC.ORIG_DEST_FLAG,
                              'O',
                              FIN.RECON_NAAG_ANUM,
                              NULL) RECON_NAAG_ANUM,
                       decode(FINREC.ORIG_DEST_FLAG,
                              'D',
                              FIN.RECON_NAAG_BNUM,
                              NULL) RECON_NAAG_BNUM,
                       FINREC.TRAFFIC_PERIOD,
                       DECODE(FIN.PRODUCT_GROUP,
                              'TELE',
                              FIN.UNIT_COST_USED,
                              FIN.FLAT_RATE_CHARGE) RATE,
                       FIN.BUSINESS_HIERARCHY,
                       'D' RECORD_TYPE,
                       FIN.DISCOUNT_TEMPLATE,
                       FIN.CARRIER_DESTINATION_NAME AS ORIG_DEST_NAME -- Chg-32
                       FROM ITUPRDI.FINANCIAL_SUMMARY@V10OP2V11OP FIN
                 WHERE FIN.BILLING_OPERATOR = FINREC.CHLD_OPR
                   AND FIN.TRAFFIC_PERIOD = V_TRF_PRD_ID
                      ---AND     FIN.BILLING_PERIOD             = V_TRF_PRD_ID   
                   AND FIN.PRODUCT_GROUP = FINREC.PRODUCT_GROUP
                   AND FIN.CASH_FLOW = FINREC.CASH_FLOW
                   AND FIN.BILLING_METHOD = FINREC.BILLING_METHOD
                   AND FIN.STATEMENT_DIRECTION = FINREC.STATEMENT_DIRECTION
                   AND FIN.CURRENCY = FINREC.CURRENCY --- CR2/point-5
                   AND FIN.AMOUNT != 0
                            AND FIN.PRODUCT_GROUP = 'TELE';            ---- OR

                  --     ( FINREC.SUB_DEL_FLAG = 'S' AND FIN.BILLED_PRODUCT = 'SMSHUB' ) OR
                  --     ( FINREC.SUB_DEL_FLAG = 'D' AND FIN.BILLED_PRODUCT LIKE 'SMS%' AND FIN.BILLED_PRODUCT != 'SMSHUB'  ) ) ;


            
              V_FIN_CNT := V_FIN_CNT + SQL%ROWCOUNT;
            
            END;
          END IF;
        
          v_logmsg := 'FINANCIAL SUMMARY Data Loaded';
          auditloggerProcedure(v_procName, v_logmsg);
        
          BEGIN
            INSERT INTO S_FINANCIAL_SUMMARY
              (FRANCHISE,
               GROUP_OPERATOR,
               BILLING_OPERATOR,
               AGREEMENT_NAAG_ANUM,
               AGREEMENT_NAAG_BNUM,
               AMOUNT,
               TAXES,
               BILLING_PERIOD,
               BILLING_METHOD,
               CALL_COUNT,
               CASH_FLOW,
               STATEMENT_DIRECTION,
               CURRENCY,
               CHARGED_USAGE,
               BILLED_PRODUCT,
               PRODUCT_GROUP,
               RATING_COMPONENT,
               RECON_NAAG_ANUM,
               RECON_NAAG_BNUM,
               TRAFFIC_PERIOD,
               RATE,
               BUSINESS_HIERARCHY,
               DISCOUNT_TEMPLATE,
               RECORD_TYPE)
              SELECT DISTINCT TZN.FRANCHISE,
                              FINREC.MST_OPR_CODE AS GROUP_OPERATOR,
                              BILLING_OPERATOR,
                              decode(FINREC.ORIG_DEST_FLAG,
                                     'O',
                                     TZN.AGREEMENT_NAAG_ANUM,
                                     NULL) AGREEMENT_NAAG_ANUM,
                              decode(FINREC.ORIG_DEST_FLAG,
                                     'D',
                                     TZN.AGREEMENT_NAAG_BNUM,
                                     NULL) AGREEMENT_NAAG_BNUM,
                              round(TZN.AMOUNT, 2) AMOUNT,
                              0 as TAXES,
                              TZN.BILLING_PERIOD,
                              TZN.BILLING_METHOD,
                              TZN.CALL_COUNT,
                              TZN.CASH_FLOW,
                              TZN.STATEMENT_DIRECTION,
                              TZN.CURRENCY,
                              TZN.CHARGED_USAGE / 60 as CHARGED_USAGE,
                              DECODE(FINREC.PROD_FLAG,
                                     'Y',
                                     TZN.BILLED_PRODUCT,
                                     'PRD') AS BILLED_PRODUCT, -- IF PROD TO MATCH, TAKE FROM FIN_SUM, ELSE FROM INV
                              TZN.PRODUCT_GROUP,
                              FINREC.COMPONENT AS RATING_COMPONENT, -- HASAN 20/01/2020 - DONT MATCH
                              decode(FINREC.ORIG_DEST_FLAG,
                                     'O',
                                     TZN.RECON_NAAG_ANUM,
                                     NULL) RECON_NAAG_ANUM,
                              decode(FINREC.ORIG_DEST_FLAG,
                                     'D',
                                     TZN.RECON_NAAG_BNUM,
                                     NULL) RECON_NAAG_BNUM,
                              FINREC.TRAFFIC_PERIOD,
                              DECODE(TZN.PRODUCT_GROUP,
                                     'TELE',
                                     TZN.UNIT_COST_USED,
                                     TZN.FLAT_RATE_CHARGE) RATE,
                              RATE_NAME as BUSINESS_HIERARCHY,
                              NULL as DISCOUNT_TEMPLATE,
                              'D' AS RECORD_TYPE
                FROM T_TZONE_FINSUM_ADJUSTED TZN
               WHERE TZN.BILLING_OPERATOR = FINREC.CHLD_OPR
                 AND TZN.PRODUCT_GROUP = FINREC.PRODUCT_GROUP
                    ---- AND     TZN.TRAFFIC_PERIOD             = V_TRF_PRD_ID    --- not available
                 AND TZN.CASH_FLOW = FINREC.CASH_FLOW
                 AND TZN.BILLING_METHOD = FINREC.BILLING_METHOD
                 AND TZN.STATEMENT_DIRECTION = FINREC.STATEMENT_DIRECTION
                 AND TZN.CURRENCY = FINREC.CURRENCY --- CR2/point-5
                 AND (FINREC.PRODUCT_GROUP = 'TELE' OR
                     (FINREC.SUB_DEL_FLAG = 'S' AND
                     TZN.BILLED_PRODUCT = 'SMSHUB') OR
                     (FINREC.SUB_DEL_FLAG = 'D' AND
                     TZN.BILLED_PRODUCT IN ('SMS', 'SMSHDL', 'SMSR')))
                 AND FINREC.TIMEZONE_FLAG = 'Y'
                 AND TZN.AMOUNT != 0
                 AND TZN.BILLING_DATE = TO_DATE(V_TRF_STDT); -- trf prd first date is stored in billing-date field in time zone gen procedure
          
            V_TZN_CNT := V_TZN_CNT + SQL%ROWCOUNT;
          
          END;
        
        END LOOP;
      
        CLOSE FINSUM;
      
      END;
    
    END;
  
    v_logmsg := 'TRANSFERRING FIN SUMMARY  DETAILS INTO INTEC DETAIL TABLE';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
      INSERT INTO S_INV_RECON_INTEC_DTL
        (FRANCHISE,
         GROUP_OPERATOR,
         AGREEMENT_NAAG_ANUM,
         AGREEMENT_NAAG_BNUM,
         AMOUNT,
         TAXES,
         BILLING_PERIOD,
         BILLING_METHOD,
         CALL_COUNT,
         CASH_FLOW,
         STATEMENT_DIRECTION,
         CURRENCY,
         CHARGED_USAGE,
         BILLED_PRODUCT,
         PRODUCT_GROUP,
         RATING_COMPONENT,
         RECON_NAAG_ANUM,
         RECON_NAAG_BNUM,
         TRAFFIC_PERIOD,
         RATE,
         BUSINESS_HIERARCHY,
         RECORD_TYPE,
         DISCOUNT_TEMPLATE,
         ORIG_DEST_NAME) --- Chg-32
        SELECT FRANCHISE,
               GROUP_OPERATOR,
               AGREEMENT_NAAG_ANUM,
               AGREEMENT_NAAG_BNUM,
               SUM(AMOUNT),
               SUM(TAXES),
               max(BILLING_PERIOD),
               BILLING_METHOD,
               SUM(CALL_COUNT),
               CASH_FLOW,
               STATEMENT_DIRECTION,
               CURRENCY,
               SUM(CHARGED_USAGE),
               BILLED_PRODUCT,
               PRODUCT_GROUP,
               RATING_COMPONENT,
               RECON_NAAG_ANUM,
               RECON_NAAG_BNUM,
               TRAFFIC_PERIOD,
               RATE,
               BUSINESS_HIERARCHY,
               RECORD_TYPE,
               listagg(DISCOUNT_TEMPLATE, ' , ') within group(order by DISCOUNT_TEMPLATE) DISCOUNT_TEMPLATE,
               ORIG_DEST_NAME
          FROM (SELECT FRANCHISE,
                       GROUP_OPERATOR,
                       AGREEMENT_NAAG_ANUM,
                       AGREEMENT_NAAG_BNUM,
                       SUM(AMOUNT) AMOUNT,
                       SUM(TAXES) TAXES,
                       MAX(BILLING_PERIOD) as BILLING_PERIOD,
                       BILLING_METHOD,
                       SUM(CALL_COUNT) CALL_COUNT,
                       CASH_FLOW,
                       STATEMENT_DIRECTION,
                       CURRENCY,
                       SUM(CHARGED_USAGE) CHARGED_USAGE,
                       BILLED_PRODUCT,
                       PRODUCT_GROUP,
                       RATING_COMPONENT,
                       RECON_NAAG_ANUM,
                       RECON_NAAG_BNUM,
                       TRAFFIC_PERIOD,
                       RATE,
                       BUSINESS_HIERARCHY,
                       RECORD_TYPE,
                       DISCOUNT_TEMPLATE,
                       ORIG_DEST_NAME
                  FROM S_FINANCIAL_SUMMARY
                 GROUP BY FRANCHISE,
                          GROUP_OPERATOR,
                          AGREEMENT_NAAG_ANUM,
                          AGREEMENT_NAAG_BNUM,
                          BILLING_METHOD,
                          CASH_FLOW,
                          STATEMENT_DIRECTION,
                          CURRENCY,
                          BILLED_PRODUCT,
                          PRODUCT_GROUP,
                          RATING_COMPONENT,
                          RECON_NAAG_ANUM,
                          RECON_NAAG_BNUM,
                          TRAFFIC_PERIOD,
                          RATE,
                          BUSINESS_HIERARCHY,
                          RECORD_TYPE,
                          DISCOUNT_TEMPLATE,
                          ORIG_DEST_NAME
                HAVING SUM(AMOUNT) != 0 --- Chg-17 : URE-FEB-24 -- some trans reversed but showing in the recon report
                )
         GROUP BY FRANCHISE,
                  GROUP_OPERATOR,
                  AGREEMENT_NAAG_ANUM,
                  AGREEMENT_NAAG_BNUM,
                  BILLING_METHOD,
                  CASH_FLOW,
                  STATEMENT_DIRECTION,
                  CURRENCY,
                  BILLED_PRODUCT,
                  PRODUCT_GROUP,
                  RATING_COMPONENT,
                  RECON_NAAG_ANUM,
                  RECON_NAAG_BNUM,
                  TRAFFIC_PERIOD,
                  RATE,
                  BUSINESS_HIERARCHY,
                  RECORD_TYPE,
				  ORIG_DEST_NAME;
    
    END;
  
    UPDATE S_INV_RECON_INTEC_DTL FIN
       SET AGREEMENT_NAAG_ANUM = NULL, AGREEMENT_NAAG_BNUM = NULL
     WHERE (GROUP_OPERATOR, PRODUCT_GROUP, CASH_FLOW, BILLING_METHOD) IN
           (SELECT DISTINCT MST_OPR_CODE,
                            PROD_GROUP,
                            CASH_FLOW,
                            BILLING_METHOD
              FROM T_SET_OPR_RECON_PROFILE
             WHERE CTRY_OPER_FLAG = 'C'
               AND ACTIVE_FLAG = 'A');
  
    v_logmsg := 'V11: SETTING COUNTRY USING NEW ROUTE REF DATA FOR ETISALAT SIDE AS WELL';
    auditloggerProcedure(v_procName, v_logmsg);
  
    -- As the country names also different for some rows between v10 and v11, ctry names to be assigned for etis side using route data for v11 invoices. 
  
    BEGIN
      MERGE INTO S_INV_RECON_INTEC_DTL L
      USING (SELECT FRANCHISE,
                    PROD_GROUP,
                    OPERATOR_CODE,
                    COUNTRY_NAME,
                    DESTN_NAME
               FROM VW_ROUTE_CNTRY_DESTN) D
      ON (D.OPERATOR_CODE = L.GROUP_OPERATOR AND TRIM(D.DESTN_NAME) = TRIM(L.ORIG_DEST_NAME))
      WHEN MATCHED THEN
        UPDATE
           SET COUNTRY_NAME = D.COUNTRY_NAME
         WHERE PRODUCT_GROUP = 'TELE'
           AND TO_DATE(SUBSTR(TRAFFIC_PERIOD, 1, 10), 'YYYY/MM/DD') >=
               '01-AUG-2025'; -- Chg-32
    
    END;
  
    --- FOR ORIGIN BASED ------UPPER
  
    UPDATE S_INV_RECON_INTEC_DTL FIN
       SET COUNTRY_NAME =
           (select DISTINCT UPPER(NAME)
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION CTRY     --- Dec28
             where substr(FIN.AGREEMENT_NAAG_ANUM, 1, 3) = CTRY.ID
               AND CTRY.EXPIRY_DATE > SYSDATE
               AND CTRY.COUNTRY_IND = 'Y')
     WHERE exists (select *
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION CTRY
             where substr(FIN.AGREEMENT_NAAG_ANUM, 1, 3) = CTRY.ID
               AND CTRY.EXPIRY_DATE > SYSDATE
               AND CTRY.COUNTRY_IND = 'Y')
       AND (GROUP_OPERATOR, PRODUCT_GROUP, CASH_FLOW, BILLING_METHOD,
            CURRENCY) IN (SELECT DISTINCT MST_OPR_CODE,
                                          PROD_GROUP,
                                          CASH_FLOW,
                                          BILLING_METHOD,
                                          CURRENCY
                            FROM T_SET_OPR_RECON_PROFILE
                           WHERE ORIG_DEST_FLAG = 'O'
                             AND ACTIVE_FLAG = 'A')
       AND FIN.AGREEMENT_NAAG_ANUM IS NOT NULL
       AND FIN.COUNTRY_NAME IS NULL;
  
    UPDATE S_INV_RECON_INTEC_DTL FIN
       SET COUNTRY_NAME =
           (select DISTINCT UPPER(NAME)
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION CTRY     --- Dec28
             where substr(FIN.AGREEMENT_NAAG_ANUM, 1, 4) = CTRY.ID
               AND CTRY.EXPIRY_DATE > SYSDATE
               AND CTRY.COUNTRY_IND = 'Y')
     WHERE exists (select *
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION CTRY
             where substr(FIN.AGREEMENT_NAAG_ANUM, 1, 4) = CTRY.ID
               AND CTRY.EXPIRY_DATE > SYSDATE
               AND CTRY.COUNTRY_IND = 'Y')
       AND (GROUP_OPERATOR, PRODUCT_GROUP, CASH_FLOW, BILLING_METHOD,
            CURRENCY) IN (SELECT DISTINCT MST_OPR_CODE,
                                          PROD_GROUP,
                                          CASH_FLOW,
                                          BILLING_METHOD,
                                          CURRENCY
                            FROM T_SET_OPR_RECON_PROFILE
                           WHERE ORIG_DEST_FLAG = 'O'
                             AND ACTIVE_FLAG = 'A')
       AND FIN.AGREEMENT_NAAG_ANUM IS NOT NULL
       AND FIN.PRODUCT_GROUP = 'DATA'
       AND FIN.COUNTRY_NAME IS NULL;
  
    UPDATE S_INV_RECON_INTEC_DTL FIN
       SET COUNTRY_NAME =
           (select DISTINCT UPPER(NAME)
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION CTRY      -- Dec28
             where substr(FIN.AGREEMENT_NAAG_BNUM, 1, 3) = CTRY.ID
               AND CTRY.EXPIRY_DATE > SYSDATE
               AND CTRY.COUNTRY_IND = 'Y')
     WHERE exists (select *
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION CTRY
             where substr(FIN.AGREEMENT_NAAG_BNUM, 1, 3) = CTRY.ID
               AND CTRY.EXPIRY_DATE > SYSDATE
               AND CTRY.COUNTRY_IND = 'Y')
       AND (GROUP_OPERATOR, PRODUCT_GROUP, CASH_FLOW, BILLING_METHOD,
            CURRENCY) IN (SELECT DISTINCT MST_OPR_CODE,
                                          PROD_GROUP,
                                          CASH_FLOW,
                                          BILLING_METHOD,
                                          CURRENCY
                            FROM T_SET_OPR_RECON_PROFILE
                           WHERE ORIG_DEST_FLAG = 'D'
                             AND ACTIVE_FLAG = 'A')
       AND FIN.AGREEMENT_NAAG_BNUM IS NOT NULL
       AND FIN.COUNTRY_NAME IS NULL;
  
    UPDATE S_INV_RECON_INTEC_DTL FIN
       SET COUNTRY_NAME =
           (select DISTINCT UPPER(NAME)
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION CTRY      -- Dec28
             where substr(FIN.AGREEMENT_NAAG_BNUM, 1, 4) = CTRY.ID
               AND CTRY.EXPIRY_DATE > SYSDATE
               AND CTRY.COUNTRY_IND = 'Y')
     WHERE exists (select *
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION CTRY
             where substr(FIN.AGREEMENT_NAAG_BNUM, 1, 4) = CTRY.ID
               AND CTRY.EXPIRY_DATE > SYSDATE
               AND CTRY.COUNTRY_IND = 'Y')
       AND (GROUP_OPERATOR, PRODUCT_GROUP, CASH_FLOW, BILLING_METHOD,
            CURRENCY) IN (SELECT DISTINCT MST_OPR_CODE,
                                          PROD_GROUP,
                                          CASH_FLOW,
                                          BILLING_METHOD,
                                          CURRENCY
                            FROM T_SET_OPR_RECON_PROFILE
                           WHERE ORIG_DEST_FLAG = 'D'
                             AND ACTIVE_FLAG = 'A')
       AND FIN.AGREEMENT_NAAG_BNUM IS NOT NULL
       AND FIN.PRODUCT_GROUP = 'DATA'
       AND FIN.COUNTRY_NAME IS NULL;
  
    v_logmsg := 'SETTING COUNTRY NAMES FOR ITFS and HCD DESTNS , BASED ON LAST 3 CHARS OF DEST_ID';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
      MERGE INTO S_INV_RECON_INTEC_DTL T1
      USING (select DISTINCT CTRY.ID COUNTRY_CODE,
                             UPPER(CTRY.NAME) COUNTRY_NAME,
                             DEST.NAME DEST_NAME ---Dec28
                    FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION CTRY,
                         ITUPRDI.NETWORK_ADDRESS_AGGREGATION DEST
              where DEST.EXPIRY_DATE > SYSDATE
                AND DEST.COUNTRY_IND != 'Y'
                AND CTRY.EXPIRY_DATE > SYSDATE
                AND CTRY.COUNTRY_IND = 'Y'
                AND TRIM(CTRY.ID) =
                    substr(DEST.ID, length(TRIM(DEST.ID)) - 2, 3)
                AND substr(DEST.ID, 1, 3) not in
                    (select ID
                       from NETWORK_ADDRESS_AGGREGATION cty
                      where COUNTRY_IND = 'Y')) T2
      ON (trim(T1.ORIG_DEST_NAME) = trim(T2.DEST_NAME))
      WHEN MATCHED THEN
        UPDATE
           SET T1.COUNTRY_NAME = T2.COUNTRY_NAME
         WHERE T1.COUNTRY_NAME IS NULL;
    
    END;
  
    v_logmsg := 'SETTING DESTN NAME USING DESTN CODE FOR DEST BASED RECON';
    auditloggerProcedure(v_procName, v_logmsg);
  
    --- FOR ORIGIN BASED ---- 
  
    UPDATE S_INV_RECON_INTEC_DTL FIN
       SET ORIG_DEST_NAME =
           (select DISTINCT NAME
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION DEST     --- UPPER
             where FIN.AGREEMENT_NAAG_ANUM = DEST.ID
               AND DEST.EXPIRY_DATE > SYSDATE
               AND DEST.COUNTRY_IND != 'Y')
     WHERE exists (select *
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION DEST
             where FIN.AGREEMENT_NAAG_ANUM = DEST.ID
               AND DEST.EXPIRY_DATE > SYSDATE
               AND DEST.COUNTRY_IND != 'Y')
       AND (GROUP_OPERATOR, PRODUCT_GROUP, CASH_FLOW, BILLING_METHOD,
            CURRENCY) IN (SELECT DISTINCT MST_OPR_CODE,
                                          PROD_GROUP,
                                          CASH_FLOW,
                                          BILLING_METHOD,
                                          CURRENCY
                            FROM T_SET_OPR_RECON_PROFILE
                           WHERE ORIG_DEST_FLAG = 'O'
                             AND CTRY_OPER_FLAG = 'D'
                             AND ACTIVE_FLAG = 'A')
       AND FIN.AGREEMENT_NAAG_ANUM IS NOT NULL
       AND FIN.ORIG_DEST_NAME IS NULL;
  
    --- FOR DESTINATION  BASED ----  
  
    UPDATE S_INV_RECON_INTEC_DTL FIN
       SET ORIG_DEST_NAME =
           (select DISTINCT NAME
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION DEST     --- UPPER
             where FIN.AGREEMENT_NAAG_BNUM = DEST.ID
               AND DEST.EXPIRY_DATE > SYSDATE)
     WHERE exists (select *
                   FROM ITUPRDI.NETWORK_ADDRESS_AGGREGATION DEST
             where FIN.AGREEMENT_NAAG_BNUM = DEST.ID
               AND DEST.EXPIRY_DATE > SYSDATE)
       AND (GROUP_OPERATOR, PRODUCT_GROUP, CASH_FLOW, BILLING_METHOD,
            CURRENCY) IN (SELECT MST_OPR_CODE,
                                 PROD_GROUP,
                                 CASH_FLOW,
                                 BILLING_METHOD,
                                 CURRENCY
                            FROM T_SET_OPR_RECON_PROFILE
                           WHERE ORIG_DEST_FLAG = 'D'
                             AND CTRY_OPER_FLAG = 'D'
                             AND ACTIVE_FLAG = 'A')
       AND FIN.AGREEMENT_NAAG_BNUM IS NOT NULL
       AND FIN.ORIG_DEST_NAME IS NULL; -- Chg-32 -- for v10 only . for v11 , already from fin-summ
  
    ---- Below is Added on 29-Apr-2021 to present invalid NAAG codes in the Main Report For User to Fix ----Working Fine-----
  
    UPDATE S_INV_RECON_INTEC_DTL
       SET ORIG_DEST_NAME = decode(NVL(AGREEMENT_NAAG_ANUM, 'B'),
                                   'B',
                                   AGREEMENT_NAAG_BNUM,
                                   AGREEMENT_NAAG_ANUM) ||
                            ' - ** Not In NAAG **'
     WHERE (AGREEMENT_NAAG_ANUM IS NOT NULL OR
           AGREEMENT_NAAG_BNUM IS NOT NULL)
       AND ORIG_DEST_NAME IS NULL;
  
    UPDATE S_INV_RECON_INTEC_DTL
       SET COUNTRY_NAME = '**INVALID_CTRY**'
     WHERE (AGREEMENT_NAAG_ANUM IS NOT NULL OR
           AGREEMENT_NAAG_BNUM IS NOT NULL)
       AND COUNTRY_NAME IS NULL;
  
    -------- ********* HANDLING VBR RECORDS HERE  ******** -----------
  
    UPDATE S_INV_RECON_INTEC_DTL
       SET COUNTRY_NAME = 'VBR', RECORD_TYPE = 'VN', RATE = RATE * (-1)
    ---WHERE  AMOUNT < 0   AND BUSINESS_HIERARCHY = 'VBR'   --- SD5134127( PMT ) 
     WHERE AMOUNT <= 0
       AND BUSINESS_HIERARCHY = 'VBR'
       AND DISCOUNT_TEMPLATE IS NOT NULL;
  
    v_logmsg := 'SETTING FLAGS FOR ORIGINAL RECORDS RELATED TO VBR REVERSED RECORDS';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
      MERGE INTO S_INV_RECON_INTEC_DTL DES
      USING (SELECT FRANCHISE,
                    GROUP_OPERATOR,
                    TRAFFIC_PERIOD,
                    PRODUCT_GROUP,
                    COUNTRY_NAME,
                    ORIG_DEST_NAME,
                    CASH_FLOW,
                    BILLING_METHOD,
                    STATEMENT_DIRECTION,
                    RATING_COMPONENT,
                    RATE,
                    CURRENCY,
                    listagg(DISCOUNT_TEMPLATE, ' , ') within group(order by DISCOUNT_TEMPLATE) DISCOUNT_TEMPLATE
               FROM (SELECT DISTINCT FRANCHISE,
                                     GROUP_OPERATOR,
                                     TRAFFIC_PERIOD,
                                     PRODUCT_GROUP,
                                     COUNTRY_NAME,
                                     ORIG_DEST_NAME,
                                     CASH_FLOW,
                                     BILLING_METHOD,
                                     STATEMENT_DIRECTION,
                                     RATING_COMPONENT,
                                     RATE,
                                     CURRENCY,
                                     DISCOUNT_TEMPLATE
                       FROM S_INV_RECON_INTEC_DTL
                      WHERE RECORD_TYPE = 'VN'
                     --- AND DISCOUNT_TEMPLATE is not null AND  BUSINESS_HIERARCHY = 'VBR' and AMOUNT < 0   --- SD5134127( PMT )
                     )
              GROUP BY FRANCHISE,
                       GROUP_OPERATOR,
                       TRAFFIC_PERIOD,
                       PRODUCT_GROUP,
                       COUNTRY_NAME,
                       ORIG_DEST_NAME,
                       CASH_FLOW,
                       BILLING_METHOD,
                       STATEMENT_DIRECTION,
                       RATING_COMPONENT,
                       RATE,
                       CURRENCY) SRC
      ON (DES.GROUP_OPERATOR = SRC.GROUP_OPERATOR AND DES.TRAFFIC_PERIOD = SRC.TRAFFIC_PERIOD AND DES.PRODUCT_GROUP = SRC.PRODUCT_GROUP AND
      ----INV.PRODUCT                 =  FIN.BILLED_PRODUCT      AND  
      DES.ORIG_DEST_NAME = SRC.ORIG_DEST_NAME AND DES.CASH_FLOW = SRC.CASH_FLOW AND DES.CURRENCY = SRC.CURRENCY AND DES.BILLING_METHOD = SRC.BILLING_METHOD AND DES.STATEMENT_DIRECTION = SRC.STATEMENT_DIRECTION AND DES.RATING_COMPONENT = SRC.RATING_COMPONENT AND DES.RATE = abs(SRC.RATE))
      WHEN MATCHED THEN
        UPDATE
           SET DES.COUNTRY_NAME      = 'VBR',
               DES.RECORD_TYPE       = 'VD',
               DES.DISCOUNT_TEMPLATE = SRC.DISCOUNT_TEMPLATE
         WHERE RECORD_TYPE = 'D'; ---> All Matching DlySum Records Including TimeZone
    
    END;
  
    DELETE FROM S_INV_RECON_INTEC_DTL
     WHERE RECORD_TYPE = 'VD'
       AND DISCOUNT_TEMPLATE IS NOT NULL
       AND BUSINESS_HIERARCHY LIKE 'TO_BE%';
  
    COMMIT;
  
    UPDATE S_INV_RECON_INTEC_DTL
       SET COUNTRY_NAME   = 'VBR',
           RECORD_TYPE    = 'VR',
           ORIG_DEST_NAME = DECODE(ORIG_DEST_NAME,
                                   NULL,
                                   'VBR FLAT AMOUNT',
                                   ORIG_DEST_NAME)
     WHERE DISCOUNT_TEMPLATE IS NOT NULL
       AND BUSINESS_HIERARCHY = 'VBR'
       AND AMOUNT > 0;
  
    ------- VBR CHANGE UP TO THIS ------------------
  
    v_logmsg := 'CONSOLIDATE S_FIN_SUM DATA AND LOAD INTO INTEC SUMMARY TABLE';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO S_INV_RECON_INTEC_SUM
      (FRANCHISE,
       GROUP_OPERATOR,
       TRAFFIC_PERIOD,
       PRODUCT_GROUP,
       BILLED_PRODUCT,
       RECORD_TYPE,
       DISCOUNT_TEMPLATE,
       COUNTRY_NAME,
       ORIG_DEST_NAME,
       RATE,
       BILLING_PERIOD,
       BILLING_METHOD,
       STATEMENT_DIRECTION,
       RATING_COMPONENT,
       CASH_FLOW,
       CURRENCY,
       CALL_COUNT,
       CHARGED_USAGE,
       AMOUNT,
       TAX_AMOUNT)
      SELECT FIN.FRANCHISE,
             FIN.GROUP_OPERATOR,
             FIN.TRAFFIC_PERIOD,
             FIN.PRODUCT_GROUP,
             MIN(FIN.BILLED_PRODUCT),
             RECORD_TYPE,
             DISCOUNT_TEMPLATE,
             COUNTRY_NAME,
             ORIG_DEST_NAME,
             RATE,
             MAX(FIN.BILLING_PERIOD),
             FIN.BILLING_METHOD,
             FIN.STATEMENT_DIRECTION,
             FIN.RATING_COMPONENT,
             FIN.CASH_FLOW,
             FIN.CURRENCY,
             NVL(SUM(FIN.CALL_COUNT), 0),
             NVL(SUM(FIN.CHARGED_USAGE), 0),
             NVL(SUM(FIN.AMOUNT), 0),
             NVL(SUM(TAXES), 0)
        FROM S_INV_RECON_INTEC_DTL FIN
       GROUP BY FRANCHISE,
                GROUP_OPERATOR,
                TRAFFIC_PERIOD,
                PRODUCT_GROUP, ----BILLED_PRODUCT , 
                RECORD_TYPE,
                COUNTRY_NAME,
                ORIG_DEST_NAME,
                RATE,
                BILLING_METHOD,
                STATEMENT_DIRECTION,
                RATING_COMPONENT,
                CASH_FLOW,
                CURRENCY,
                DISCOUNT_TEMPLATE;
  
    v_logmsg := 'SETTING PROFILE PARAMETERS IN INTEC SUMMARY TABLE TO USE FOR EXCESS INTEC SIDE RECORDS';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE S_INV_RECON_INTEC_SUM INT
       SET (ACTIVE_FLAG,
            MASTER_FLAG,
            TIMEZONE_FLAG,
            SUB_DEL_FLAG,
            ORIG_DEST_FLAG,
            CTRY_OPER_FLAG,
            THRESH_TYPE_TQ,
            THRESH_VALUE_TQ,
            THRESH_PERC_TQ,
            THRESH_TYPE_RQ,
            THRESH_VALUE_RQ,
            THRESH_PERC_RQ,
            THRESH_OVRIDE,
            PROD_FLAG) =
           (SELECT DISTINCT ACTIVE_FLAG,
                            MASTER_FLAG,
                            TIMEZONE_FLAG,
                            SUB_DEL_FLAG,
                            ORIG_DEST_FLAG,
                            CTRY_OPER_FLAG,
                            THRESH_TYPE_TQ,
                            THRESH_VALUE_TQ,
                            THRESH_PERC_TQ,
                            THRESH_TYPE_RQ,
                            THRESH_VALUE_RQ,
                            THRESH_PERC_RQ,
                            THRESH_OVRIDE,
                            PROD_FLAG
              FROM T_SET_OPR_RECON_PROFILE PRF
             WHERE PRF.MST_OPR_CODE = INT.GROUP_OPERATOR
               AND PRF.PROD_GROUP = INT.PRODUCT_GROUP
               AND PRF.CASH_FLOW = INT.CASH_FLOW
               AND PRF.BILLING_METHOD = INT.BILLING_METHOD
               AND PRF.CURRENCY = INT.CURRENCY)
     WHERE EXISTS (SELECT DISTINCT 1
              FROM T_SET_OPR_RECON_PROFILE PRF
             WHERE PRF.MST_OPR_CODE = INT.GROUP_OPERATOR
               AND PRF.PROD_GROUP = INT.PRODUCT_GROUP
               AND PRF.CASH_FLOW = INT.CASH_FLOW
               AND PRF.BILLING_METHOD = INT.BILLING_METHOD
               AND PRF.CURRENCY = INT.CURRENCY);
  
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
    
      UPDATE S_INV_RECON_LDR_SUM
         SET UNITS = CASE
                       WHEN PRODUCT_GROUP = 'TELE' THEN
                        nvl(CHARGED_USAGE, 0)
                       ELSE
                        nvl(CALL_COUNT, 0)
                     END;
    
      UPDATE S_INV_RECON_INTEC_SUM
         SET UNITS = CASE
                       WHEN PRODUCT_GROUP = 'TELE' THEN
                        nvl(CHARGED_USAGE, 0)
                       ELSE
                        nvl(CALL_COUNT, 0)
                     END;
    
      v_logmsg := 'SETTING RECORD TYPE IN INVOICE_SUMM USING INTEC_SUM FOR MATCHING VBR RECORDS';
      auditloggerProcedure(v_procName, v_logmsg);
    
      BEGIN
        MERGE INTO S_INV_RECON_LDR_SUM INV
        USING (select DISTINCT FRANCHISE,
                               GROUP_OPERATOR,
                               TRAFFIC_PERIOD,
                               PRODUCT_GROUP, ---- BILLED_PRODUCT , ***if add , error
                               COUNTRY_NAME,
                               ORIG_DEST_NAME,
                               CASH_FLOW,
                               BILLING_METHOD,
                               STATEMENT_DIRECTION,
                               RATING_COMPONENT,
                               CURRENCY
                 from S_INV_RECON_INTEC_SUM
                where RECORD_TYPE like 'V%') FIN
        ON (INV.FRANCHISE = FIN.FRANCHISE AND INV.MST_OPR_CODE = FIN.GROUP_OPERATOR AND INV.TRAFFIC_PERIOD = FIN.TRAFFIC_PERIOD AND INV.PRODUCT_GROUP = FIN.PRODUCT_GROUP AND
        ----INV.PRODUCT                 =  FIN.BILLED_PRODUCT      AND  
        INV.ORIG_DEST_NAME = FIN.ORIG_DEST_NAME AND INV.CASH_FLOW = FIN.CASH_FLOW AND INV.CURRENCY = FIN.CURRENCY AND INV.BILLING_METHOD = FIN.BILLING_METHOD AND INV.STATEMENT_DIRECTION = FIN.STATEMENT_DIRECTION AND INV.COMPONENT = FIN.RATING_COMPONENT)
        WHEN MATCHED THEN
          UPDATE
             SET INV.COUNTRY_NAME = FIN.COUNTRY_NAME, INV.RECORD_TYPE = 'V';
      
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          NULL;
      END;
    
      COMMIT;
    
  END;

  PROCEDURE RECON_DETAILS_DISPUTE(ERROR_CODE OUT NUMBER) AS
    v_unmap_disp_amt  number(10, 2) := 0;
    v_unmap_thrsh_prc number(5) := 0;
    v_ldr_seq_no      number(5) := 0;
    vn_tzone_opr      NUMBER := '';
    v_int_seq_no      number(5) := 0;
    v_audit_id        NUMBER := 0;
    v_new_seqno       number(5) := 0;
    v_unlock_cnt      number(5) := 0;
    v_rec_stdt        TIMESTAMP;
    v_inv_unmaped_amt number(15, 2) := 0;
    v_inv_prc_amt     number(15, 2) := 0;
    v_unmap_thrsh_amt number(10, 2) := 0;
    vs_user_comment   VARCHAR2(200);
    v_logmsg          VARCHAR2(2000);
    v_procName        VARCHAR2(50);
  BEGIN
    error_code := 200;
    v_procName := 'RECON_OPR_INTEC_DATA';
    v_logmsg   := 'START OF PROCEDURE RECON_DETAILS_DISPUTE';
    auditloggerProcedure(v_procName, v_logmsg);
    DELETE FROM T_SET_INVOICE_RECON;
  
    v_logmsg := 'LOADING NON-VBR RECORDS  WHICH ARE MATCHING ON RATE';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_INVOICE_RECON
      (FRANCHISE,
       MST_OPR_CODE,
       BILLING_PERIOD,
       TRAFFIC_PERIOD,
       INVOICE_NUMBER,
       BILLING_METHOD,
       STATEMENT_DIRECTION,
       PRODUCT_GROUP,
       COMPONENT,
       PRODUCT,
       INV_PRODUCT,
       CASH_FLOW,
       CURRENCY,
       RATE,
       CALL_COUNT,
       CHARGED_USAGE,
       UNITS,
       NET_AMOUNT,
       NET_AMOUNT_ORIG,
       TAX_AMOUNT,
       COUNTRY,
       ORIGINORDEST,
       DESCRIPTION,
       BATCH_ID,
       ACTIVE_FLAG,
       MASTER_FLAG,
       TIMEZONE_FLAG,
       PROD_FLAG,
       SUB_DEL_FLAG,
       ORIG_DEST_FLAG,
       CTRY_OPER_FLAG,
       THRESH_TYPE_TQ,
       THRESH_VALUE_TQ,
       THRESH_PERC_TQ,
       THRESH_TYPE_RQ,
       THRESH_VALUE_RQ,
       THRESH_PERC_RQ,
       THRESH_OVRIDE,
       COUNTRY_NAME,
       ORIG_DEST_NAME,
       INTEC_COUNTRY,
       INTEC_DEST,
       INTEC_RATE,
       INTEC_COUNT,
       INTEC_AMOUNT,
       INTEC_USAGE,
       INTEC_UNITS,
       INV_SEQ_NO,
       RECORD_TYPE,
       DISCOUNT_TEMPLATE)
      SELECT DISTINCT INV.FRANCHISE,
                      INV.MST_OPR_CODE,
                      INV.BILLING_PERIOD,
                      INV.TRAFFIC_PERIOD,
                      INV.INVOICE_NUMBER,
                      INV.BILLING_METHOD,
                      INV.STATEMENT_DIRECTION,
                      INV.PRODUCT_GROUP,
                      INV.COMPONENT,
                      INV.PRODUCT,
                      INV.INV_PRODUCT,
                      INV.CASH_FLOW,
                      INV.CURRENCY,
                      INV.RATE,
                      INV.CALL_COUNT,
                      INV.CHARGED_USAGE,
                      INV.UNITS,
                      INV.NET_AMOUNT,
                      INV.NET_AMOUNT_ORIG,
                      NVL(INV.TAXES, 0) TAX_AMOUNT,
                      INV.COUNTRY,
                      INV.ORIGINORDEST,
                      INV.DESCRIPTION,
                      INV.BATCH_ID,
                      INV.ACTIVE_FLAG,
                      INV.MASTER_FLAG,
                      INV.TIMEZONE_FLAG,
                      INV.PROD_FLAG,
                      INV.SUB_DEL_FLAG,
                      INV.ORIG_DEST_FLAG,
                      INV.CTRY_OPER_FLAG,
                      INV.THRESH_TYPE_TQ,
                      INV.THRESH_VALUE_TQ,
                      INV.THRESH_PERC_TQ,
                      INV.THRESH_TYPE_RQ,
                      INV.THRESH_VALUE_RQ,
                      INV.THRESH_PERC_RQ,
                      INV.THRESH_OVRIDE,
                      INV.COUNTRY_NAME,
                      INV.ORIG_DEST_NAME,
                      INT.COUNTRY_NAME INTEC_COUNTRY,
                      INT.ORIG_DEST_NAME INTEC_DEST,
                      INT.RATE INTEC_RATE,
                      INT.CALL_COUNT INTEC_COUNT,
                      INT.AMOUNT INTEC_AMOUNT,
                      INT.CHARGED_USAGE INTEC_USAGE,
                      INT.UNITS INTEC_UNITS,
                      -2 AS INV_SEQ_NO,
                      INT.RECORD_TYPE,
                      INT.DISCOUNT_TEMPLATE
        FROM S_INV_RECON_LDR_SUM inv, S_INV_RECON_INTEC_SUM INT
       where INV.COUNTRY_NAME = INT.COUNTRY_NAME
         AND nvl(INV.ORIG_DEST_NAME, 'AAA') =
             nvl(INT.ORIG_DEST_NAME, 'AAA')
         AND INV.FRANCHISE = INT.FRANCHISE
         AND INV.MST_OPR_CODE = INT.GROUP_OPERATOR
         AND INV.TRAFFIC_PERIOD = INT.TRAFFIC_PERIOD
         AND INV.BILLING_METHOD = INT.BILLING_METHOD
         AND INV.STATEMENT_DIRECTION = INT.STATEMENT_DIRECTION
         AND INV.PRODUCT_GROUP = INT.PRODUCT_GROUP
         AND INV.COMPONENT = INT.RATING_COMPONENT
         AND INV.PRODUCT = INT.BILLED_PRODUCT
         AND INV.CASH_FLOW = INT.CASH_FLOW
         AND INV.CURRENCY = INT.CURRENCY
         AND inv.rate = int.rate
         AND INV.RECORD_TYPE != 'V'
         and INT.RECORD_TYPE NOT LIKE 'V%'; ---- exclude VBR Set
  
    --- Chg-17 : Steps For Closest Matching -------------------------------------------
    v_logmsg := 'SETTING DUMMY BATCH-ID(-2) IN LDR AND INTEC SUMMARY TBLS FOR RATE MATCHING CASES';
    auditloggerProcedure(v_procName, v_logmsg);
    UPDATE S_INV_RECON_LDR_SUM INV
       SET BATCH_ID = -2
     WHERE EXISTS (SELECT *
              FROM T_SET_INVOICE_RECON INT
             where INV.COUNTRY_NAME = INT.COUNTRY_NAME
               AND nvl(INV.ORIG_DEST_NAME, 'AAA') =
                   nvl(INT.ORIG_DEST_NAME, 'AAA')
               AND INV.MST_OPR_CODE = INT.MST_OPR_CODE
               AND INV.TRAFFIC_PERIOD = INT.TRAFFIC_PERIOD
               AND INV.BILLING_METHOD = INT.BILLING_METHOD
               AND INV.STATEMENT_DIRECTION = INT.STATEMENT_DIRECTION
               AND INV.PRODUCT_GROUP = INT.PRODUCT_GROUP
               AND INV.COMPONENT = INT.COMPONENT
               AND INV.PRODUCT = INT.PRODUCT
               AND INV.CASH_FLOW = INT.CASH_FLOW
               AND INV.CURRENCY = INT.CURRENCY
               AND INV.RATE = INT.RATE);
  
    UPDATE S_INV_RECON_INTEC_SUM INT
       SET BATCH_ID = -2
     WHERE EXISTS (SELECT *
              FROM T_SET_INVOICE_RECON REC
             WHERE INT.COUNTRY_NAME = REC.INTEC_COUNTRY
               AND TRIM(nvl(INT.ORIG_DEST_NAME, 'AAA')) =
                   TRIM(nvl(REC.ORIG_DEST_NAME, 'AAA'))
               AND INT.GROUP_OPERATOR = REC.MST_OPR_CODE
               AND INT.TRAFFIC_PERIOD = REC.TRAFFIC_PERIOD
               AND INT.BILLING_METHOD = REC.BILLING_METHOD
               AND INT.STATEMENT_DIRECTION = REC.STATEMENT_DIRECTION
               AND INT.PRODUCT_GROUP = REC.PRODUCT_GROUP
               AND INT.RATING_COMPONENT = REC.COMPONENT
               AND INT.BILLED_PRODUCT = REC.PRODUCT
               AND INT.CASH_FLOW = REC.CASH_FLOW
               AND INT.CURRENCY = REC.CURRENCY
               AND INT.RATE = REC.INTEC_RATE);
    --- AND  INT.RECORD_TYPE not like 'V%'  ; release if required 
  
    v_logmsg := 'SETTING SEQ NUMBER IN BATCH-ID IN INV AND INTEC SUMMARY TBLS FOR CLOSEST MATCHING';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE S_INV_RECON_LDR_SUM
       SET BATCH_ID = ROWNUM, INV_SEQ_NO = 999
     WHERE nvl(BATCH_ID, 0) != -2
       AND RECORD_TYPE not like 'V%'; ---- except VBR records; 
  
    UPDATE S_INV_RECON_INTEC_SUM
       SET BATCH_ID = ROWNUM, INT_SEQ_NO = 999
     WHERE nvl(BATCH_ID, 0) != -2
       AND RECORD_TYPE not like 'V%';
  
    ---- setting sequence number in both LDR and INT SUM tables 
  
    /*-----
    Order   Approach
    1 make set of records by criss cross matching
    2 record diff between both rates for all records 
    3 sort on the rate diff 
    4 check if either side is locked
    5 if both side unlocked, set NEW seqn and lock
    6 If left-side is unlocked but right is locked
    7 check right-side any unlocked records
    8 if no, set seqn for left-side as single 
    9 if right-side is unlocked but left is locked
    10  check left-side any unlocked
    11  if no, set new seqn in right-side 
    -----*/
  
    v_logmsg := 'SETTING INV/INT SEQ Nos IN INV/INTEC SUMMARY TBLS BY CLOSEST MATCHING RATES FOR NOT MATCHING RECORDS';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      FOR CTY IN (SELECT INV.FRANCHISE,
                         INV.MST_OPR_CODE,
                         INT.TRAFFIC_PERIOD,
                         INT.PRODUCT_GROUP,
                         INT.CASH_FLOW,
                         INT.BILLING_METHOD,
                         INT.STATEMENT_DIRECTION,
                         INT.CURRENCY,
                         INT.COUNTRY_NAME,
                         INT.ORIG_DEST_NAME DESTN,
                         INV.RATE LDR_RATE,
                         INT.RATE INT_RATE,
                         INV.batch_id LDR_BATCH,
                         INT.batch_id INT_BATCH,
                         ABS(INV.RATE - INT.RATE) AS RATE_DIFF
                    FROM S_INV_RECON_LDR_SUM INV, S_INV_RECON_INTEC_SUM INT
                   WHERE INV.MST_OPR_CODE = INT.GROUP_OPERATOR
                     AND INV.TRAFFIC_PERIOD = INT.TRAFFIC_PERIOD
                     AND INV.BILLING_METHOD = INT.BILLING_METHOD
                     AND INV.STATEMENT_DIRECTION = INT.STATEMENT_DIRECTION
                     AND INV.PRODUCT_GROUP = INT.PRODUCT_GROUP
                     AND INV.CASH_FLOW = INT.CASH_FLOW
                     AND INV.CURRENCY = INT.CURRENCY
                     AND INV.COUNTRY_NAME = INT.COUNTRY_NAME
                     AND INV.ORIG_DEST_NAME = INT.ORIG_DEST_NAME
                     AND INV.BATCH_ID != -2
                     AND INT.BATCH_ID != -2
                  --- AND    INV.INV_SEQ_NO = NULL AND    INT.INT_SEQ_NO = NULL    
                   ORDER BY MST_OPR_CODE,
                            TRAFFIC_PERIOD,
                            PRODUCT_GROUP,
                            CASH_FLOW,
                            BILLING_METHOD,
                            STATEMENT_DIRECTION,
                            INV.ORIG_DEST_NAME,
                            RATE_DIFF) LOOP
      
        SELECT MAX(INV_SEQ_NO)
          INTO v_ldr_seq_no
          FROM S_INV_RECON_LDR_SUM
         WHERE BATCH_ID = CTY.LDR_BATCH;
      
        SELECT MAX(INT_SEQ_NO)
          INTO v_int_seq_no
          FROM S_INV_RECON_INTEC_SUM
         WHERE BATCH_ID = CTY.INT_BATCH;
      
        IF (v_ldr_seq_no = 999 and v_int_seq_no = 999) -- both side rates not yet linked  
         THEN
          v_new_seqno := v_new_seqno + 1;
        
          BEGIN
            UPDATE S_INV_RECON_LDR_SUM
               SET INV_SEQ_NO = v_new_seqno
             WHERE BATCH_ID = CTY.LDR_BATCH
               AND INV_SEQ_NO = 999
               AND MST_OPR_CODE = CTY.MST_OPR_CODE
               AND ORIG_DEST_NAME = CTY.DESTN;
          
          END;
        
          BEGIN
            UPDATE S_INV_RECON_INTEC_SUM
               SET INT_SEQ_NO = v_new_seqno
             WHERE BATCH_ID = CTY.INT_BATCH
               AND INT_SEQ_NO = 999
               AND GROUP_OPERATOR = CTY.MST_OPR_CODE
               AND ORIG_DEST_NAME = CTY.DESTN;
          
          END;
          v_logmsg := 'UPDATED SEQUENCE : ' || CTY.LDR_BATCH || ' = ' ||
                      CTY.INT_BATCH || ' --> ' || v_new_seqno;
          auditloggerProcedure(v_procName, v_logmsg);
          CONTINUE;
        
        ELSIF (v_ldr_seq_no = 999 and v_int_seq_no != 999) -- INTEC side rate already linked
         THEN
        
          BEGIN
            SELECT COUNT(1)
              into v_unlock_cnt
              FROM S_INV_RECON_INTEC_SUM INT
             WHERE GROUP_OPERATOR = CTY.MST_OPR_CODE
               AND TRAFFIC_PERIOD = CTY.TRAFFIC_PERIOD
               AND BILLING_METHOD = CTY.BILLING_METHOD
               AND STATEMENT_DIRECTION = CTY.STATEMENT_DIRECTION
               AND PRODUCT_GROUP = CTY.PRODUCT_GROUP
               AND CASH_FLOW = CTY.CASH_FLOW
               AND CURRENCY = CTY.CURRENCY
               AND COUNTRY_NAME = CTY.COUNTRY_NAME
               AND ORIG_DEST_NAME = CTY.DESTN
               AND BATCH_ID != -2
               AND INT_SEQ_NO = 999;
          
          END;
        
          IF (v_unlock_cnt = 0) -- Single no-matching LDR Record 
           THEN
          
            v_new_seqno := v_new_seqno + 1;
          
            BEGIN
              UPDATE S_INV_RECON_LDR_SUM
                 SET INV_SEQ_NO = v_new_seqno
               WHERE BATCH_ID = CTY.LDR_BATCH
                 AND INV_SEQ_NO = 999
                 AND MST_OPR_CODE = CTY.MST_OPR_CODE
                 AND ORIG_DEST_NAME = CTY.DESTN;
            
            END;
          
            DBMS_OUTPUT.PUT_LINE('..... UPDATED SEQUENCE - LDR - SINGLE : ' ||
                                 CTY.LDR_BATCH || ' = ' || CTY.INT_BATCH ||
                                 ' --> ' || v_new_seqno);
          
          ELSE
            CONTINUE; --- take next record from loop
          
          END IF;
        
        ELSIF (v_ldr_seq_no != 999 and v_int_seq_no = 999) -- LDR  side rate already linked
         THEN
        
          BEGIN
            SELECT COUNT(1)
              into v_unlock_cnt
              FROM S_INV_RECON_LDR_SUM INV
             WHERE MST_OPR_CODE = CTY.MST_OPR_CODE
               AND TRAFFIC_PERIOD = CTY.TRAFFIC_PERIOD
               AND BILLING_METHOD = CTY.BILLING_METHOD
               AND STATEMENT_DIRECTION = CTY.STATEMENT_DIRECTION
               AND PRODUCT_GROUP = CTY.PRODUCT_GROUP
               AND CASH_FLOW = CTY.CASH_FLOW
               AND CURRENCY = CTY.CURRENCY
               AND COUNTRY_NAME = CTY.COUNTRY_NAME
               AND ORIG_DEST_NAME = CTY.DESTN
               AND BATCH_ID != -2
               AND INV_SEQ_NO = 999;
          
          END;
        
          IF (v_unlock_cnt = 0) -- Single no-matching INTEC Record 
           THEN
          
            v_new_seqno := v_new_seqno + 1;
          
            BEGIN
              UPDATE S_INV_RECON_INTEC_SUM
                 SET INT_SEQ_NO = v_new_seqno
               WHERE BATCH_ID = CTY.INT_BATCH
                 AND INT_SEQ_NO = 999
                 AND GROUP_OPERATOR = CTY.MST_OPR_CODE
                 AND ORIG_DEST_NAME = CTY.DESTN;
            
            END;
          
            DBMS_OUTPUT.PUT_LINE('..... UPDATED SEQUENCE - INTEC - SINGLE : ' ||
                                 CTY.LDR_BATCH || ' = ' || CTY.INT_BATCH ||
                                 ' --> ' || v_new_seqno);
          
          ELSE
            CONTINUE; --- take next record from loop
          
          END IF;
        
        END IF;
      
      END LOOP; --- CTY LOOP 
    
    END;
  
    v_logmsg := 'SETTING SEQ NUMBER FOR *** VBR *** RECORDS WITHIN OPERATOR';
    auditloggerProcedure(v_procName, v_logmsg);
  
    DECLARE
      v_grpopr VARCHAR2(50) := '';
      v_destn2 VARCHAR2(50) := '';
      v_seqn2  NUMBER(5) := 0;
    
      CURSOR VBR IS
        SELECT GROUP_OPERATOR,
               TRAFFIC_PERIOD,
               PRODUCT_GROUP,
               CASH_FLOW,
               BILLING_METHOD,
               STATEMENT_DIRECTION,
               RECORD_TYPE,
               COUNTRY_NAME,
               ORIG_DEST_NAME,
               RATE,
               ROWNUM AS REC_NO,
               CTRY_OPER_FLAG,
               AMOUNT,
               CURRENCY
          FROM S_INV_RECON_INTEC_SUM INT
         WHERE INT.RECORD_TYPE like 'V%'
         ORDER BY GROUP_OPERATOR,
                  TRAFFIC_PERIOD,
                  PRODUCT_GROUP,
                  CASH_FLOW,
                  BILLING_METHOD,
                  STATEMENT_DIRECTION,
                  CURRENCY,
                  COUNTRY_NAME,
                  ORIG_DEST_NAME,
                  RECORD_TYPE,
                  RATE,
                  AMOUNT DESC
           FOR UPDATE;
    
      REC VBR%ROWTYPE;
    
    BEGIN
      OPEN VBR;
      LOOP
      
        FETCH VBR
          INTO REC;
        EXIT WHEN VBR%NOTFOUND;
      
        ---DBMS_OUTPUT.PUT_LINE('V_SEQ / ROWNUM / CNTRY / DESTN : ' || to_char(v_seqn1) || ' / ' || to_char(REC.REC_NO) || ' / ' || REC.COUNTRY_NAME || ' / ' || REC.ORIG_DEST_NAME );
      
        IF REC.GROUP_OPERATOR = v_grpopr THEN
          v_seqn2  := v_seqn2 + 1;
          v_grpopr := REC.GROUP_OPERATOR;
        
        ELSE
          v_seqn2  := 1;
          v_grpopr := REC.GROUP_OPERATOR;
        END IF;
      
        UPDATE S_INV_RECON_INTEC_SUM
           SET INT_SEQ_NO = v_seqn2
         WHERE CURRENT OF VBR;
      
      END LOOP;
    
      CLOSE VBR;
    
    END;
  
    v_logmsg := 'LOADING RECON DATA FROM BOTH INV and  INTEC BY MATCHING COMMON and  CNTRY/DEST/RATE(SEQ_NO) USING  OUTER JOIN';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_INVOICE_RECON
      (FRANCHISE,
       MST_OPR_CODE,
       BILLING_PERIOD,
       TRAFFIC_PERIOD,
       INVOICE_NUMBER,
       BILLING_METHOD,
       STATEMENT_DIRECTION,
       PRODUCT_GROUP,
       COMPONENT,
       PRODUCT,
       INV_PRODUCT,
       CASH_FLOW,
       CURRENCY,
       RATE,
       CALL_COUNT,
       CHARGED_USAGE,
       UNITS,
       NET_AMOUNT,
       NET_AMOUNT_ORIG,
       TAX_AMOUNT,
       COUNTRY,
       ORIGINORDEST,
       DESCRIPTION,
       BATCH_ID,
       ACTIVE_FLAG,
       MASTER_FLAG,
       TIMEZONE_FLAG,
       PROD_FLAG,
       SUB_DEL_FLAG,
       ORIG_DEST_FLAG,
       CTRY_OPER_FLAG,
       THRESH_TYPE_TQ,
       THRESH_VALUE_TQ,
       THRESH_PERC_TQ,
       THRESH_TYPE_RQ,
       THRESH_VALUE_RQ,
       THRESH_PERC_RQ,
       THRESH_OVRIDE,
       COUNTRY_NAME,
       ORIG_DEST_NAME,
       INTEC_COUNTRY,
       INTEC_DEST,
       INTEC_RATE,
       INTEC_COUNT,
       INTEC_AMOUNT,
       INTEC_USAGE,
       INTEC_UNITS,
       INV_SEQ_NO,
       RECORD_TYPE,
       DISCOUNT_TEMPLATE)
      SELECT *
        FROM (SELECT NVL(INV.FRANCHISE, INT.FRANCHISE) FRANCHISE,
                     NVL(INV.MST_OPR_CODE, INT.GROUP_OPERATOR) MST_OPR_CODE,
                     NVL(INV.BILLING_PERIOD, 0) BILLING_PERIOD,
                     NVL(INV.TRAFFIC_PERIOD, INT.TRAFFIC_PERIOD) TRAFFIC_PERIOD,
                     NVL(INV.INVOICE_NUMBER, ' ') INVOICE_NUMBER,
                     NVL(INV.BILLING_METHOD, INT.BILLING_METHOD) BILLING_METHOD,
                     NVL(INV.STATEMENT_DIRECTION, INT.STATEMENT_DIRECTION) STATEMENT_DIRECTION,
                     NVL(INV.PRODUCT_GROUP, INT.PRODUCT_GROUP) PRODUCT_GROUP,
                     NVL(INV.COMPONENT, INT.RATING_COMPONENT) COMPONENT,
                     NVL(INV.PRODUCT, INT.BILLED_PRODUCT) PRODUCT,
                     NVL(INV.INV_PRODUCT, INT.BILLED_PRODUCT) INV_PRODUCT,
                     NVL(INV.CASH_FLOW, INT.CASH_FLOW) CASH_FLOW,
                     NVL(INV.CURRENCY, INT.CURRENCY) CURRENCY,
                     NVL(INV.RATE, 0) RATE,
                     NVL(INV.CALL_COUNT, 0) CALL_COUNT,
                     NVL(INV.CHARGED_USAGE, 0) CHARGED_USAGE,
                     NVL(INV.UNITS, 0) UNITS,
                     NVL(INV.NET_AMOUNT, 0) NET_AMOUNT,
                     NVL(INV.NET_AMOUNT_ORIG, 0) NET_AMOUNT_ORIG,
                     NVL(INV.TAXES, 0) TAX_AMOUNT,
                     NVL(INV.COUNTRY, '') COUNTRY,
                     NVL(INV.ORIGINORDEST, '') ORIGINORDEST,
                     DESCRIPTION,
                     -----NVL(INV.DESCRIPTION, INT.ORIG_DEST_NAME)   DESCRIPTION ,
                     NVL(INV.INV_SEQ_NO, INT.INT_SEQ_NO) +
                     NVL(INT.INT_SEQ_NO, INV.INV_SEQ_NO) AS BATCH_ID, --- Chg-17 : for Sorting
                     NVL(INV.ACTIVE_FLAG, INT.ACTIVE_FLAG) ACTIVE_FLAG,
                     NVL(INV.MASTER_FLAG, INT.MASTER_FLAG) MASTER_FLAG,
                     NVL(INV.TIMEZONE_FLAG, INT.TIMEZONE_FLAG) TIMEZONE_FLAG,
                     NVL(INV.PROD_FLAG, INT.PROD_FLAG) PROD_FLAG,
                     NVL(INV.SUB_DEL_FLAG, INT.SUB_DEL_FLAG) SUB_DEL_FLAG,
                     NVL(INV.ORIG_DEST_FLAG, INT.ORIG_DEST_FLAG) ORIG_DEST_FLAG,
                     NVL(INV.CTRY_OPER_FLAG, INT.CTRY_OPER_FLAG) CTRY_OPER_FLAG,
                     NVL(INV.THRESH_TYPE_TQ, INT.THRESH_TYPE_TQ) THRESH_TYPE_TQ,
                     NVL(INV.THRESH_VALUE_TQ, INT.THRESH_VALUE_TQ) THRESH_VALUE_TQ,
                     NVL(INV.THRESH_PERC_TQ, INT.THRESH_PERC_TQ) THRESH_PERC_TQ,
                     NVL(INV.THRESH_TYPE_RQ, INT.THRESH_TYPE_RQ) THRESH_TYPE_RQ,
                     NVL(INV.THRESH_VALUE_RQ, INT.THRESH_VALUE_RQ) THRESH_VALUE_RQ,
                     NVL(INV.THRESH_PERC_RQ, INT.THRESH_PERC_RQ) THRESH_PERC_RQ,
                     NVL(INV.THRESH_OVRIDE, INT.THRESH_OVRIDE) THRESH_OVRIDE,
                     NVL(INV.COUNTRY_NAME, INT.COUNTRY_NAME) COUNTRY_NAME,
                     NVL(INV.ORIG_DEST_NAME, INT.ORIG_DEST_NAME) ORIG_DEST_NAME,
                     NVL(INT.COUNTRY_NAME, INV.COUNTRY_NAME) INTEC_COUNTRY,
                     NVL(INT.ORIG_DEST_NAME, INV.ORIG_DEST_NAME) INTEC_DEST,
                     NVL(INT.RATE, 0) INTEC_RATE,
                     NVL(INT.CALL_COUNT, 0) INTEC_COUNT,
                     NVL(INT.AMOUNT, 0) INTEC_AMOUNT,
                     NVL(INT.CHARGED_USAGE, 0) INTEC_USAGE,
                     NVL(INT.UNITS, 0) INTEC_UNITS,
                     NVL(INV_SEQ_NO, 999) + NVL(INT_SEQ_NO, 999) INV_SEQ_NO, -- For Sorting Purpose
                     CASE
                       WHEN INT.RECORD_TYPE IS NULL AND
                            (TRIM(INT.COUNTRY_NAME) = 'VBR' or
                            TRIM(INV.COUNTRY_NAME) = 'VBR') THEN
                        'VZ'
                       WHEN INT.RECORD_TYPE IS NOT NULL THEN
                        INT.RECORD_TYPE
                       ELSE
                        'D'
                     END RECORD_TYPE,
                     INT.DISCOUNT_TEMPLATE
                FROM S_INV_RECON_LDR_SUM INV
                FULL OUTER JOIN S_INV_RECON_INTEC_SUM INT
                  ON (INV.COUNTRY_NAME = INT.COUNTRY_NAME AND
                     ((INT.COUNTRY_NAME = 'VBR' and
                     INV.COUNTRY_NAME = 'VBR') OR
                     (nvl(INV.ORIG_DEST_NAME, 'AAA') =
                     nvl(INT.ORIG_DEST_NAME, 'AAA') and
                     INV.COUNTRY_NAME != 'VBR' AND
                     INT.COUNTRY_NAME != 'VBR')) AND
                     INV.INV_SEQ_NO = INT.INT_SEQ_NO AND
                     INV.MST_OPR_CODE = INT.GROUP_OPERATOR AND
                     INV.TRAFFIC_PERIOD = INT.TRAFFIC_PERIOD AND
                     INV.BILLING_METHOD = INT.BILLING_METHOD AND
                     INV.STATEMENT_DIRECTION = INT.STATEMENT_DIRECTION AND
                     INV.PRODUCT_GROUP = INT.PRODUCT_GROUP AND
                     INV.COMPONENT = INT.RATING_COMPONENT AND
                     INV.PRODUCT = INT.BILLED_PRODUCT AND
                     INV.CASH_FLOW = INT.CASH_FLOW AND
                     INV.CURRENCY = INT.CURRENCY)
               where NVL(INV.BATCH_ID, -1) != -2
                 AND NVL(INT.BATCH_ID, -1) != -2)
       WHERE NVL(INV_SEQ_NO, 0) >= 0;
  
    -----Place to differentiate the Dispute Types in DIFF_TYPE.
    v_logmsg := 'SETTING THE DIFFERENCE VALUES IN ALL RECORDS';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
      UPDATE T_SET_INVOICE_RECON
         SET DIFF_COUNT = CALL_COUNT - INTEC_COUNT,
             DIFF_USAGE = CHARGED_USAGE - INTEC_USAGE,
             DIFF_RATE  = RATE - INTEC_RATE,
             DIFF_UNITS = UNITS - INTEC_UNITS
       WHERE DIFF_TYPE is NULL;
    
    END;
  
    v_logmsg := 'UPDATE DIFFERENCE FOR COUNTRY OR DESTINATION MISSING CASES';
    auditloggerProcedure(v_procName, v_logmsg);
    ---IF respective Country/Destination is MISSING in either side
    ---o     Traffic Dispute     -           Inv Rate x Inv Count  ( = Inv Amount )
    ---o     Rate Dispute         -           NA
  
    UPDATE T_SET_INVOICE_RECON
       SET DIFF_TYPE      = 'T',
           DIFF_DESCR     = DECODE(substr(RECORD_TYPE, 1, 1),
                                   'V',
                                   'DEST/CTRY MISSING',
                                   'CNTRY/DEST MISSING'),
           DIFF_AMOUNT_TQ = NET_AMOUNT - INTEC_AMOUNT,
           DIFF_AMOUNT_RQ = 0
     WHERE (MST_OPR_CODE, TRAFFIC_PERIOD, PRODUCT_GROUP, CASH_FLOW,
            BILLING_METHOD, STATEMENT_DIRECTION, COMPONENT, CURRENCY,
            COUNTRY_NAME, NVL(ORIG_DEST_NAME, 'AAA'), INTEC_COUNTRY,
            NVL(INTEC_DEST, 'BBB')) IN
           (SELECT Distinct MST_OPR_CODE,
                            TRAFFIC_PERIOD,
                            PRODUCT_GROUP,
                            CASH_FLOW,
                            BILLING_METHOD,
                            STATEMENT_DIRECTION,
                            COMPONENT,
                            CURRENCY,
                            COUNTRY_NAME,
                            NVL(ORIG_DEST_NAME, 'AAA'),
                            INTEC_COUNTRY,
                            NVL(INTEC_DEST, 'BBB')
              FROM T_SET_INVOICE_RECON
             WHERE DIFF_TYPE is NULL
               AND COUNTRY_NAME != 'VBR'
             GROUP BY MST_OPR_CODE,
                      TRAFFIC_PERIOD,
                      PRODUCT_GROUP,
                      CASH_FLOW,
                      BILLING_METHOD,
                      STATEMENT_DIRECTION,
                      COMPONENT,
                      CURRENCY,
                      COUNTRY_NAME,
                      ORIG_DEST_NAME,
                      INTEC_COUNTRY,
                      INTEC_DEST
            HAVING SUM(INTEC_RATE) = 0 OR SUM(RATE) = 0)
       AND COUNTRY_NAME != 'VBR'
       AND DIFF_TYPE is NULL;
  
    v_logmsg := 'UPDATE DIFFERENCE FOR RATE and  AMOUNT MATCHING CASES';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE T_SET_INVOICE_RECON
       SET DIFF_TYPE      = 'M',
           DIFF_DESCR     = 'RATE/AMOUNT MATCHING',
           DIFF_AMOUNT_TQ = NET_AMOUNT - INTEC_AMOUNT,
           DIFF_AMOUNT_RQ = NET_AMOUNT - INTEC_AMOUNT
     WHERE DIFF_RATE = 0
       AND NET_AMOUNT - INTEC_AMOUNT = 0
       AND COUNTRY_NAME != 'VBR'
       AND DIFF_TYPE is NULL;
  
    v_logmsg := 'UPDATE DIFFERENCE FOR SINGLE RATE MIS-MATCH CASES';
    auditloggerProcedure(v_procName, v_logmsg);
  
    ---IF Destination has single rates in the Invoice and INTEC but does not match ( if the dest has single rate in both ) 
    ---o     Traffic Dispute     -  (Inv Units - Intec Units) x Intec Rate
    ---o     Rate Dispute        -  (Inv Rate - INTEC Rate) x Invoice Units
  
    UPDATE T_SET_INVOICE_RECON
       SET DIFF_TYPE      = 'R',
           DIFF_DESCR     = 'SNGLE RATE/TRFIC MISMATCH',
           DIFF_AMOUNT_TQ = DIFF_UNITS * INTEC_RATE,
           DIFF_AMOUNT_RQ = DIFF_RATE * UNITS
     WHERE (MST_OPR_CODE, TRAFFIC_PERIOD, PRODUCT_GROUP, CASH_FLOW,
            BILLING_METHOD, STATEMENT_DIRECTION, COMPONENT, CURRENCY,
            COUNTRY_NAME, ORIG_DEST_NAME) IN
           (SELECT MST_OPR_CODE,
                   TRAFFIC_PERIOD,
                   PRODUCT_GROUP,
                   CASH_FLOW,
                   BILLING_METHOD,
                   STATEMENT_DIRECTION,
                   COMPONENT,
                   CURRENCY,
                   COUNTRY_NAME,
                   ORIG_DEST_NAME
              FROM T_SET_INVOICE_RECON
             WHERE (ORIG_DEST_NAME = INTEC_DEST OR
                   COUNTRY_NAME = INTEC_COUNTRY)
               AND DIFF_TYPE is NULL
               AND COUNTRY_NAME != 'VBR'
             GROUP BY MST_OPR_CODE,
                      TRAFFIC_PERIOD,
                      PRODUCT_GROUP,
                      CASH_FLOW,
                      BILLING_METHOD,
                      STATEMENT_DIRECTION,
                      COMPONENT,
                      CURRENCY,
                      COUNTRY_NAME,
                      ORIG_DEST_NAME
            HAVING COUNT(1) = 1 AND SUM(RATE) != SUM(INTEC_RATE) AND MIN(RATE) > 0 AND MIN(INTEC_RATE) > 0)
       AND COUNTRY_NAME != 'VBR';
  
    v_logmsg := 'UPDATE DIFFERENCE FOR DESTINATION/COUNTRY MATCHING But RATE MISSING';
    auditloggerProcedure(v_procName, v_logmsg);
  
    ---IF CNTRY/DEST MATCHING But Rate is MISSING in the INTEC for selected Destination
    ---o     Traffic Dispute     -  NA
    ---o     Rate Dispute        -  Inv Rate x Inv Count ( = Inv Amount ) 
  
    UPDATE T_SET_INVOICE_RECON
       SET DIFF_TYPE      = 'R',
           DIFF_DESCR     = 'RATE MISSING',
           DIFF_AMOUNT_TQ = 0,
           DIFF_AMOUNT_RQ = NET_AMOUNT - INTEC_AMOUNT ------DIFF_RATE  * abs(UNITS - INTEC_UNITS)      
     WHERE ((COUNTRY_NAME = INTEC_COUNTRY AND CTRY_OPER_FLAG = 'C') OR
           (INTEC_DEST = ORIG_DEST_NAME AND CTRY_OPER_FLAG = 'D'))
          -----AND  ( RATE = 0  OR  INTEC_RATE = 0 ) 
       AND ((RATE = 0 AND NET_AMOUNT != 0) OR
           (INTEC_RATE = 0 AND INTEC_AMOUNT != 0))
       AND DIFF_TYPE is NULL
       AND COUNTRY_NAME != 'VBR';
  
    ---IF respective Rate is available in the INTEC for selected Destination
    ---o     Traffic Dispute   - (Inv Units - Intec Units) x Intec Rate
    ---o     Rate Dispute      - NA
  
    UPDATE T_SET_INVOICE_RECON
       SET DIFF_TYPE      = 'T',
           DIFF_DESCR     = 'RATE MATCH/TRFC MISMATCH',
           DIFF_AMOUNT_TQ = INTEC_RATE * DIFF_UNITS,
           DIFF_AMOUNT_RQ = 0
     WHERE ((COUNTRY_NAME = INTEC_COUNTRY AND CTRY_OPER_FLAG = 'C') OR
           (INTEC_DEST = ORIG_DEST_NAME AND CTRY_OPER_FLAG = 'D'))
       AND RATE = INTEC_RATE
       AND NET_AMOUNT != INTEC_AMOUNT
       AND DIFF_TYPE is NULL
       AND COUNTRY_NAME != 'VBR';
  
    --- CR2-Point8.b  
  
    UPDATE T_SET_INVOICE_RECON
       SET DIFF_TYPE      = 'T',
           DIFF_DESCR     = 'TRAFFIC QUERY/RATE MISSING',
           DIFF_AMOUNT_TQ = DIFF_UNITS *
                            DECODE(INTEC_RATE, 0, RATE, INTEC_RATE),
           DIFF_AMOUNT_RQ = 0
     WHERE ((COUNTRY_NAME = INTEC_COUNTRY AND CTRY_OPER_FLAG = 'C') OR
           (INTEC_DEST = ORIG_DEST_NAME AND CTRY_OPER_FLAG = 'D'))
       AND RATE != INTEC_RATE
       AND (RATE = 0 OR INTEC_RATE = 0)
       AND DIFF_UNITS != 0
       AND DIFF_TYPE is NULL
       AND COUNTRY_NAME != 'VBR';
  
    UPDATE T_SET_INVOICE_RECON
       SET DIFF_TYPE      = 'B',
           DIFF_DESCR     = 'RATE/TRAFFIC MISMATCH',
           DIFF_AMOUNT_TQ = DIFF_UNITS * INTEC_RATE,
           DIFF_AMOUNT_RQ = DIFF_RATE * UNITS
     WHERE ((COUNTRY_NAME = INTEC_COUNTRY AND CTRY_OPER_FLAG = 'C') OR
           (INTEC_DEST = ORIG_DEST_NAME AND CTRY_OPER_FLAG = 'D'))
       AND RATE != INTEC_RATE
       AND DIFF_UNITS != 0
       AND DIFF_TYPE is NULL
       AND COUNTRY_NAME != 'VBR';
  
    /*---- For Future .. if required ....
    UPDATE T_SET_INVOICE_RECON 
    SET     DIFF_TYPE = 'T' , DIFF_DESCR = 'TRAFFIC MISMATCH' ,
            DIFF_AMOUNT_TQ  = abs(NET_AMOUNT) - abs(INTEC_AMOUNT) , DIFF_AMOUNT_RQ = 0
    WHERE   DIFF_TYPE  is NULL 
    AND     COUNTRY_NAME = 'VBR'
    AND     NET_AMOUNT < 0 AND  INTEC_AMOUNT < 0 AND NET_AMOUNT != INTEC_AMOUNT  ;
    -------*/
  
    ---PMT59293 - HASAN - 12-DEC-2023 --- Orig Inv Amt to be presented in the reports and used for diff check for VBR records in OPR side 
  
    UPDATE T_SET_INVOICE_RECON
       SET DIFF_TYPE      = 'T',
           DIFF_DESCR     = 'TRAFFIC MISMATCH',
           NET_AMOUNT     = NET_AMOUNT_ORIG, -- orig inv amount from loader
           DIFF_AMOUNT_TQ = NET_AMOUNT_ORIG - INTEC_AMOUNT,
           DIFF_AMOUNT_RQ = 0
     WHERE DIFF_TYPE is NULL
       AND COUNTRY_NAME = 'VBR';
  
    -- HASAN-MAY13------
    v_logmsg := 'INVERTING (Symbol Change) DISPUTE VALUES FOR DECLARATION Documents';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE T_SET_INVOICE_RECON
       SET DIFF_AMOUNT_TQ = DIFF_AMOUNT_TQ * (-1),
           DIFF_AMOUNT_RQ = DIFF_AMOUNT_RQ * (-1),
           DIFF_RATE      = DIFF_RATE * (-1),
           DIFF_UNITS     = DIFF_UNITS * (-1),
           DIFF_COUNT     = DIFF_COUNT * (-1),
           DIFF_USAGE     = DIFF_USAGE * (-1)
     WHERE CASH_FLOW = 'R';
  
    v_logmsg := 'SETTING DISPUTE AMOUNT TOTAL IN RECON TABLE';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE T_SET_INVOICE_RECON
       SET DISP_AMT_TOTAL = DIFF_AMOUNT_TQ + DIFF_AMOUNT_RQ,
           DISP_AMT_PAID  = 0,
           DISP_AMT_NET   = DIFF_AMOUNT_TQ + DIFF_AMOUNT_RQ;
  
    DBMS_OUTPUT.PUT_LINE('..........APPLYING **** THRESHOLD ****  BASED ON PARAMETERS IN OPERATORS PROFILE ');
  
    DBMS_OUTPUT.PUT_LINE('..........SETTING FLAGS FOR RECORD LEVEL THRESHOLD FOR THE RATE DISPUTES');
  
    ---- Above step is replaced below as THRESH_PERC_RQ is not applicable as of now -----
  
    UPDATE T_SET_INVOICE_RECON
       SET DISP_TYPE_RQ = 'A'
     WHERE DIFF_AMOUNT_RQ > 0
       AND DISP_AMT_TOTAL != 0
       AND DIFF_AMOUNT_RQ > THRESH_VALUE_RQ
       AND RECORD_TYPE != 'V';
  
    ----COMMIT;
  
    ---- 04-05-2021 : Set Rpt Seq No For Invalid Invalid Intec Ctry Codes and  Erasing Opr Country and  Dest Names For Reporting Bottom Of The Main Report--- as per Hasan
  
    UPDATE T_SET_INVOICE_RECON
       SET RPT_SEQ_NO = 98, COUNTRY_NAME = ' ', ORIG_DEST_NAME = ' '
     WHERE INTEC_COUNTRY = '**INVALID_CTRY**';
  
    v_logmsg := 'ADDING REJECTED RECORDS AS TRFIC DISPUTES FOR THRESHOLD CHECK - DIFF_TYPE=T';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_INVOICE_RECON
      (FRANCHISE,
       MST_OPR_CODE,
       BILLING_PERIOD,
       TRAFFIC_PERIOD,
       INVOICE_NUMBER,
       BILLING_METHOD,
       STATEMENT_DIRECTION,
       PRODUCT_GROUP,
       COMPONENT,
       PRODUCT,
       INV_PRODUCT,
       CASH_FLOW,
       CURRENCY,
       RATE,
       CALL_COUNT,
       CHARGED_USAGE,
       NET_AMOUNT,
       NET_AMOUNT_ORIG,
       TAX_AMOUNT,
       DESCRIPTION,
       UNITS,
       BATCH_ID,
       ACTIVE_FLAG,
       MASTER_FLAG,
       TIMEZONE_FLAG,
       SUB_DEL_FLAG,
       ORIG_DEST_FLAG,
       CTRY_OPER_FLAG,
       PROD_FLAG,
       THRESH_TYPE_TQ,
       THRESH_VALUE_TQ,
       THRESH_PERC_TQ,
       THRESH_TYPE_RQ,
       THRESH_VALUE_RQ,
       THRESH_PERC_RQ,
       THRESH_OVRIDE,
       INTEC_COUNTRY,
       COUNTRY_NAME,
       ORIG_DEST_NAME,
       DIFF_TYPE,
       DIFF_DESCR,
       DISP_AMT_TOTAL,
       DISP_AMT_NET,
       RECORD_TYPE,
       INV_SEQ_NO,
       RPT_SEQ_NO,
       DIFF_UNITS,
       DIFF_AMOUNT_TQ,
       DIFF_RATE,
       DISP_TYPE_TQ)
      SELECT FRANCHISE,
             nvl(MST_OPR_CODE, OPERATOR) MST_OPR_CODE,
             BILLING_PERIOD,
             TRAFFIC_PERIOD,
             INVOICE_NUMBER,
             BILLING_METHOD,
             STATEMENT_DIRECTION,
             PRODUCT_GROUP,
             COMPONENT,
             PRODUCT,
             INV_PRODUCT,
             CASH_FLOW,
             CURRENCY,
             RATE,
             CALL_COUNT,
             CHARGED_USAGE,
             NET_AMOUNT,
             NET_AMOUNT_ORIG,
             TAXES,
             DESCRIPTION,
             decode(PRODUCT_GROUP, 'DATA', CALL_COUNT, CHARGED_USAGE) UNITS,
             BATCH_ID,
             ACTIVE_FLAG,
             MASTER_FLAG,
             TIMEZONE_FLAG,
             SUB_DEL_FLAG,
             ORIG_DEST_FLAG,
             CTRY_OPER_FLAG,
             PROD_FLAG,
             THRESH_TYPE_TQ,
             THRESH_VALUE_TQ,
             THRESH_PERC_TQ,
             THRESH_TYPE_RQ,
             THRESH_VALUE_RQ,
             THRESH_PERC_RQ,
             THRESH_OVRIDE,
             COUNTRY_NAME, ---> same ctry for rjt records.
             COUNTRY_NAME,
             ORIG_DEST_NAME,
             decode(DIFF_DESCR, 'NO DEST MAPPING', 'N', 'T') DIFF_TYPE, -- Chg-21
             DIFF_DESCR,
             NET_AMOUNT,
             NET_AMOUNT,
             'D' RECORD_TYPE,
             2 INV_SEQ_NO,
             0 RPT_SEQ_NO,
             decode(PRODUCT_GROUP, 'DATA', CALL_COUNT, CHARGED_USAGE) DIFF_UNITS,
             NET_AMOUNT DIFF_AMOUNT_TQ,
             RATE DIFF_RATE,
             decode(DIFF_DESCR, 'NO DEST MAPPING', NULL, 'D') DISP_TYPE_TQ
        FROM S_INV_RECON_LDR_DTL
       WHERE DIFF_TYPE = 'E';
  
    UPDATE T_SET_INVOICE_RECON
       SET RPT_SEQ_NO = 95
     WHERE DIFF_DESCR LIKE '%INVALID%'
        OR DIFF_DESCR LIKE '%NO DEST MAPPING%';
  
    v_logmsg := 'Chg-21= DECIDING DESTN UN-MAPPING CASES TO BE DISPUTED OR NOT BASED ON THRESHOLD';
    auditloggerProcedure(v_procName, v_logmsg);
    --Rule-1= In Case NO  Un-Mapped data , No Change in the Flow
    --Rule-2= The unmapped dispute amount should be compared against the total invoice amount. 
    --        If the disp amt is above 10 % ( configured in the Operators Profile ) of the Inv Amount, then only dispute the unmapped destinations 
    --Rule-3= If the disp-amount is above the set threshold ( configured in the Operators Profile ) for unmapped destinations, then also to be disputed. 
  
    BEGIN
    
      FOR HDR in (SELECT DISTINCT MST_OPR_CODE,
                                  TRAFFIC_PERIOD,
                                  PRODUCT_GROUP,
                                  CASH_FLOW,
                                  BILLING_METHOD,
                                  CURRENCY
                    FROM T_SET_INVOICE_RECON) LOOP
      
        v_unmap_disp_amt  := 0;
        v_unmap_thrsh_amt := 0;
        v_unmap_thrsh_prc := 0;
        v_inv_unmaped_amt := 0;
        v_inv_prc_amt     := 0;
      
        SELECT SUM(DIFF_AMOUNT_TQ)
          INTO v_unmap_disp_amt
          FROM T_SET_INVOICE_RECON
         WHERE MST_OPR_CODE = HDR.MST_OPR_CODE
           AND TRAFFIC_PERIOD = HDR.TRAFFIC_PERIOD
           AND PRODUCT_GROUP = HDR.PRODUCT_GROUP
           AND CASH_FLOW = HDR.CASH_FLOW
           AND CURRENCY = HDR.CURRENCY
           AND BILLING_METHOD = HDR.BILLING_METHOD
           AND DIFF_DESCR LIKE '%NO DEST MAPPING%';
      
        IF (v_unmap_disp_amt = 0) THEN
        
          CONTINUE; --- no action 
        
        END IF;
      
        SELECT max(UNMAP_DEST_THRSH_AMT), max(UNMAP_DEST_THRSH_PRC) -- max == to avoid multi-rows 
          INTO v_unmap_thrsh_amt, v_unmap_thrsh_prc
          FROM T_SET_OPR_RECON_PROFILE
         WHERE MST_OPR_CODE = HDR.MST_OPR_CODE
           AND PROD_GROUP = HDR.PRODUCT_GROUP
           AND CASH_FLOW = HDR.CASH_FLOW
           AND BILLING_METHOD = HDR.BILLING_METHOD
           AND CURRENCY = HDR.CURRENCY;
      
        SELECT SUM(NET_AMOUNT) * v_unmap_thrsh_prc / 100
          INTO v_inv_prc_amt
          FROM T_SET_INVOICE_RECON
         WHERE MST_OPR_CODE = HDR.MST_OPR_CODE
           AND TRAFFIC_PERIOD = HDR.TRAFFIC_PERIOD
           AND PRODUCT_GROUP = HDR.PRODUCT_GROUP
           AND CASH_FLOW = HDR.CASH_FLOW
           AND CURRENCY = HDR.CURRENCY
           AND BILLING_METHOD = HDR.BILLING_METHOD;
      
        -- In Case  Un-Mapped disp amount Is Above Un-Map Threshold Amt OR Above Invoice Threshold Amt , Dispute Un-Mapped Cases
      
        IF (v_unmap_disp_amt >= v_unmap_thrsh_amt OR
           v_unmap_disp_amt >= v_inv_prc_amt) THEN
        
          UPDATE T_SET_INVOICE_RECON
             SET DIFF_TYPE = 'T', DISP_TYPE_TQ = 'D'
           WHERE MST_OPR_CODE = HDR.MST_OPR_CODE
             AND PRODUCT_GROUP = HDR.PRODUCT_GROUP
             AND TRAFFIC_PERIOD = HDR.TRAFFIC_PERIOD
             AND CASH_FLOW = HDR.CASH_FLOW
             AND BILLING_METHOD = HDR.BILLING_METHOD
             AND CURRENCY = HDR.CURRENCY
             AND DIFF_DESCR LIKE '%NO DEST MAPPING%';
        
          v_logmsg := 'UNMAPPED RECORDS QUALIFIED FOR DISPUTE REPORT : ' ||
                      SQL%ROWCOUNT;
          auditloggerProcedure(v_procName, v_logmsg);
        
        END IF;
      
      END LOOP;
    
    END;
  
    v_logmsg := 'LOOPING DATA FOR FINDING  TRAFFIC DISPUTES';
    auditloggerProcedure(v_procName, v_logmsg);
  
    DECLARE
      x number;
      -----
      CURSOR TRFCUR IS
        SELECT MST_OPR_CODE,
               TRAFFIC_PERIOD,
               PRODUCT_GROUP,
               CASH_FLOW,
               BILLING_METHOD,
               CURRENCY,
               THRESH_TYPE_TQ,
               THRESH_VALUE_TQ,
               THRESH_PERC_TQ,
               THRESH_OVRIDE,
               SUM(NET_AMOUNT) RPT_INV_AMT,
               SUM(DIFF_AMOUNT_TQ) RPT_DIFF_AMT,
               MAX(THRESH_PERC_TQ) * SUM(NET_AMOUNT) / 100 RPT_PERC_AMT
          FROM T_SET_INVOICE_RECON
         WHERE DIFF_TYPE IS NOT NULL
         GROUP BY MST_OPR_CODE,
                  TRAFFIC_PERIOD,
                  PRODUCT_GROUP,
                  CASH_FLOW,
                  BILLING_METHOD,
                  CURRENCY,
                  THRESH_TYPE_TQ,
                  THRESH_VALUE_TQ,
                  THRESH_PERC_TQ,
                  THRESH_OVRIDE;
    
      TRFREC TRFCUR%ROWTYPE;
    
    BEGIN
    
      OPEN TRFCUR;
    
      LOOP
      
        FETCH TRFCUR
          INTO TRFREC;
        EXIT WHEN TRFCUR%NOTFOUND;
      
        ---- If Report Level Total Disp Amt Exceeds Traffic Threshold OR Report Threshold ---------
      
        IF TRFREC.RPT_DIFF_AMT > TRFREC.THRESH_VALUE_TQ OR
           TRFREC.RPT_DIFF_AMT > TRFREC.RPT_PERC_AMT THEN
        
          --- CR2-Point9-Change-- If Dest Total Exceeds Threshold, show all Dest Related Records. Otherwise, do not show any Dest Related Records. 
          v_logmsg := 'SETTING AS TRAFFIC DISPUTE FOR DESTINATION LEVEL DISPUTE AMOUNT ABOVE THRESHOLD';
          auditloggerProcedure(v_procName, v_logmsg);
        
          UPDATE T_SET_INVOICE_RECON
             SET DIFF_TYPE    = 'T',
                 DISP_TYPE_TQ = 'D',
                 DIFF_DESCR   = 'DEST_LVL DISPUTE'
           WHERE (MST_OPR_CODE, TRAFFIC_PERIOD, PRODUCT_GROUP, CASH_FLOW,
                  BILLING_METHOD, CURRENCY, THRESH_TYPE_TQ, COUNTRY_NAME,
                  ORIG_DEST_NAME) IN
                 (SELECT MST_OPR_CODE,
                         TRAFFIC_PERIOD,
                         PRODUCT_GROUP,
                         CASH_FLOW,
                         BILLING_METHOD,
                         CURRENCY,
                         THRESH_TYPE_TQ,
                         COUNTRY_NAME,
                         ORIG_DEST_NAME
                    FROM T_SET_INVOICE_RECON
                   WHERE DIFF_AMOUNT_TQ != 0 ------ BOTH SIDE DIFF 
                     AND MST_OPR_CODE = TRFREC.MST_OPR_CODE
                     AND TRAFFIC_PERIOD = TRFREC.TRAFFIC_PERIOD
                     AND PRODUCT_GROUP = TRFREC.PRODUCT_GROUP
                     AND CASH_FLOW = TRFREC.CASH_FLOW
                     AND CURRENCY = TRFREC.CURRENCY
                     AND BILLING_METHOD = TRFREC.BILLING_METHOD
                     AND THRESH_TYPE_TQ = TRFREC.THRESH_TYPE_TQ
                     AND TRFREC.THRESH_TYPE_TQ = 'D'
                     AND COUNTRY_NAME != 'VBR' ---- EXCLUDE VBR RECORDS 
                   GROUP BY MST_OPR_CODE,
                            TRAFFIC_PERIOD,
                            PRODUCT_GROUP,
                            CASH_FLOW,
                            BILLING_METHOD,
                            CURRENCY,
                            THRESH_TYPE_TQ,
                            COUNTRY_NAME,
                            ORIG_DEST_NAME
                  HAVING COUNT(1) > 1 AND (SUM(DIFF_AMOUNT_TQ) > MAX(THRESH_VALUE_TQ) OR SUM(DIFF_AMOUNT_TQ) > MAX(THRESH_PERC_TQ) * SUM(NET_AMOUNT) / 100) -- Chg-20. same Thresh % is in all rows for combo so MAX
                  -- SUM(DIFF_AMOUNT_TQ) > MAX( THRESH_PERC_TQ  * NET_AMOUNT/100 )    -- % chk is not working  so replaced by above 
                  )
             AND DIFF_AMOUNT_TQ != 0
             AND DISP_TYPE_TQ IS NULL;
        
          UPDATE T_SET_INVOICE_RECON
             SET DIFF_TYPE    = 'N',
                 DISP_TYPE_TQ = 'D',
                 DIFF_DESCR   = 'NO DISPUTE'
           WHERE (MST_OPR_CODE, TRAFFIC_PERIOD, PRODUCT_GROUP, CASH_FLOW,
                  BILLING_METHOD, CURRENCY, THRESH_TYPE_TQ, COUNTRY_NAME,
                  ORIG_DEST_NAME) IN
                 (SELECT MST_OPR_CODE,
                         TRAFFIC_PERIOD,
                         PRODUCT_GROUP,
                         CASH_FLOW,
                         BILLING_METHOD,
                         CURRENCY,
                         THRESH_TYPE_TQ,
                         COUNTRY_NAME,
                         ORIG_DEST_NAME
                    FROM T_SET_INVOICE_RECON
                   WHERE DIFF_AMOUNT_TQ != 0 ------ BOTH SIDE DIFF 
                     AND MST_OPR_CODE = TRFREC.MST_OPR_CODE
                     AND TRAFFIC_PERIOD = TRFREC.TRAFFIC_PERIOD
                     AND PRODUCT_GROUP = TRFREC.PRODUCT_GROUP
                     AND CASH_FLOW = TRFREC.CASH_FLOW
                     AND CURRENCY = TRFREC.CURRENCY
                     AND BILLING_METHOD = TRFREC.BILLING_METHOD
                     AND THRESH_TYPE_TQ = TRFREC.THRESH_TYPE_TQ
                     AND TRFREC.THRESH_TYPE_TQ = 'D'
                     AND DIFF_DESCR != 'DEST_LVL DISPUTE'
                     AND COUNTRY_NAME != 'VBR' ---- EXCLUDE VBR RECORDS 
                   GROUP BY MST_OPR_CODE,
                            TRAFFIC_PERIOD,
                            PRODUCT_GROUP,
                            CASH_FLOW,
                            BILLING_METHOD,
                            CURRENCY,
                            THRESH_TYPE_TQ,
                            COUNTRY_NAME,
                            ORIG_DEST_NAME
                  HAVING COUNT(1) > 1 AND (SUM(DIFF_AMOUNT_TQ) < MAX(THRESH_VALUE_TQ) OR SUM(DIFF_AMOUNT_TQ) < MAX(THRESH_PERC_TQ) * SUM(NET_AMOUNT) / 100) -- Chg-20. same Thresh % is in all rows for combo so MAX
                  -- OR SUM(DIFF_AMOUNT_TQ)  < MAX( THRESH_PERC_TQ * NET_AMOUNT/100 )   -- % chk is not working so replaced by below 
                  )
             AND DIFF_AMOUNT_TQ != 0
             AND DIFF_DESCR != 'DEST_LVL DISPUTE'
             AND DISP_TYPE_TQ IS NULL;
        
          --- Below will work for single rate destination records , rate matching but amount not matching 
          v_logmsg := 'SETTING DISPUTE TYPE FOR RATE LEVEL THRESHOLD For Single Rate Records';
          auditloggerProcedure(v_procName, v_logmsg);
        
          UPDATE T_SET_INVOICE_RECON
             SET DISP_TYPE_TQ = 'R'
           WHERE MST_OPR_CODE = TRFREC.MST_OPR_CODE
             AND TRAFFIC_PERIOD = TRFREC.TRAFFIC_PERIOD
             AND PRODUCT_GROUP = TRFREC.PRODUCT_GROUP
             AND CASH_FLOW = TRFREC.CASH_FLOW
             AND CURRENCY = TRFREC.CURRENCY
             AND BILLING_METHOD = TRFREC.BILLING_METHOD
             AND THRESH_TYPE_TQ = TRFREC.THRESH_TYPE_TQ
             AND DIFF_AMOUNT_TQ > 0
             AND DISP_TYPE_TQ IS NULL
             AND COUNTRY_NAME != 'VBR' ---- Exclude VBR Records 
             AND DIFF_TYPE != 'N' ---  Exclude NO dispute cases 
             AND TRFREC.THRESH_TYPE_TQ = 'D';
        
          UPDATE T_SET_INVOICE_RECON
             SET DISP_TYPE_TQ = 'C'
           WHERE (MST_OPR_CODE, TRAFFIC_PERIOD, PRODUCT_GROUP, CASH_FLOW,
                  BILLING_METHOD, CURRENCY, THRESH_TYPE_TQ, COUNTRY_NAME) IN
                 (SELECT DISTINCT MST_OPR_CODE,
                                  TRAFFIC_PERIOD,
                                  PRODUCT_GROUP,
                                  CASH_FLOW,
                                  BILLING_METHOD,
                                  CURRENCY,
                                  THRESH_TYPE_TQ,
                                  COUNTRY_NAME
                    FROM T_SET_INVOICE_RECON
                   WHERE MST_OPR_CODE = TRFREC.MST_OPR_CODE
                     AND TRAFFIC_PERIOD = TRFREC.TRAFFIC_PERIOD
                     AND PRODUCT_GROUP = TRFREC.PRODUCT_GROUP
                     AND CASH_FLOW = TRFREC.CASH_FLOW
                     AND CURRENCY = TRFREC.CURRENCY
                     AND BILLING_METHOD = TRFREC.BILLING_METHOD
                     AND THRESH_TYPE_TQ = TRFREC.THRESH_TYPE_TQ
                     AND (TRFREC.THRESH_TYPE_TQ = 'C' OR
                         COUNTRY_NAME = 'VBR')
                   GROUP BY MST_OPR_CODE,
                            TRAFFIC_PERIOD,
                            PRODUCT_GROUP,
                            CASH_FLOW,
                            BILLING_METHOD,
                            CURRENCY,
                            THRESH_TYPE_TQ,
                            COUNTRY_NAME
                  HAVING SUM(DIFF_AMOUNT_TQ) > 0);
        
        ELSIF TRFREC.THRESH_OVRIDE = 'N' ---> CR2-Point4 - May27
         THEN
        
          --- CR2-Point9-Change-- If Dest Total Exceeds Threshold, show all Dest Related Records. Otherwise, do not show any Dest Related Records. 
          v_logmsg := 'SETTING AS TRAFFIC DISPUTE FOR DESTINATION LEVEL DISPUTE AMOUNT ABOVE THRESHOLD';
          auditloggerProcedure(v_procName, v_logmsg);
        
          UPDATE T_SET_INVOICE_RECON
             SET DIFF_TYPE    = 'T',
                 DISP_TYPE_TQ = 'D',
                 DIFF_DESCR   = 'DEST_LVL DISPUTE'
           WHERE (MST_OPR_CODE, TRAFFIC_PERIOD, PRODUCT_GROUP, CASH_FLOW,
                  BILLING_METHOD, CURRENCY, THRESH_TYPE_TQ, COUNTRY_NAME,
                  ORIG_DEST_NAME) IN
                 (SELECT MST_OPR_CODE,
                         TRAFFIC_PERIOD,
                         PRODUCT_GROUP,
                         CASH_FLOW,
                         BILLING_METHOD,
                         CURRENCY,
                         THRESH_TYPE_TQ,
                         COUNTRY_NAME,
                         ORIG_DEST_NAME
                    FROM T_SET_INVOICE_RECON
                   WHERE DIFF_AMOUNT_TQ != 0 ------ BOTH SIDE DIFF 
                     AND MST_OPR_CODE = TRFREC.MST_OPR_CODE
                     AND TRAFFIC_PERIOD = TRFREC.TRAFFIC_PERIOD
                     AND PRODUCT_GROUP = TRFREC.PRODUCT_GROUP
                     AND CASH_FLOW = TRFREC.CASH_FLOW
                     AND CURRENCY = TRFREC.CURRENCY
                     AND BILLING_METHOD = TRFREC.BILLING_METHOD
                     AND THRESH_TYPE_TQ = TRFREC.THRESH_TYPE_TQ
                     AND TRFREC.THRESH_TYPE_TQ = 'D'
                     AND COUNTRY_NAME != 'VBR' ---- EXCLUDE VBR RECORDS 
                   GROUP BY MST_OPR_CODE,
                            TRAFFIC_PERIOD,
                            PRODUCT_GROUP,
                            CASH_FLOW,
                            BILLING_METHOD,
                            CURRENCY,
                            THRESH_TYPE_TQ,
                            COUNTRY_NAME,
                            ORIG_DEST_NAME
                  HAVING COUNT(1) > 1 AND (SUM(DIFF_AMOUNT_TQ) > MAX(THRESH_VALUE_TQ) OR SUM(DIFF_AMOUNT_TQ) > MAX(THRESH_PERC_TQ) * SUM(NET_AMOUNT) / 100) -- Chg-20. same Thresh % is in all rows for combo so MAX
                  -- OR SUM(DIFF_AMOUNT_TQ) > MAX( THRESH_PERC_TQ * NET_AMOUNT/100 )   -- not working 
                  )
             AND DIFF_AMOUNT_TQ != 0
             AND DISP_TYPE_TQ IS NULL;
        
          v_logmsg := 'SETTING AS NO-DISPUTE FOR DESTINATION LEVEL DISPUTE AMOUNT BELOW THRESHOLD';
          auditloggerProcedure(v_procName, v_logmsg);
        
          UPDATE T_SET_INVOICE_RECON
             SET DIFF_TYPE    = 'N',
                 DISP_TYPE_TQ = 'D',
                 DIFF_DESCR   = 'NO DISPUTE'
           WHERE (MST_OPR_CODE, TRAFFIC_PERIOD, PRODUCT_GROUP, CASH_FLOW,
                  BILLING_METHOD, CURRENCY, THRESH_TYPE_TQ, COUNTRY_NAME,
                  ORIG_DEST_NAME) IN
                 (SELECT MST_OPR_CODE,
                         TRAFFIC_PERIOD,
                         PRODUCT_GROUP,
                         CASH_FLOW,
                         BILLING_METHOD,
                         CURRENCY,
                         THRESH_TYPE_TQ,
                         COUNTRY_NAME,
                         ORIG_DEST_NAME
                    FROM T_SET_INVOICE_RECON
                   WHERE DIFF_AMOUNT_TQ != 0 ------ BOTH SIDE DIFF 
                     AND MST_OPR_CODE = TRFREC.MST_OPR_CODE
                     AND TRAFFIC_PERIOD = TRFREC.TRAFFIC_PERIOD
                     AND PRODUCT_GROUP = TRFREC.PRODUCT_GROUP
                     AND CASH_FLOW = TRFREC.CASH_FLOW
                     AND CURRENCY = TRFREC.CURRENCY
                     AND BILLING_METHOD = TRFREC.BILLING_METHOD
                     AND THRESH_TYPE_TQ = TRFREC.THRESH_TYPE_TQ
                     AND TRFREC.THRESH_TYPE_TQ = 'D'
                     AND DIFF_DESCR != 'DEST_LVL DISPUTE'
                     AND COUNTRY_NAME != 'VBR' ---- EXCLUDE VBR RECORDS 
                   GROUP BY MST_OPR_CODE,
                            TRAFFIC_PERIOD,
                            PRODUCT_GROUP,
                            CASH_FLOW,
                            BILLING_METHOD,
                            CURRENCY,
                            THRESH_TYPE_TQ,
                            COUNTRY_NAME,
                            ORIG_DEST_NAME
                  HAVING COUNT(1) > 1 AND (SUM(DIFF_AMOUNT_TQ) < MAX(THRESH_VALUE_TQ) OR SUM(DIFF_AMOUNT_TQ) < MAX(THRESH_PERC_TQ) * SUM(NET_AMOUNT) / 100) -- Chg-20. same Thresh% is in all rows for combo so MAX
                  -- SUM(DIFF_AMOUNT_TQ)  < MAX( THRESH_PERC_TQ * NET_AMOUNT/100 )   -- replaced by above as % chk is not working
                  )
             AND DIFF_AMOUNT_TQ != 0
             AND DIFF_DESCR != 'DEST_LVL DISPUTE'
             AND DISP_TYPE_TQ IS NULL;
        
          --- Below will work for single rate destination records , rate matching but amount not matching       
          v_logmsg := 'SETTING DISPUTE TYPE FOR RATE LEVEL THRESHOLD IF NOT QUALIFIED FOR ABOVE';
          auditloggerProcedure(v_procName, v_logmsg);
        
          UPDATE T_SET_INVOICE_RECON
             SET DISP_TYPE_TQ = 'D'
           WHERE (DIFF_AMOUNT_TQ > THRESH_VALUE_TQ OR
                 DIFF_AMOUNT_TQ > (THRESH_PERC_TQ * NET_AMOUNT / 100))
             AND MST_OPR_CODE = TRFREC.MST_OPR_CODE
             AND TRAFFIC_PERIOD = TRFREC.TRAFFIC_PERIOD
             AND PRODUCT_GROUP = TRFREC.PRODUCT_GROUP
             AND CASH_FLOW = TRFREC.CASH_FLOW
             AND CURRENCY = TRFREC.CURRENCY
             AND BILLING_METHOD = TRFREC.BILLING_METHOD
             AND THRESH_TYPE_TQ = TRFREC.THRESH_TYPE_TQ
             AND DIFF_AMOUNT_TQ > 0 ------ ONLY INV SIDE IS MORE
             AND TRFREC.THRESH_TYPE_TQ = 'D'
             AND COUNTRY_NAME != 'VBR' ---- EXCLUDE VBR RECORDS 
             AND DIFF_TYPE != 'N' ---- Except No Dispute Cases 
             AND DISP_TYPE_TQ IS NULL;
        
          v_logmsg := 'SETTING DISPUTE TYPE FOR COUNTRY LEVEL THRESHOLD INCLUDING VBR';
          auditloggerProcedure(v_procName, v_logmsg);
        
          UPDATE T_SET_INVOICE_RECON
             SET DISP_TYPE_TQ = 'C'
           WHERE (MST_OPR_CODE, TRAFFIC_PERIOD, PRODUCT_GROUP, CASH_FLOW,
                  BILLING_METHOD, CURRENCY, THRESH_TYPE_TQ, COUNTRY_NAME) IN
                 (SELECT MST_OPR_CODE,
                         TRAFFIC_PERIOD,
                         PRODUCT_GROUP,
                         CASH_FLOW,
                         BILLING_METHOD,
                         CURRENCY,
                         THRESH_TYPE_TQ,
                         COUNTRY_NAME
                    FROM T_SET_INVOICE_RECON
                   WHERE DIFF_AMOUNT_TQ != 0 ------ BOTH SIDE DIFF 
                     AND MST_OPR_CODE = TRFREC.MST_OPR_CODE
                     AND TRAFFIC_PERIOD = TRFREC.TRAFFIC_PERIOD
                     AND PRODUCT_GROUP = TRFREC.PRODUCT_GROUP
                     AND CASH_FLOW = TRFREC.CASH_FLOW
                     AND CURRENCY = TRFREC.CURRENCY
                     AND BILLING_METHOD = TRFREC.BILLING_METHOD
                     AND THRESH_TYPE_TQ = TRFREC.THRESH_TYPE_TQ
                     AND (TRFREC.THRESH_TYPE_TQ = 'C' OR
                         COUNTRY_NAME = 'VBR')
                   GROUP BY MST_OPR_CODE,
                            TRAFFIC_PERIOD,
                            PRODUCT_GROUP,
                            CASH_FLOW,
                            BILLING_METHOD,
                            CURRENCY,
                            THRESH_TYPE_TQ,
                            COUNTRY_NAME
                  HAVING(SUM(DIFF_AMOUNT_TQ) > MAX(THRESH_VALUE_TQ) OR SUM(DIFF_AMOUNT_TQ) > MAX(THRESH_PERC_TQ) * sum(NET_AMOUNT) / 100))
             AND DIFF_AMOUNT_TQ != 0
             AND DISP_TYPE_TQ IS NULL; --- added 
        END IF;
      
      END LOOP;
    
      CLOSE TRFCUR;
    
    END;
  
    -- CR2-Point-8 
  
    --- Assigning same value to new columns before re-calculating
  
    UPDATE T_SET_INVOICE_RECON
       SET INTEC_AMOUNT_2 = INTEC_AMOUNT, DIFF_AMOUNT_TQ_2 = DIFF_AMOUNT_TQ;
  
    UPDATE T_SET_INVOICE_RECON
       SET RPT_SEQ_NO = 90
     WHERE INTEC_COUNTRY = 'VBR';
    COMMIT;
  END;

  -- PROCEDURE_LAST

  PROCEDURE RECON_SUMMARY_HIST(ERROR_CODE OUT NUMBER) AS
    v_unmap_disp_amt  number(10, 2) := 0;
    v_unmap_thrsh_prc number(5) := 0;
    v_ldr_seq_no      number(5) := 0;
    vn_tzone_opr      NUMBER := '';
    v_int_seq_no      number(5) := 0;
    v_audit_id        NUMBER := 0;
    v_new_seqno       number(5) := 0;
    v_unlock_cnt      number(5) := 0;
    v_rec_stdt        TIMESTAMP;
    v_inv_unmaped_amt number(15, 2) := 0;
    v_inv_prc_amt     number(15, 2) := 0;
    v_unmap_thrsh_amt number(10, 2) := 0;
    vs_user_comment   VARCHAR2(200);
    vs_new_hdr_id     number(10) := 0;
    vs_prev_hdr_id    number(10) := 0;
    v_logmsg          VARCHAR2(2000);
    v_procName        VARCHAR2(50);
  
  BEGIN
    error_code := 200;
    v_procName := 'RECON_SUMMARY_HIST';
    v_logmsg   := 'START OF PROCEDURE RECON_SUMMARY_HIST';
    auditloggerProcedure(v_procName, v_logmsg);
  
    DELETE FROM S_SET_INV_RECON_HDR;
  
    INSERT INTO S_SET_INV_RECON_HDR
      (HDR_ID,
       RECON_VER,
       FRANCHISE,
       MST_OPR_CODE,
       PRODUCT_GROUP,
       TRAFFIC_PERIOD,
       BILLING_METHOD,
       STATEMENT_DIRECTION,
       CASH_FLOW,
       CURRENCY,
       INV_STATUS,
       CALL_COUNT,
       UNITS,
       NET_AMOUNT,
       TAX_AMOUNT,
       NET_AMOUNT_ORIG,
       INTEC_COUNT,
       INTEC_AMOUNT,
       INTEC_AMOUNT_2,
       INTEC_UNITS,
       DIFF_COUNT,
       DIFF_UNITS,
       DIFF_RATE_AVG,
       DIFF_AMOUNT_TQ,
       DIFF_AMOUNT_TQ_2,
       DIFF_AMOUNT_RQ,
       DISP_STATUS,
       DISP_STATUS_DT,
       DISP_REMARKS,
       RECON_STATUS,
       RECON_STATUS_DT,
       ACTIVE_FLAG,
       MASTER_FLAG,
       TIMEZONE_FLAG,
       PROD_FLAG,
       SUB_DEL_FLAG,
       ORIG_DEST_FLAG,
       CTRY_OPER_FLAG,
       THRESH_TYPE_TQ,
       THRESH_VALUE_TQ,
       THRESH_PERC_TQ,
       THRESH_TYPE_RQ,
       THRESH_VALUE_RQ,
       THRESH_PERC_RQ,
       THRESH_OVRIDE,
       CREATED_BY,
       CREATED_DATE,
       TRAFFIC_STDATE)
      SELECT SEQ_INV_RECON_HDR.NEXTVAL HDR_ID,
             1                         RECON_VER,
             FRANCHISE,
             MST_OPR_CODE,
             PRODUCT_GROUP,
             TRAFFIC_PERIOD,
             BILLING_METHOD,
             STATEMENT_DIRECTION,
             CASH_FLOW,
             CURRENCY,
             INV_STATUS,
             CALL_COUNT,
             UNITS,
             NET_AMOUNT,
             TAX_AMOUNT,
             NET_AMOUNT_ORIG,
             INTEC_COUNT,
             INTEC_AMOUNT,
             INTEC_AMOUNT_2,
             INTEC_UNITS,
             DIFF_COUNT,
             DIFF_UNITS,
             DIFF_RATE_AVG,
             DIFF_AMOUNT_TQ,
             DIFF_AMOUNT_TQ_2,
             DIFF_AMOUNT_RQ,
             DISP_STATUS,
             DISP_STATUS_DT,
             DISP_REMARKS,
             RECON_STATUS,
             RECON_STATUS_DT,
             ACTIVE_FLAG,
             MASTER_FLAG,
             TIMEZONE_FLAG,
             PROD_FLAG,
             SUB_DEL_FLAG,
             ORIG_DEST_FLAG,
             CTRY_OPER_FLAG,
             THRESH_TYPE_TQ,
             THRESH_VALUE_TQ,
             THRESH_PERC_TQ,
             THRESH_TYPE_RQ,
             THRESH_VALUE_RQ,
             THRESH_PERC_RQ,
             THRESH_OVRIDE,
             USER                      CREATED_BY,
             SYSDATE                   CREATED_DATE,
             TRAFFIC_STDATE
        FROM (SELECT REC.FRANCHISE,
                     REC.MST_OPR_CODE,
                     REC.PRODUCT_GROUP,
                     REC.TRAFFIC_PERIOD,
                     REC.BILLING_METHOD,
                     REC.STATEMENT_DIRECTION,
                     REC.CASH_FLOW,
                     REC.CURRENCY,
                     'CREATED' INV_STATUS,
                     SUM(REC.CALL_COUNT) CALL_COUNT,
                     SUM(REC.UNITS) UNITS,
                     SUM(REC.NET_AMOUNT) NET_AMOUNT,
                     SUM(REC.NET_AMOUNT_ORIG) NET_AMOUNT_ORIG,
                     SUM(REC.TAX_AMOUNT) TAX_AMOUNT,
                     SUM(REC.INTEC_COUNT) INTEC_COUNT,
                     SUM(REC.INTEC_AMOUNT) INTEC_AMOUNT,
                     SUM(REC.INTEC_AMOUNT_2) INTEC_AMOUNT_2,
                     SUM(REC.INTEC_UNITS) INTEC_UNITS,
                     SUM(REC.DIFF_COUNT) DIFF_COUNT,
                     SUM(REC.DIFF_UNITS) DIFF_UNITS,
                     ROUND(AVG(REC.DIFF_RATE), 4) DIFF_RATE_AVG,
                     SUM(REC.DIFF_AMOUNT_TQ) DIFF_AMOUNT_TQ,
                     SUM(REC.DIFF_AMOUNT_TQ_2) DIFF_AMOUNT_TQ_2,
                     SUM(REC.DIFF_AMOUNT_RQ) DIFF_AMOUNT_RQ,
                     'GENERATED' DISP_STATUS,
                     SYSDATE DISP_STATUS_DT,
                     'DISPUTE BELOW THRESHOLD' DISP_REMARKS, --- default value 
                     'NO_DISPUTE' RECON_STATUS,
                     SYSDATE RECON_STATUS_DT,
                     ACTIVE_FLAG,
                     MASTER_FLAG,
                     TIMEZONE_FLAG,
                     PROD_FLAG,
                     SUB_DEL_FLAG,
                     ORIG_DEST_FLAG,
                     CTRY_OPER_FLAG,
                     THRESH_TYPE_TQ,
                     THRESH_VALUE_TQ,
                     THRESH_PERC_TQ,
                     THRESH_TYPE_RQ,
                     THRESH_VALUE_RQ,
                     THRESH_PERC_RQ,
                     THRESH_OVRIDE,
                     MAX(TO_DATE(SUBSTR(TRAFFIC_PERIOD, 1, 10), 'YYYY/MM/DD')) TRAFFIC_STDATE
                FROM T_SET_INVOICE_RECON REC
               GROUP BY FRANCHISE,
                        MST_OPR_CODE,
                        PRODUCT_GROUP,
                        TRAFFIC_PERIOD,
                        BILLING_METHOD,
                        STATEMENT_DIRECTION,
                        CASH_FLOW,
                        CURRENCY,
                        ACTIVE_FLAG,
                        MASTER_FLAG,
                        TIMEZONE_FLAG,
                        PROD_FLAG,
                        SUB_DEL_FLAG,
                        ORIG_DEST_FLAG,
                        CTRY_OPER_FLAG,
                        THRESH_TYPE_TQ,
                        THRESH_VALUE_TQ,
                        THRESH_PERC_TQ,
                        THRESH_TYPE_RQ,
                        THRESH_VALUE_RQ,
                        THRESH_PERC_RQ,
                        THRESH_OVRIDE
               ORDER BY FRANCHISE,
                        MST_OPR_CODE,
                        PRODUCT_GROUP,
                        TRAFFIC_PERIOD,
                        BILLING_METHOD,
                        STATEMENT_DIRECTION,
                        CASH_FLOW,
                        CURRENCY);
  
    v_logmsg := 'SETTING HEADER_ID  IN THE  RECON TABLE From HEADER TABLE';
    auditloggerProcedure(v_procName, v_logmsg);
    BEGIN
      MERGE INTO T_SET_INVOICE_RECON REC
      USING (SELECT DISTINCT FRANCHISE,
                             MST_OPR_CODE,
                             PRODUCT_GROUP,
                             TRAFFIC_PERIOD,
                             BILLING_METHOD,
                             STATEMENT_DIRECTION,
                             CASH_FLOW,
                             CURRENCY,
                             HDR_ID
               FROM S_SET_INV_RECON_HDR) HDR
      ON (HDR.MST_OPR_CODE = REC.MST_OPR_CODE AND HDR.product_group = REC.product_group AND HDR.traffic_period = REC.traffic_period AND HDR.CASH_FLOW = REC.CASH_FLOW AND HDR.BILLING_METHOD = REC.BILLING_METHOD AND HDR.STATEMENT_DIRECTION = REC.STATEMENT_DIRECTION AND HDR.CURRENCY = REC.CURRENCY)
      WHEN MATCHED THEN
        UPDATE SET REC.HDR_ID = HDR.HDR_ID;
    END;
  
    ----------update DISP_AMT_TOTAL and  NET fields  for DIFF_TYPE != 'N' and  Not NULL in HDR-SESS
    v_logmsg := 'SETTING RATE DISPUTE AMOUNT IN THE HEADER TABLE FOR REPORTING and FOLLOW-UP';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      MERGE INTO S_SET_INV_RECON_HDR HDR
      USING (SELECT HDR_ID,
                    SUM(RECN.CALL_COUNT) disp_ldr_count,
                    SUM(RECN.UNITS) disp_ldr_units,
                    SUM(RECN.NET_AMOUNT) disp_ldr_amount,
                    SUM(RECN.TAX_AMOUNT) disp_ldr_tax,
                    SUM(RECN.INTEC_COUNT) disp_int_count,
                    SUM(RECN.INTEC_AMOUNT) disp_int_amount,
                    SUM(RECN.INTEC_AMOUNT_2) disp_int_amount_2,
                    SUM(RECN.INTEC_UNITS) disp_int_units,
                    SUM(RECN.DIFF_COUNT) disp_count,
                    SUM(RECN.DIFF_UNITS) disp_units,
                    ROUND(AVG(RECN.DIFF_RATE), 4) disp_rate_avg,
                    SUM(RECN.DIFF_AMOUNT_TQ) disp_amount_tq,
                    SUM(RECN.DIFF_AMOUNT_TQ_2) disp_amount_tq2,
                    SUM(RECN.DIFF_AMOUNT_RQ) disp_amount_rq2,
                    SUM(RECN.DIFF_AMOUNT_TQ_2) DISP_AMT_BAL_TQ2,
                    SUM(RECN.DIFF_AMOUNT_RQ) DISP_AMT_BAL_RQ2,
                    'GENERATED' DISP_STATUS,
                    'DISPUTE ABOVE THRESHOLD' DISP_REMARKS,
                    'GENERATED' RECON_STATUS
               FROM T_SET_INVOICE_RECON RECN
              WHERE HDR_ID > 0
                AND DIFF_TYPE != 'N'
                AND DIFF_TYPE IS NOT NULL
                AND DISP_TYPE_RQ IS NOT NULL
                AND DIFF_AMOUNT_RQ > 0
              GROUP BY HDR_ID) REC
      ON (HDR.HDR_ID = REC.HDR_ID)
      WHEN MATCHED THEN
        UPDATE
           SET HDR.disp_ldr_count   = REC.disp_ldr_count,
               HDR.disp_ldr_units   = REC.disp_ldr_units,
               HDR.disp_ldr_amount  = REC.disp_ldr_amount,
               HDR.disp_ldr_tax     = REC.disp_ldr_tax,
               HDR.disp_int_count   = REC.disp_int_count,
               HDR.disp_int_amount  = REC.disp_int_amount,
               HDR.disp_int_units   = REC.disp_int_units,
               HDR.disp_count       = REC.disp_count,
               HDR.disp_units       = REC.disp_units,
               HDR.disp_rate_avg    = REC.disp_rate_avg,
               HDR.disp_amount_rq2  = REC.disp_amount_rq2,
               HDR.DISP_AMT_BAL_RQ2 = REC.DISP_AMT_BAL_RQ2,
               HDR.DISP_STATUS      = REC.DISP_STATUS,
               HDR.RECON_STATUS     = REC.RECON_STATUS,
               HDR.DISP_REMARKS     = REC.DISP_REMARKS;
    
    END;
  
    v_logmsg := 'SETTING TRAFFIC DISPUTE AMOUNT IN THE HEADER TABLE FOR REPORTING and FOLLOW-UP';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      MERGE INTO S_SET_INV_RECON_HDR HDR
      USING (SELECT HDR_ID,
                    SUM(RECN.CALL_COUNT) disp_ldr_count,
                    SUM(RECN.UNITS) disp_ldr_units,
                    SUM(RECN.NET_AMOUNT) disp_ldr_amount,
                    SUM(RECN.TAX_AMOUNT) disp_ldr_tax,
                    SUM(RECN.INTEC_COUNT) disp_int_count,
                    SUM(RECN.INTEC_AMOUNT) disp_int_amount,
                    SUM(RECN.INTEC_AMOUNT_2) disp_int_amount_2,
                    SUM(RECN.INTEC_UNITS) disp_int_units,
                    SUM(RECN.DIFF_COUNT) disp_count,
                    SUM(RECN.DIFF_UNITS) disp_units,
                    ROUND(AVG(RECN.DIFF_RATE), 4) disp_rate_avg,
                    SUM(RECN.DIFF_AMOUNT_TQ) disp_amount_tq,
                    SUM(RECN.DIFF_AMOUNT_TQ_2) disp_amount_tq2,
                    SUM(RECN.DIFF_AMOUNT_RQ) disp_amount_rq2,
                    SUM(RECN.DIFF_AMOUNT_TQ_2) DISP_AMT_BAL_TQ2,
                    SUM(RECN.DIFF_AMOUNT_RQ) DISP_AMT_BAL_RQ2,
                    'GENERATED' DISP_STATUS,
                    'DISPUTE ABOVE THRESHOLD' DISP_REMARKS,
                    'GENERATED' RECON_STATUS
               FROM T_SET_INVOICE_RECON RECN
              WHERE HDR_ID > 0
                AND DIFF_TYPE != 'N'
                AND DIFF_TYPE IS NOT NULL
                AND DISP_TYPE_TQ IS NOT NULL
                AND DIFF_AMOUNT_TQ_2 != 0
              GROUP BY HDR_ID) REC
      ON (HDR.HDR_ID = REC.HDR_ID)
      WHEN MATCHED THEN
        UPDATE
           SET HDR.disp_ldr_count    = REC.disp_ldr_count,
               HDR.disp_ldr_units    = REC.disp_ldr_units,
               HDR.disp_ldr_amount   = REC.disp_ldr_amount,
               HDR.disp_ldr_tax      = REC.disp_ldr_tax,
               HDR.disp_int_count    = REC.disp_int_count,
               HDR.disp_int_amount   = REC.disp_int_amount,
               HDR.disp_int_units    = REC.disp_int_units,
               HDR.disp_count        = REC.disp_count,
               HDR.disp_units        = REC.disp_units,
               HDR.disp_rate_avg     = REC.disp_rate_avg,
               HDR.disp_int_amount_2 = REC.disp_int_amount_2,
               HDR.disp_amount_tq    = REC.disp_amount_tq,
               HDR.disp_amount_tq2   = REC.disp_amount_tq2,
               HDR.DISP_AMT_BAL_TQ2  = REC.DISP_AMT_BAL_TQ2,
               HDR.DISP_STATUS       = REC.DISP_STATUS,
               HDR.RECON_STATUS      = REC.RECON_STATUS,
               HDR.DISP_REMARKS      = REC.DISP_REMARKS;
    
    END;
  
    v_logmsg := 'SETTING INVOICE NUMBERS AS MERGED IN THE HDR TABLE';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      MERGE INTO S_SET_INV_RECON_HDR HDR
      USING (SELECT FRANCHISE,
                    MST_OPR_CODE,
                    PRODUCT_GROUP,
                    TRAFFIC_PERIOD,
                    CASH_FLOW,
                    BILLING_METHOD,
                    STATEMENT_DIRECTION,
                    CURRENCY,
                    listagg(INVOICE_NUMBER, ' , ') within group(order by INVOICE_NUMBER) INVOICE_NUMBER
               FROM (SELECT distinct FRANCHISE,
                                     MST_OPR_CODE,
                                     PRODUCT_GROUP,
                                     TRAFFIC_PERIOD,
                                     CASH_FLOW,
                                     BILLING_METHOD,
                                     STATEMENT_DIRECTION,
                                     CURRENCY,
                                     INVOICE_NUMBER
                       FROM S_INV_RECON_LDR_DTL
                      ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9)
              GROUP BY FRANCHISE,
                       MST_OPR_CODE,
                       PRODUCT_GROUP,
                       TRAFFIC_PERIOD,
                       CASH_FLOW,
                       BILLING_METHOD,
                       STATEMENT_DIRECTION,
                       CURRENCY) REC
      ON (HDR.MST_OPR_CODE = REC.MST_OPR_CODE AND HDR.PRODUCT_GROUP = REC.PRODUCT_GROUP AND HDR.TRAFFIC_PERIOD = REC.TRAFFIC_PERIOD AND HDR.CASH_FLOW = REC.CASH_FLOW AND HDR.BILLING_METHOD = REC.BILLING_METHOD AND HDR.STATEMENT_DIRECTION = REC.STATEMENT_DIRECTION AND HDR.CURRENCY = REC.CURRENCY)
      WHEN MATCHED THEN
        UPDATE SET HDR.INVOICE_NUMBER = REC.INVOICE_NUMBER;
    END;
  
    DBMS_OUTPUT.PUT_LINE('..........SETTING CURRENT INVOICE STATUS and  TRAFFIC ST_DATE IN THE HDR TABLE');
    v_logmsg := 'SETTING CURRENT INVOICE STATUS and  TRAFFIC ST_DATE IN THE HDR TABLE ';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      MERGE INTO S_SET_INV_RECON_HDR REC
      USING (SELECT TRN.FRANCHISE,
                    OPR.MST_CODE MST_OPR_CODE,
                    TRN.PRODUCT_GROUP,
                    TRN.TRAFFIC_PERIOD,
                    TRN.BILLING_METHOD,
                    TRN.STATEMENT_DIRECTION,
                    TRN.CASH_FLOW,
                    TRN.CURRENCY,
                    MAX(TRN.STATUS) || '-' || COUNT(DISTINCT TRN.status) INV_STATUS,
                    --- MAX(TO_DATE(SUBSTR(TRAFFIC_PERIOD,1,10),'YYYY/MM/DD'))  TRAFFIC_STDATE,  --- done already above 
                    MAX(TRN.USER_NAME) USER_NAME
               FROM VW_ACCOUNT_TRANSACTIONS TRN, VW_MST_OPR_PRD_COMBO OPR
              WHERE (TRN.ICT_OPERATOR, TRN.PRODUCT_GROUP, TRN.TRAFFIC_PERIOD,
                     TRN.CASH_FLOW, TRN.BILLING_METHOD,
                     TRN.STATEMENT_DIRECTION, TRN.CURRENCY) IN
                    (SELECT distinct OPERATOR,
                                     PRODUCT_GROUP,
                                     TRAFFIC_PERIOD,
                                     CASH_FLOW,
                                     BILLING_METHOD,
                                     STATEMENT_DIRECTION,
                                     CURRENCY
                       FROM S_INV_RECON_LDR_DTL)
                AND TRN.ICT_OPERATOR(+) = OPR.CHLD_CODE
                AND TRN.PRODUCT_GROUP = OPR.PRD_GRP
                AND TRN.ACCOUNT_TRANSACTION = 'DOCUMENT_RECEIVED'
                AND TRN.CONDITION = 'ACTUAL'
              GROUP BY TRN.FRANCHISE,
                       MST_CODE,
                       PRODUCT_GROUP,
                       TRAFFIC_PERIOD,
                       BILLING_METHOD,
                       STATEMENT_DIRECTION,
                       CASH_FLOW,
                       CURRENCY) TRNS
      ON (TRNS.MST_OPR_CODE = REC.MST_OPR_CODE AND TRNS.product_group = REC.product_group AND TRNS.traffic_period = REC.traffic_period AND TRNS.CASH_FLOW = REC.CASH_FLOW AND TRNS.BILLING_METHOD = REC.BILLING_METHOD AND TRNS.STATEMENT_DIRECTION = REC.STATEMENT_DIRECTION AND TRNS.CURRENCY = REC.CURRENCY)
      WHEN MATCHED THEN
        UPDATE
           SET REC.INV_STATUS = TRNS.INV_STATUS,
               --- REC.TRAFFIC_STDATE = TRNS.TRAFFIC_STDATE , --- done already above
               REC.CREATED_BY   = TRNS.USER_NAME,
               REC.CREATED_DATE = SYSDATE;
    
    END;
  
    --- drop2-uc9 -- insert only if no recs in map table  **********************
  
    v_logmsg := 'MOVING UN_MAPPED DESTINATIONS INTO MAPPING TABLE FOR USER ACTION ';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_IPMX_DEST_MAPPING
      (PRODUCT_GROUP,
       OPR_CODE,
       OPR_NAME,
       OPR_DEST_NAME,
       MAPPING_STATUS,
       CREATED_BY,
       CREATED_DATE,
       LAST_MODIFIED_BY,
       LAST_MODIFIED_DATE)
      SELECT DISTINCT REC.PRODUCT_GROUP,
                      REC.MST_OPR_CODE,
                      OPR.NAME,
                      REC.DESCRIPTION,
                      'PENDING',
                      USER,
                      SYSDATE,
                      USER,
                      SYSDATE
        FROM S_INV_RECON_LDR_DTL REC, ORGANISATION OPR
       WHERE REC.MST_OPR_CODE = OPR.ID
         AND REC.DIFF_TYPE = 'E'
         AND REC.DIFF_DESCR = 'NO DEST MAPPING'
         AND (REC.PRODUCT_GROUP, REC.MST_OPR_CODE, REC.DESCRIPTION) NOT IN
             (SELECT DISTINCT PRODUCT_GROUP, OPR_CODE, OPR_DEST_NAME
                FROM T_IPMX_DEST_MAPPING);
  
    v_logmsg := 'NUMBER OF UN_MAPPED DESTINATIONS MOVED TO MAPPING TABLE' ||
                SQL%ROWCOUNT;
    auditloggerProcedure(v_procName, v_logmsg);
  
    --- Below 2 Steps Added On 26/10/2021 : CR-226441 - Sending EDRs - Billing Reconciliation Portal 
    v_logmsg := 'LOADING ETISALAT DESTN NAME and  NAAG CODES IN A NEW TABLE FOR SEND_CDR PROJECT';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_INV_RECON_DESTNS
      (ID, MST_OPR_CODE, HDR_ID, INTEC_COUNTRY, NAME, DEST_NAAG)
      SELECT DISTINCT 0                   AS ID,
                      MST_OPR_CODE,
                      HDR_ID,
                      INTEC_COUNTRY,
                      INTEC_DEST          as NAME,
                      AGREEMENT_NAAG_BNUM AS DEST_NAAG
        FROM S_INV_RECON_INTEC_dtl DTL, T_SET_INVOICE_RECON REC
       WHERE DTL.GROUP_OPERATOR = REC.MST_OPR_CODE
         AND DTL.TRAFFIC_PERIOD = REC.TRAFFIC_PERIOD
         AND DTL.CASH_FLOW = REC.CASH_FLOW
         AND DTL.BILLING_METHOD = REC.BILLING_METHOD
         AND DTL.COUNTRY_NAME = REC.INTEC_COUNTRY
         AND DTL.ORIG_DEST_NAME = REC.INTEC_DEST
         AND INTEC_DEST IS NOT NULL;
  
    v_logmsg := 'SETTING ORIG INV AMT AS NET AMOUNT FOR VBR RECORDS IN LDR DTL FOR NEXT STEP';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE S_INV_RECON_LDR_DTL
       SET NET_AMOUNT = NET_AMOUNT_ORIG
     WHERE (MST_OPR_CODE, TRAFFIC_PERIOD, INVOICE_NUMBER, COUNTRY_NAME,
            ORIG_DEST_NAME, RATE) IN
           (SELECT MST_OPR_CODE,
                   TRAFFIC_PERIOD,
                   INVOICE_NUMBER,
                   COUNTRY,
                   ORIG_DEST_NAME,
                   RATE
              FROM S_INV_RECON_LDR_SUM
             WHERE COUNTRY_NAME = 'VBR');
  
    v_logmsg := 'LOADING INVOICE DETAILS INTO T_SET_INV_RECON_INVOICES';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_INV_RECON_INVOICES
      (ID,
       MST_OPR_CODE,
       HDR_ID,
       INVOICE_NUMBER,
       NET_AMOUNT,
       NET_AMOUNT_ORIG,
       TAX_AMOUNT,
       GROSS_AMOUNT,
       CURRENCY)
      WITH REC_HDR AS
       (SELECT DISTINCT FRANCHISE,
                        HDR_ID,
                        MST_OPR_CODE,
                        PRODUCT_GROUP,
                        TRAFFIC_PERIOD,
                        BILLING_METHOD,
                        STATEMENT_DIRECTION,
                        CASH_FLOW,
                        CURRENCY
          FROM T_SET_INVOICE_RECON)
      SELECT T_SET_INV_RECON_INVOICES_SEQ.NEXTVAL as ID,
             MST_OPR_CODE,
             HDR_ID,
             INVOICE_NUMBER,
             NET_AMOUNT,
             NET_AMOUNT_ORIG,
             TAX_AMOUNT,
             GROSS_AMOUNT,
             CURRENCY
        FROM (SELECT REC.MST_OPR_CODE,
                     REC.HDR_ID,
                     LDR.INVOICE_NUMBER,
                     LDR.CURRENCY,
                     SUM(LDR.NET_AMOUNT) NET_AMOUNT,
                     SUM(LDR.NET_AMOUNT_ORIG) NET_AMOUNT_ORIG,
                     SUM(LDR.TAXES) TAX_AMOUNT,
                     SUM(LDR.GROSS_AMOUNT) GROSS_AMOUNT
                FROM REC_HDR REC, S_INV_RECON_LDR_DTL LDR
               WHERE LDR.MST_OPR_CODE = REC.MST_OPR_CODE
                 AND LDR.product_group = REC.product_group
                 AND LDR.traffic_period = REC.traffic_period
                 AND LDR.CASH_FLOW = REC.CASH_FLOW
                 AND LDR.BILLING_METHOD = REC.BILLING_METHOD
                 AND LDR.STATEMENT_DIRECTION = REC.STATEMENT_DIRECTION
                 AND LDR.CURRENCY = REC.CURRENCY
               GROUP BY REC.HDR_ID,
                        REC.MST_OPR_CODE,
                        LDR.INVOICE_NUMBER,
                        LDR.CURRENCY);
  
    v_logmsg := 'SETTING INVOICE RECEIVED DATE IN INV TABLE FROM LDR-SUM TBL FOR INV-360';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
      MERGE INTO T_SET_INV_RECON_INVOICES REC
      USING (SELECT HDR.MST_OPR_CODE,
                    HDR.HDR_ID,
                    REC.INVOICE_NUMBER,
                    MAX(REC.INV_RCVD_DATE) INV_RCVD_DATE
               FROM S_INV_RECON_LDR_SUM REC, S_SET_INV_RECON_HDR HDR
              WHERE HDR.MST_OPR_CODE = REC.MST_OPR_CODE
                AND HDR.product_group = REC.product_group
                AND HDR.traffic_period = REC.traffic_period
                AND HDR.CASH_FLOW = REC.CASH_FLOW
                AND HDR.BILLING_METHOD = REC.BILLING_METHOD
                AND HDR.STATEMENT_DIRECTION = REC.STATEMENT_DIRECTION
                AND HDR.CURRENCY = REC.CURRENCY
              GROUP BY HDR.MST_OPR_CODE, HDR.HDR_ID, REC.INVOICE_NUMBER) H
      ON (REC.HDR_ID = H.HDR_ID AND REC.MST_OPR_CODE = H.MST_OPR_CODE AND REC.INVOICE_NUMBER = H.INVOICE_NUMBER)
      WHEN MATCHED THEN
        UPDATE SET REC.INV_RCVD_DATE = H.INV_RCVD_DATE;
    
    END;
  
    v_logmsg := 'PPM243390- INV360 - Setting IPMx Approval status for Invoices';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE T_SET_INV_RECON_INVOICES
       SET APPROVE_FLAG_IPMX = 'A', APPROVE_USER_IPMX = 'SYSTEM'
     WHERE HDR_ID IN (SELECT HDR_ID
                        FROM S_SET_INV_RECON_HDR
                       WHERE DISP_STATUS = 'AUTO CLOSED');
  
    v_logmsg := 'PPM243390- Upgrade - Setting Agreement ID in the Invoices Table';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      MERGE INTO T_SET_INV_RECON_INVOICES INV
      USING (SELECT DISTINCT MST_OPR_CODE,
                             HDR_ID,
                             ACT.INVOICE_NUMBER,
                             ACT.FRANCHISE,
                             ACT.PRODUCT_GROUP,
                             ACT.TRAFFIC_PERIOD,
                             ACT.CASH_FLOW,
                             ACT.BILLING_METHOD,
                             ACT.STATEMENT_DIRECTION,
                             ACT.CURRENCY,
                             ACT.AGREEMENT_ID
               FROM VW_ACCOUNT_TRANSACTIONS ACT, S_SET_INV_RECON_HDR REC
              WHERE ACT.OPERATOR = REC.MST_OPR_CODE
                AND ACT.INVOICE_NUMBER = REC.INVOICE_NUMBER
                AND ACT.product_group = REC.product_group
                AND ACT.traffic_period = REC.traffic_period
                AND ACT.CASH_FLOW = REC.CASH_FLOW
                AND ACT.BILLING_METHOD = REC.BILLING_METHOD
                AND ACT.STATEMENT_DIRECTION = REC.STATEMENT_DIRECTION
                AND ACT.CURRENCY = REC.CURRENCY) FAM
      ON (INV.MST_OPR_CODE = FAM.MST_OPR_CODE AND INV.INVOICE_NUMBER = FAM.INVOICE_NUMBER AND INV.HDR_ID = FAM.HDR_ID)
      WHEN MATCHED THEN
        UPDATE SET INV.AGREEMENT_ID = FAM.AGREEMENT_ID;
    
    END;
  
    v_logmsg := 'DROP3- MARKING ReRun STATUS FOR THOSE NEW RECONS GENERATED BY PORTAL REQUEST ';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
      MERGE INTO S_SET_INV_RECON_HDR DES
      USING (SELECT DISTINCT FRANCHISE,
                             MST_OPR_CODE,
                             PRODUCT_GROUP,
                             TRAFFIC_PERIOD,
                             CASH_FLOW,
                             BILLING_METHOD,
                             STATEMENT_DIRECTION,
                             CURRENCY,
                             LAST_MODIFIED_BY,
                             HDR_ID PRV_HDR_ID
               FROM T_SET_INV_RECON_HDR
              WHERE USER_ACTION = 'ReRun'
                AND ACTION_STATUS = 'RUNNING') SRC
      ON (DES.MST_OPR_CODE = SRC.MST_OPR_CODE AND DES.TRAFFIC_PERIOD = SRC.TRAFFIC_PERIOD AND DES.PRODUCT_GROUP = SRC.PRODUCT_GROUP AND DES.CASH_FLOW = SRC.CASH_FLOW AND DES.CURRENCY = SRC.CURRENCY AND DES.BILLING_METHOD = SRC.BILLING_METHOD AND DES.STATEMENT_DIRECTION = SRC.STATEMENT_DIRECTION)
      WHEN MATCHED THEN
        UPDATE ----SET   RECON_STATUS = 'ReRun' , CREATED_BY = SRC.LAST_MODIFIED_BY ;
           SET RECON_STATUS = 'ReRun',
               CREATED_BY   = NVL(SRC.LAST_MODIFIED_BY, 'SYSTEM'),
               RECON_VER    = SRC.PRV_HDR_ID; -- Chg-27 ;
    
    END;
  
    v_logmsg := 'Chg-27 - Setting Prev Latest Recon For The Fresh / ReRun Recon To Show Audit Logs in Portal';
    auditloggerProcedure(v_procName, v_logmsg);
    BEGIN
      MERGE INTO S_SET_INV_RECON_HDR DES
      USING (SELECT FRANCHISE,
                    MST_OPR_CODE,
                    PRODUCT_GROUP,
                    TRAFFIC_PERIOD,
                    CASH_FLOW,
                    BILLING_METHOD,
                    STATEMENT_DIRECTION,
                    CURRENCY,
                    MAX(HDR_ID) PRV_HDR_ID
               FROM T_SET_INV_RECON_HDR_HIST
              WHERE (MST_OPR_CODE, PRODUCT_GROUP, TRAFFIC_PERIOD, CASH_FLOW,
                     BILLING_METHOD, STATEMENT_DIRECTION, CURRENCY) IN
                    (SELECT MST_OPR_CODE,
                            PRODUCT_GROUP,
                            TRAFFIC_PERIOD,
                            CASH_FLOW,
                            BILLING_METHOD,
                            STATEMENT_DIRECTION,
                            CURRENCY
                       FROM S_SET_INV_RECON_HDR
                      WHERE RECON_VER < 10) --- fresh or reloaded recons
              GROUP BY FRANCHISE,
                       MST_OPR_CODE,
                       PRODUCT_GROUP,
                       TRAFFIC_PERIOD,
                       CASH_FLOW,
                       BILLING_METHOD,
                       STATEMENT_DIRECTION,
                       CURRENCY) SRC
      ON (DES.MST_OPR_CODE = SRC.MST_OPR_CODE AND DES.TRAFFIC_PERIOD = SRC.TRAFFIC_PERIOD AND DES.PRODUCT_GROUP = SRC.PRODUCT_GROUP AND DES.CASH_FLOW = SRC.CASH_FLOW AND DES.CURRENCY = SRC.CURRENCY AND DES.BILLING_METHOD = SRC.BILLING_METHOD AND DES.STATEMENT_DIRECTION = SRC.STATEMENT_DIRECTION)
      WHEN MATCHED THEN
        UPDATE SET RECON_VER = SRC.PRV_HDR_ID;
    END;
  
    v_logmsg := 'DROP3- SETTING DELETE STATUS FOR GENERATED AND  COMPLETED STATUS FOR OTHER RECONS MARKED FOR RE-RUN ';
    auditloggerProcedure(v_procName, v_logmsg);
    UPDATE T_SET_INV_RECON_HDR A
       SET A.RECON_STATUS     = DISP_STATUS, --- save orig status for auditing 
           A.DISP_STATUS      = decode(DISP_STATUS,
                                       'GENERATED',
                                       'DELETED',
                                       DISP_STATUS),
           A.ACTION_STATUS    = 'COMPLETED',
           LAST_MODIFIED_DATE = sysdate
     WHERE ((A.USER_ACTION = 'ReRun' AND A.ACTION_STATUS = 'RUNNING') OR
           A.DISP_STATUS = 'GENERATED')
       AND (MST_OPR_CODE, PRODUCT_GROUP, TRAFFIC_PERIOD, CASH_FLOW,
            BILLING_METHOD, STATEMENT_DIRECTION, CURRENCY) IN
           (SELECT DISTINCT MST_OPR_CODE,
                            PRODUCT_GROUP,
                            TRAFFIC_PERIOD,
                            CASH_FLOW,
                            BILLING_METHOD,
                            STATEMENT_DIRECTION,
                            CURRENCY
              FROM S_SET_INV_RECON_HDR CUR);
  
    v_logmsg := 'DROP3: MOVE ALL EXISTING RECONS FOR SAME OPR INTO HISTORY TABLE FOR DRILL-DOWN';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_INV_RECON_HDR_HIST
      SELECT *
        FROM T_SET_INV_RECON_HDR HIST
       WHERE (MST_OPR_CODE, PRODUCT_GROUP, TRAFFIC_PERIOD, CASH_FLOW,
              BILLING_METHOD, STATEMENT_DIRECTION, CURRENCY) IN
             (SELECT DISTINCT MST_OPR_CODE,
                              PRODUCT_GROUP,
                              TRAFFIC_PERIOD,
                              CASH_FLOW,
                              BILLING_METHOD,
                              STATEMENT_DIRECTION,
                              CURRENCY
                FROM S_SET_INV_RECON_HDR CUR);
  
    v_logmsg := 'DROP3: DELETE PREVIOUSLY *** GENERATED***  DETAIL RECORDS FOR SAME OPERATOR';
    auditloggerProcedure(v_procName, v_logmsg);
  
    --MB Started..
    DELETE FROM T_SET_INVOICE_RECON_HIST HIST
     WHERE (MST_OPR_CODE, HDR_ID, PRODUCT_GROUP, TRAFFIC_PERIOD, CASH_FLOW,
            BILLING_METHOD, STATEMENT_DIRECTION, CURRENCY) IN
           (SELECT DISTINCT MST_OPR_CODE,
                            HDR_ID,
                            PRODUCT_GROUP,
                            TRAFFIC_PERIOD,
                            CASH_FLOW,
                            BILLING_METHOD,
                            STATEMENT_DIRECTION,
                            CURRENCY
              FROM T_SET_INV_RECON_HDR PREV
             WHERE DISP_STATUS = 'DELETED'
               AND (MST_OPR_CODE, PRODUCT_GROUP, TRAFFIC_PERIOD, CASH_FLOW,
                    BILLING_METHOD, STATEMENT_DIRECTION, CURRENCY) IN
                   (SELECT DISTINCT MST_OPR_CODE,
                                    PRODUCT_GROUP,
                                    TRAFFIC_PERIOD,
                                    CASH_FLOW,
                                    BILLING_METHOD,
                                    STATEMENT_DIRECTION,
                                    CURRENCY
                      FROM S_SET_INV_RECON_HDR CUR));
  
    v_logmsg := 'DROP3: DELETE DOCUMENT RECORDS FOR THE PREV *** GENERATED***  HEADER RECORDS FOR SAME OPERATOR';
    auditloggerProcedure(v_procName, v_logmsg);
  
    DELETE FROM T_SET_INV_RECON_DOCUMENT
     WHERE document_id in
           (SELECT RECON_RESULT_HTML_FILE_ID document_id
              FROM T_SET_INV_RECON_HDR
             WHERE (MST_OPR_CODE, HDR_ID) IN
                   (SELECT DISTINCT MST_OPR_CODE, HDR_ID
                      FROM T_SET_INV_RECON_HDR PREV
                     WHERE DISP_STATUS = 'DELETED'
                       AND (MST_OPR_CODE, PRODUCT_GROUP, TRAFFIC_PERIOD,
                            CASH_FLOW, BILLING_METHOD, STATEMENT_DIRECTION,
                            CURRENCY) IN
                           (SELECT DISTINCT MST_OPR_CODE,
                                            PRODUCT_GROUP,
                                            TRAFFIC_PERIOD,
                                            CASH_FLOW,
                                            BILLING_METHOD,
                                            STATEMENT_DIRECTION,
                                            CURRENCY
                              FROM S_SET_INV_RECON_HDR CUR))
            UNION ALL
            SELECT DISP_RESULT_HTML_FILE_ID document_id
              FROM T_SET_INV_RECON_HDR
             WHERE (MST_OPR_CODE, HDR_ID) IN
                   (SELECT DISTINCT MST_OPR_CODE, HDR_ID
                      FROM T_SET_INV_RECON_HDR PREV
                     WHERE DISP_STATUS = 'DELETED'
                       AND (MST_OPR_CODE, PRODUCT_GROUP, TRAFFIC_PERIOD,
                            CASH_FLOW, BILLING_METHOD, STATEMENT_DIRECTION,
                            CURRENCY) IN
                           (SELECT DISTINCT MST_OPR_CODE,
                                            PRODUCT_GROUP,
                                            TRAFFIC_PERIOD,
                                            CASH_FLOW,
                                            BILLING_METHOD,
                                            STATEMENT_DIRECTION,
                                            CURRENCY
                              FROM S_SET_INV_RECON_HDR CUR)));
  
    v_logmsg := 'COPYING FINAL RECON DETAILS FROM CURRENT INTO HISTORY Table';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_INVOICE_RECON_HIST
      SELECT * FROM T_SET_INVOICE_RECON;
  
    IF (SQL%ROWCOUNT > 0) THEN
    
      v_logmsg := 'INSERT CONTROL RECORD INTO T_SET_INV_RECON_CTRL WITH LAST BATCH RUN DATE';
      auditloggerProcedure(v_procName, v_logmsg);
    
      ---INSERT  INTO T_SET_INV_RECON_CONTROL ( LAST_BATCH_DATE, LAST_BATCH_ID , STATUS_FLAG  , STATUS_DESC  , CREATED_DATE  ) 
      ---SELECT  v_rec_stdt , max(HDR_ID) , 'C' , 'COMPLETED' , sysdate
      ---FROM    S_SET_INV_RECON_HDR ;
    
      UPDATE T_SET_INV_RECON_CONTROL
         SET LAST_BATCH_DATE = v_rec_stdt,
             STATUS_FLAG     = 'C',
             STATUS_DESC     = 'COMPLETED';
    
    END IF;
  
    v_logmsg := 'DROP3- AUDITING FOR PREV EXISTING RECONS MATCHING WITH REGENERATED RECONS';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      FOR RRUN IN (SELECT 'INV_RECON_HDR' SOURCE,
                          HDR_ID,
                          'SYSTEM' USERNAME,
                          'BACK_END' USER_FULL_NAME,
                          DISP_STATUS USER_ACTION, --- For Orig Status, use RECON_STATUS field 
                          'MOVED TO HISTORY' USER_COMMENT,
                          SYSTIMESTAMP ACTION_TIMESTATMP
                     FROM T_SET_INV_RECON_HDR
                    WHERE (MST_OPR_CODE, PRODUCT_GROUP, TRAFFIC_PERIOD,
                           CASH_FLOW, BILLING_METHOD, STATEMENT_DIRECTION,
                           CURRENCY) IN
                          (SELECT DISTINCT MST_OPR_CODE,
                                           PRODUCT_GROUP,
                                           TRAFFIC_PERIOD,
                                           CASH_FLOW,
                                           BILLING_METHOD,
                                           STATEMENT_DIRECTION,
                                           CURRENCY
                             FROM S_SET_INV_RECON_HDR CUR)) LOOP
        v_audit_id := 0;
        SELECT T_SET_INV_RECON_AUDIT_SEQ.NEXTVAL into v_audit_id FROM DUAL;
      
        INSERT INTO T_SET_INV_RECON_AUDIT
          (ID,
           SOURCE,
           HDR_ID,
           USERNAME,
           USER_FULL_NAME,
           USER_ACTION,
           USER_COMMENT,
           ACTION_TIMESTATMP)
        VALUES
          (v_audit_id,
           RRUN.SOURCE,
           RRUN.HDR_ID,
           RRUN.USERNAME,
           RRUN.USER_FULL_NAME,
           RRUN.USER_ACTION,
           RRUN.USER_COMMENT,
           RRUN.ACTION_TIMESTATMP);
      
      END LOOP;
    
    END;
  
    v_logmsg := 'Chg-18- SETTING CLOSED STATUS FOR ELIGIBLE OPRs FOR RECONS HAVING NO-DISPUTES';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      MERGE INTO S_SET_INV_RECON_HDR DES
      USING (SELECT MST_OPR_CODE,
                    PROD_GROUP,
                    CASH_FLOW,
                    BILLING_METHOD,
                    CURRENCY,
                    AUTO_CLOSE_FLAG
               FROM T_SET_OPR_RECON_PROFILE
              WHERE AUTO_CLOSE_FLAG = 'Y') SRC
      ON (DES.MST_OPR_CODE = SRC.MST_OPR_CODE AND DES.PRODUCT_GROUP = SRC.PROD_GROUP AND DES.CASH_FLOW = SRC.CASH_FLOW AND DES.CURRENCY = SRC.CURRENCY AND DES.BILLING_METHOD = SRC.BILLING_METHOD)
      WHEN MATCHED THEN
        UPDATE --- SET   APPROVED_FLAG = 'Y' , DISP_STATUS = 'AUTO CLOSED' ,APPROVED_DATE  = SYSDATE ,  -- Replaced ( Chg-30 )
           SET APPROVED_FLAG      = 'AP',
               DISP_STATUS        = 'AUTO CLOSED',
               APPROVED_DATE      = SYSDATE, -- With ( Chg-30)
               USER_ACTION        = 'AutoClose',
               ACTION_STATUS      = 'SUBMITTED',
               LAST_MODIFIED_BY   = 'SYSTEM',
               LAST_MODIFIED_DATE = SYSDATE
         WHERE DISP_REMARKS = 'DISPUTE BELOW THRESHOLD';
    
    END;
  
    v_logmsg := 'Chg-18 = ADDING RECON RECORDS INTO AUDIT TABLE FOR FRESH/Re-Run RECONS/Auto-CLOSURES';
    auditloggerProcedure(v_procName, v_logmsg);
  
    BEGIN
    
      v_audit_id := 0;
      FOR LDR IN (SELECT 'INV_RECON_HDR' SOURCE,
                         HDR_ID,
                         MST_OPR_CODE,
                         DISP_STATUS,
                         --- decode(DISP_STATUS,'AUTO CLOSED','SYSTEM','ReRun','SYSTEM',NVL(CREATED_BY,'SYSTEM'))      USERNAME,  -- 17-Mar-2025 
                         decode(DISP_STATUS, 'GENERATED', 'FM', 'SYSTEM') USERNAME,
                         decode(DISP_STATUS,
                                'AUTO CLOSED',
                                'Auto-Closed',
                                'BACK_END') USER_FULL_NAME,
                         ---decode(DISP_STATUS,'AUTO CLOSED','AutoClose','GENERATED','ReRun','FMLOADER')     USER_ACTION,
                         CASE
                           WHEN DISP_STATUS = 'AUTO CLOSED' THEN
                            'AutoClose'
                           WHEN RECON_STATUS = 'ReRun' THEN
                            'ReRun'
                           ELSE
                            'FMLOADER'
                         END USER_ACTION,
                         decode(DISP_STATUS,
                                'AUTO CLOSED',
                                'Dispute Is CLOSED as Dispute Amount Is Below Threshold',
                                'GENERATED') USER_COMMENT,
                         SYSTIMESTAMP ACTION_TIMESTATMP,
                         RECON_VER as PREV_RECON,
                         RECON_STATUS
                    FROM S_SET_INV_RECON_HDR) LOOP
      
        vs_new_hdr_id  := LDR.HDR_ID;
        vs_prev_hdr_id := LDR.PREV_RECON;
      
        IF (LDR.RECON_STATUS = 'ReRun') THEN
        
          ---Chg-31 : Cloning the documents of Prev Recon to the relevant Re-Generated Recons To Avoid attach once again by user
        
          BEGIN
            INSERT INTO t_set_inv_recon_document
              (DOCUMENT_ID,
               HDR_ID,
               INVOICE_NUMBER,
               DOCUMENT_TYPE,
               DOCUMENT_NAME,
               DOCUMENT_SIZE,
               COMMENTS,
               CONTENT_TYPE,
               CREATED_BY,
               CREATED_DATE)
              select t_set_inv_recon_document_SEQ.NEXTVAL as DOCUMENT_ID, -- seq 
                     HDR_ID,
                     INVOICE_NUMBER,
                     DOCUMENT_TYPE,
                     DOCUMENT_NAME,
                     0                                    as DOCUMENT_SIZE,
                     COMMENTS,
                     CONTENT_TYPE,
                     CREATED_BY,
                     CREATED_DATE
                from (SELECT vs_new_hdr_id HDR_ID,
                             INVOICE_NUMBER,
                             DOCUMENT_TYPE,
                             DOCUMENT_NAME,
                             COMMENTS,
                             CONTENT_TYPE,
                             CREATED_BY,
                             CREATED_DATE
                        from t_set_inv_recon_document
                       where document_type = 'INVOICE'
                         and hdr_id = vs_prev_hdr_id
                         and INVOICE_NUMBER IN
                             (SELECT DISTINCT INVOICE_NUMBER
                                FROM T_SET_INVOICE_RECON
                               WHERE HDR_ID = vs_new_hdr_id) -- only for the latest invoices
                      UNION ALL
                      select vs_new_hdr_id HDR_ID,
                             INVOICE_NUMBER,
                             DOCUMENT_TYPE,
                             DOCUMENT_NAME,
                             COMMENTS,
                             CONTENT_TYPE,
                             CREATED_BY,
                             CREATED_DATE
                        from t_set_inv_recon_document
                       where document_type not like '%RESULT%'
                         AND document_type != 'INVOICE'
                         and hdr_id = vs_prev_hdr_id);
          
          END;
        
        END IF;
      
        v_audit_id      := 0;
        vs_user_comment := '';
      
        SELECT T_SET_INV_RECON_AUDIT_SEQ.NEXTVAL into v_audit_id FROM DUAL;
      
        IF (LDR.DISP_STATUS = 'AUTO CLOSED') -- for inv-360 
         THEN
          UPDATE T_SET_INV_RECON_INVOICES
             SET APPROVE_FLAG_IPMX = 'A',
                 APPROVE_DATE_IPMX = SYSDATE,
                 APPROVE_FLAG_FAM  = 'AP',
                 APPROVE_DATE_FAM  = SYSDATE, -- Chg-30 ( 'A' --> 'AP'  ) 
                 APPROVE_USER_IPMX = 'SYSTEM'
           WHERE HDR_ID = LDR.HDR_ID;
        
          BEGIN
               UPDATE ITUPRDFAM.FM_DOCUMENT_TRANS@V10OP2V11OP
               SET CUSTOM_5 = 'Approve_Request'
             WHERE (AGREEMENT_ID, DOCUMENT_NUMBER) IN
                   (SELECT AGREEMENT_ID, INVOICE_NUMBER
                      FROM T_SET_INV_RECON_INVOICES
                     WHERE HDR_ID = LDR.HDR_ID)
               AND DOCUMENT_STATUS = 'O';
          
          EXCEPTION
            WHEN OTHERS THEN
              v_logmsg := 'ERROR: UPDATE CUSTOM_5 IN FM_DOC..TRANS FOR HDR :' ||
                          LDR.HDR_ID || ',' || sqlerrm;
              auditloggerProcedure(v_procName, v_logmsg);
              error_code := 300;
              ROLLBACK;
              RETURN;
          END;
        
        END IF;
      
        IF (LDR.USER_COMMENT = 'GENERATED') --- Chg-29
         THEN
          BEGIN
            SELECT 'GENERATED  ( CHILD CODES: ' ||
                   listagg(CHLD_CODE, ' , ') within group(order by CHLD_CODE) || ' ) '
              into vs_user_comment
              FROM (SELECT DISTINCT MST_CODE, CHLD_CODE
                      FROM VW_MST_OPR_PRD_COMBO
                     WHERE MST_CODE = LDR.MST_OPR_CODE)
             GROUP BY MST_CODE;
          
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              vs_user_comment := 'GENERATED';
              DBMS_OUTPUT.PUT_LINE('..........Chg-18: No Data in Combo For :' ||
                                   LDR.MST_OPR_CODE);
          END;
        
        ELSE
        
          vs_user_comment := LDR.USER_COMMENT;
        
        END IF;
      
        INSERT INTO T_SET_INV_RECON_AUDIT
          (ID,
           SOURCE,
           HDR_ID,
           USERNAME,
           USER_FULL_NAME,
           USER_ACTION,
           USER_COMMENT,
           ACTION_TIMESTATMP)
        VALUES
          (v_audit_id,
           LDR.SOURCE,
           LDR.HDR_ID,
           LDR.USERNAME,
           LDR.USER_FULL_NAME,
           LDR.USER_ACTION,
           vs_user_comment,
           LDR.ACTION_TIMESTATMP);
      
      END LOOP;
    
    END;
  
    v_logmsg := 'DROP3: DELETE ALL PREV RECON HDRs FOR SAME OPERATOR AS THEY ALREADY TRFRD TO HISTORY';
    auditloggerProcedure(v_procName, v_logmsg);
  
    DELETE FROM T_SET_INV_RECON_HDR PREV
     WHERE (MST_OPR_CODE, PRODUCT_GROUP, TRAFFIC_PERIOD, CASH_FLOW,
            BILLING_METHOD, STATEMENT_DIRECTION, CURRENCY) IN
           (SELECT DISTINCT MST_OPR_CODE,
                            PRODUCT_GROUP,
                            TRAFFIC_PERIOD,
                            CASH_FLOW,
                            BILLING_METHOD,
                            STATEMENT_DIRECTION,
                            CURRENCY
              FROM S_SET_INV_RECON_HDR CUR);
  
    --- enh-6:  new approach : TZONE to be updated in Recon from the last TZONE ReRun Data Which Is Used  In Recon -----
  
    BEGIN
      FOR REC IN (SELECT DISTINCT HDR_ID,
                                  MST_OPR_CODE,
                                  PRODUCT_GROUP,
                                  TRAFFIC_STDATE,
                                  BILLING_METHOD,
                                  STATEMENT_DIRECTION,
                                  CASH_FLOW
                    FROM S_SET_INV_RECON_HDR) LOOP
      
        vn_tzone_opr := '';
      
        BEGIN
          SELECT MAX(TIME_ZONE_OPR)
            into vn_tzone_opr
            FROM T_TZONE_ADJ_JOBS_DTL
           WHERE MST_OPR_CODE = REC.MST_OPR_CODE
             AND HDR_JOB_ID =
                 (SELECT MAX(HDR_JOB_ID)
                    FROM T_TZONE_ADJ_JOBS_DTL
                   WHERE MST_OPR_CODE = REC.MST_OPR_CODE
                     AND PROD_GROUP = REC.PRODUCT_GROUP
                     AND CASH_FLOW = REC.CASH_FLOW
                     AND BILL_METH = REC.BILLING_METHOD
                     AND STMT_DIRN = REC.STATEMENT_DIRECTION
                     AND TRF_FROM_DATE = REC.TRAFFIC_STDATE
                     AND DTL_JOB_STATUS = 'COMPLETED'
                     AND TO_CHAR(REMARKS) IS NULL);
        
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            NULL;
          
        END;
      
        IF (vn_tzone_opr is not null) THEN
        
          BEGIN
            UPDATE S_SET_INV_RECON_HDR
               SET TIMEZONE_OPR = vn_tzone_opr
             WHERE HDR_ID = REC.HDR_ID;
          
          END;
        
        END IF;
      
      END LOOP;
    
    END;
  
    v_logmsg := 'Chg-16: SETTING DISPUTE AMOUNT as ZERO IN CASE OVERALL NET DISP AMT IS -VE';
    auditloggerProcedure(v_procName, v_logmsg);
  
    UPDATE S_SET_INV_RECON_HDR
       SET DISP_AMOUNT_TQ2  = 0,
           DISP_AMT_BAL_TQ2 = 0,
           DISP_AMOUNT_RQ2  = 0,
           DISP_AMT_BAL_RQ2 = 0
     WHERE (DISP_AMOUNT_TQ2 + DISP_AMOUNT_RQ2) < 0;
  
    DBMS_OUTPUT.PUT_LINE('............... Chg-16- DISPUTE AMOUNT UPDATED AS ZERO FOR -VE DISP_AMOUNT = ' ||
                         SQL%ROWCOUNT);
  
    v_logmsg := 'COPYING  FRESH HEADER RECORDS FROM SESSION TO PHYSICAL TABLE';
    auditloggerProcedure(v_procName, v_logmsg);
  
    INSERT INTO T_SET_INV_RECON_HDR
      SELECT * FROM S_SET_INV_RECON_HDR;
  s
    COMMIT;
  
    v_logmsg := 'RECONCILATION PROCEDURE COMPLETED';
    auditloggerProcedure(v_procName, v_logmsg);
  
  END;

END;
/
