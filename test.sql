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
