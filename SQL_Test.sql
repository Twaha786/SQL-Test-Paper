-- Tools used:
-- https://livesql.oracle.com/


-- Question 2:

CREATE TABLE BCM_SUPPLIER (
    SUPPLIER_REF NUMBER GENERATED ALWAYS AS IDENTITY,
    SUPPLIER_NAME VARCHAR2(255) NOT NULL,
    SUPP_CONTACT_NAME VARCHAR2(255),
    SUPP_ADDRESS VARCHAR2(255),
    SUPP_CONTACT_NUMBER VARCHAR2(25),
    SUPP_EMAIL VARCHAR2(255),
    CONSTRAINT BCM_SUPPLIER_PK PRIMARY KEY (SUPPLIER_REF)
);

CREATE TABLE BCM_ORDER (
    ORDER_ID NUMBER GENERATED ALWAYS AS IDENTITY,
    ORDER_REF VARCHAR2(25) NOT NULL,
    SUPPLIER_REF NUMBER NOT NULL,
    ORDER_DATE DATE,
    ORDER_TOTAL_AMOUNT NUMBER,
    ORDER_DESCRIPTION VARCHAR2(255),
    ORDER_STATUS VARCHAR2(25),
    CONSTRAINT BCM_ORDER_PK PRIMARY KEY (ORDER_ID),
    CONSTRAINT BCM_ORDER_FK FOREIGN KEY (SUPPLIER_REF) REFERENCES BCM_SUPPLIER (SUPPLIER_REF)
);

CREATE TABLE BCM_INVOICE (
    INVOICE_ID NUMBER GENERATED ALWAYS AS IDENTITY,
    SUPPLIER_REF NUMBER NOT NULL,
    ORDER_ID NUMBER,
    INVOICE_REFERENCE VARCHAR2(25) NOT NULL,
    INVOICE_DATE DATE NOT NULL,
    INVOICE_STATUS VARCHAR2(25),
    INVOICE_HOLD_REASON VARCHAR2(255),
    INVOICE_AMOUNT NUMBER,
    INVOICE_DESCRIPTION VARCHAR2(255),
    CONSTRAINT BCM_INVOICE_PK PRIMARY KEY (INVOICE_ID),
    CONSTRAINT FK_BCM_INVOICE_SUPPLIER
        FOREIGN KEY (SUPPLIER_REF) REFERENCES BCM_SUPPLIER(SUPPLIER_REF),
    CONSTRAINT FK_BCM_INVOICE_ORDER
        FOREIGN KEY (ORDER_ID) REFERENCES BCM_ORDER(ORDER_ID)
);


CREATE TABLE BCM_ORDER_LINE (
    ORDER_LINE_ID NUMBER GENERATED ALWAYS AS IDENTITY,
    ORDER_ID NUMBER,
    ORDER_LINE_AMOUNT NUMBER,
    PRIMARY KEY (ORDER_LINE_ID),
    CONSTRAINT FK_BCM_ORDER_LINE
        FOREIGN KEY (ORDER_ID) REFERENCES BCM_ORDER(ORDER_ID)
);



-- Question 3:

CREATE OR REPLACE PACKAGE MigrationPackage AS
    PROCEDURE PerformMigration;
END MigrationPackage;


PACKAGE BODY MigrationPackage AS 
 		    PROCEDURE PerformMigration IS 
 		        CURSOR c_order_mgt IS SELECT * FROM XXBCM_ORDER_MGT; 
 		        rec c_order_mgt%ROWTYPE; 
 		 
 		        v_supplier_id BCM_SUPPLIER.SUPPLIER_REF%TYPE; 
 		        v_order_id BCM_ORDER.ORDER_ID%TYPE; 
 		    BEGIN 
 		        -- Loop through XXBCM_ORDER_MGT and populate BCM_SUPPLIER, BCM_ORDER, BCM_ORDER_LINE, and BCM_INVOICE 
 		        FOR rec IN c_order_mgt LOOP 
 		            -- Insert into BCM_SUPPLIER table 
 		            INSERT INTO BCM_SUPPLIER (SUPPLIER_NAME, SUPP_CONTACT_NAME, SUPP_ADDRESS, SUPP_CONTACT_NUMBER, SUPP_EMAIL) 
 		            VALUES (rec.SUPPLIER_NAME, rec.SUPP_CONTACT_NAME, rec.SUPP_ADDRESS, REPLACE(REPLACE(rec.SUPP_CONTACT_NUMBER, '.', ''), ' ', ''), rec.SUPP_EMAIL) 
 		            RETURNING SUPPLIER_REF INTO v_supplier_id; 
 		 
 		             -- Insert into BCM_ORDER table 
 		            IF LENGTH(rec.ORDER_DATE) = 11 THEN 
 		                -- Format "DD-MON-YYYY" 
 		                INSERT INTO BCM_ORDER (ORDER_REF, SUPPLIER_REF, ORDER_TOTAL_AMOUNT, ORDER_DESCRIPTION, ORDER_STATUS, ORDER_DATE) 
 		                VALUES (rec.ORDER_REF, v_supplier_id, REPLACE(REPLACE(REPLACE(REPLACE(rec.ORDER_TOTAL_AMOUNT, ',', ''), 'o', '0'), 'I', '1'), 'S', '5'), rec.ORDER_DESCRIPTION, rec.ORDER_STATUS, TO_DATE(rec.ORDER_DATE, 'DD-MON-YYYY')) 
 		                RETURNING ORDER_ID INTO v_order_id;  
 		            ELSE 
 		                -- Format "MM/DD/YYYY" 
 		                INSERT INTO BCM_ORDER (ORDER_REF, SUPPLIER_REF, ORDER_TOTAL_AMOUNT, ORDER_DESCRIPTION, ORDER_STATUS, ORDER_DATE) 
 		                VALUES (rec.ORDER_REF, v_supplier_id, REPLACE(REPLACE(REPLACE(REPLACE(rec.ORDER_TOTAL_AMOUNT, ',', ''), 'o', '0'), 'I', '1'), 'S', '5'), rec.ORDER_DESCRIPTION, rec.ORDER_STATUS, TO_DATE(rec.ORDER_DATE, 'DD-MM-YYYY')) 
 		                RETURNING ORDER_ID INTO v_order_id; 
 		            END IF; 		 		            
				-- Insert into BCM_ORDER_LINE table 
 		            INSERT INTO BCM_ORDER_LINE (ORDER_ID, ORDER_LINE_AMOUNT) 
 		            VALUES (v_order_id, REPLACE(REPLACE(REPLACE(REPLACE(rec.ORDER_LINE_AMOUNT, ',', ''), 'o', '0'), 'I', '1'), 'S', '5')); 
 		 
 		            -- Insert into BCM_INVOICE table 
 		            IF LENGTH(rec.INVOICE_DATE) = 11 THEN 
 		                -- Format "DD-MON-YYYY" 
 		                INSERT INTO BCM_INVOICE (SUPPLIER_REF, ORDER_ID, INVOICE_REFERENCE, INVOICE_DATE, INVOICE_STATUS, INVOICE_HOLD_REASON, INVOICE_AMOUNT, INVOICE_DESCRIPTION) 
 		                VALUES (v_supplier_id, v_order_id, rec.INVOICE_REFERENCE, TO_DATE(rec.INVOICE_DATE, 'DD-MON-YYYY'), rec.INVOICE_STATUS, rec.INVOICE_HOLD_REASON, REPLACE(REPLACE(REPLACE(REPLACE(rec.INVOICE_AMOUNT, ',', ''), 'o', '0'), 'I', '1'), 'S', '5'), rec.INVOICE_DESCRIPTION); 
 		            ELSE 
 		                -- Format "MM/DD/YYYY" 
 		                INSERT INTO BCM_INVOICE (SUPPLIER_REF, ORDER_ID, INVOICE_REFERENCE, INVOICE_DATE, INVOICE_STATUS, INVOICE_HOLD_REASON, INVOICE_AMOUNT, INVOICE_DESCRIPTION) 
 		                VALUES (v_supplier_id, v_order_id, rec.INVOICE_REFERENCE, TO_DATE(rec.INVOICE_DATE, 'DD-MM-YYYY'), rec.INVOICE_STATUS, rec.INVOICE_HOLD_REASON, REPLACE(REPLACE(REPLACE(REPLACE(rec.INVOICE_AMOUNT, ',', ''), 'o', '0'), 'I', '1'), 'S', '5'), rec.INVOICE_DESCRIPTION); 
 		            END IF; 
 		            
 		             
 		          
 		        END LOOP; 
 		 
 		        COMMIT; 
 		    EXCEPTION 
 		        WHEN OTHERS THEN 
 		            ROLLBACK; 
 		            RAISE; 
		    END PerformMigration; 
 		END MigrationPackage; 
 		/


-- QUESTION 4

-- Create a temp table to store summarised data

CREATE TABLE BCM_SUMMARY_ORDERS (
    SUMMARY_ID NUMBER GENERATED ALWAYS AS IDENTITY,
    Order Reference VARCHAR2(50),
    Order Period VARCHAR2(20),
    Supplier Name VARCHAR2(100),
    Order Total Amount VARCHAR2(20),
    Order Status VARCHAR2(20),
    Invoice Reference VARCHAR2(50),
    Invoice Total Amount VARCHAR2(20),
    Action VARCHAR2(20)
);

-- ==================================

CREATE OR REPLACE PROCEDURE GENERATE_SUMMARY_ORDERS AS
BEGIN

     DELETE FROM BCM_SUMMARY_ORDERS;

    -- Insert summarized data into the summary table
    INSERT INTO BCM_SUMMARY_ORDERS (
        "Order Reference",
        "Order Period",
        "Supplier Name",
        "Order Total Amount",
        "Order Status",
        "Invoice Reference",
        "Invoice Total Amount",
        "Action"
    )
     SELECT  
      REPLACE(LTRIM(REPLACE(SUBSTR(O.ORDER_REF, 3), '0', ' ')), ' ', '0'), 
     TO_CHAR(O.ORDER_DATE, 'Mon-YY'),
     INITCAP(SUBSTR(S.SUPPLIER_NAME, 1, 1)) || LOWER(SUBSTR(S.SUPPLIER_NAME, 2)),
     TO_CHAR(SUM(OL.ORDER_LINE_AMOUNT), '99,999,990.00'),
     O.ORDER_STATUS,
     INV.INVOICE_REFERENCE, 
     TO_CHAR(SUM(INV.INVOICE_AMOUNT), '99,999,990.00'), 
     CASE 
           WHEN (INV.INVOICE_STATUS = 'paid') THEN 'OK'
           WHEN (INV.INVOICE_STATUS = 'Pending') THEN 'To follow up'
           ELSE 'To verify'
        END AS Action
    FROM 
        BCM_ORDER O 
        INNER JOIN BCM_SUPPLIER S ON S.SUPPLIER_REF = O.SUPPLIER_REF 
        INNER JOIN BCM_ORDER_LINE OL ON OL.ORDER_ID = O.ORDER_ID 
        INNER JOIN BCM_INVOICE INV ON INV.ORDER_ID = O.ORDER_ID 
    GROUP BY 
        O.ORDER_REF, O.ORDER_DATE, S.SUPPLIER_NAME, O.ORDER_STATUS, INV.INVOICE_REFERENCE, INV.INVOICE_STATUS;
    
    COMMIT;
END GENERATE_SUMMARY_ORDERS;
/
-- ==================================

 -- Display summarized data from the summary table. 
SELECT * FROM BCM_SUMMARY_ORDERS;

￼


-- QUESTION 5


CREATE OR REPLACE FUNCTION GetSecondHighestOrderDetails RETURN SYS_REFCURSOR IS
    order_cursor SYS_REFCURSOR;
BEGIN
    OPEN order_cursor FOR
        SELECT
    O.ORDER_REF,
    TO_CHAR(O.ORDER_DATE, 'Month DD, YYYY') AS order_period,
    UPPER(INITCAP(S.SUPPLIER_NAME)) AS supplier_name,
    TO_CHAR(SUM(OL.ORDER_LINE_AMOUNT), '99,999,990.00') AS order_total,
    O.order_status,
    LISTAGG(I.INVOICE_REFERENCE, ‘|’) WITHIN GROUP (ORDER BY I.INVOICE_REFERENCE) AS "Invoice References"
FROM
    (SELECT
        O.ORDER_REF,
        O.ORDER_DATE,
        O.SUPPLIER_REF,
        O.ORDER_ID,
        O.order_status,
        SUM(OL.ORDER_LINE_AMOUNT) AS ORDER_LINE_TOTAL,
        RANK() OVER (ORDER BY SUM(OL.ORDER_LINE_AMOUNT) DESC) AS rank
    FROM
        BCM_ORDER O
        INNER JOIN BCM_ORDER_LINE OL ON OL.ORDER_ID = O.ORDER_ID
        INNER JOIN BCM_INVOICE I ON I.ORDER_ID = O.ORDER_ID
    WHERE OL.ORDER_LINE_AMOUNT IS NOT NULL
    GROUP BY
        O.ORDER_REF, O.ORDER_DATE, O.SUPPLIER_REF, O.ORDER_ID, O.order_status) O
INNER JOIN BCM_SUPPLIER S ON S.SUPPLIER_REF = O.SUPPLIER_REF
INNER JOIN BCM_ORDER_LINE OL ON OL.ORDER_ID = O.ORDER_ID
INNER JOIN BCM_INVOICE I ON I.ORDER_ID = O.ORDER_ID
WHERE
    O.rank = 2 AND OL.ORDER_LINE_AMOUNT IS NOT NULL
GROUP BY
    O.ORDER_REF, O.ORDER_DATE, S.SUPPLIER_NAME, O.order_status;


    RETURN order_cursor;
END;
/

-- ====================


DECLARE
    order_cursor SYS_REFCURSOR;
    order_reference VARCHAR2(100);
    order_period VARCHAR2(20);
    supplier_name VARCHAR2(100);
    order_total VARCHAR2(20);
    order_status VARCHAR2(50);
    invoice_references VARCHAR2(4000); -- Adjust the size as needed
BEGIN
    order_cursor := GetSecondHighestOrderDetails;

    FETCH order_cursor INTO order_reference, order_period, supplier_name, order_total, order_status, invoice_references;
    
    IF order_cursor%FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Order Reference: ' || order_reference);
        DBMS_OUTPUT.PUT_LINE('Order Period: ' || order_period);
        DBMS_OUTPUT.PUT_LINE('Supplier Name: ' || supplier_name);
        DBMS_OUTPUT.PUT_LINE('Order Total Amount: ' || order_total);
        DBMS_OUTPUT.PUT_LINE('Order Status: ' || order_status);
        DBMS_OUTPUT.PUT_LINE('Invoice References: ' || invoice_references);
    ELSE
        DBMS_OUTPUT.PUT_LINE('No records found.');
    END IF;

    CLOSE order_cursor;
END;
/



-- QUESTION 6



CREATE OR REPLACE PROCEDURE GET_SUPPLIER_ORDER_SUMMARY AS
BEGIN
    FOR result IN (
        SELECT
            SUP.SUPPLIER_NAME AS "Supplier Name",
            COUNT(O.ORDER_ID) AS "Number of Orders",
            TO_CHAR(SUM(OL.ORDER_LINE_AMOUNT), 'FM999,999,990.00') AS "Total Amount Ordered"
        FROM
            BCM_ORDER O
            INNER JOIN BCM_SUPPLIER SUP ON SUP.SUPPLIER_REF = O.SUPPLIER_REF
            INNER JOIN BCM_ORDER_LINE OL ON O.ORDER_ID = OL.ORDER_ID
        WHERE
            O.ORDER_DATE BETWEEN TO_DATE('2022-01-01', 'YYYY-MM-DD') AND TO_DATE('2022-08-31', 'YYYY-MM-DD')
        GROUP BY
            SUP.SUPPLIER_NAME
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Supplier Name: ' || result."Supplier Name");
        DBMS_OUTPUT.PUT_LINE('Number of Orders: ' || result."Number of Orders");
        DBMS_OUTPUT.PUT_LINE('Total Amount Ordered: ' || result."Total Amount Ordered");
        DBMS_OUTPUT.PUT_LINE('-----------------------------------');
    END LOOP;
END;
/


BEGIN
    GET_SUPPLIER_ORDER_SUMMARY;
END;
/

￼

