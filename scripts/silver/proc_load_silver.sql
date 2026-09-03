/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process
    to populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Cleans and transforms Bronze data.
    - Inserts transformed data into Silver tables.
    - Handles NULL and invalid values.
    - Removes duplicate customer records.
    - Calculates product end dates using LEAD().
    - Normalizes category, product line, gender, marital status and countries.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';


        /*
        ================================================================
        CRM TABLES
        ================================================================
        */

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';


        /*
        ================================================================
        1. Loading silver.crm_cust_info
        ================================================================
        */

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.crm_cust_info';

        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info
        (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )

        SELECT
            cst_id,
            cst_key,

            TRIM(cst_firstname) AS cst_firstname,

            TRIM(cst_lastname) AS cst_lastname,

            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S'
                    THEN 'Single'

                WHEN UPPER(TRIM(cst_marital_status)) = 'M'
                    THEN 'Married'

                ELSE 'n/a'
            END AS cst_marital_status,

            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F'
                    THEN 'Female'

                WHEN UPPER(TRIM(cst_gndr)) = 'M'
                    THEN 'Male'

                ELSE 'n/a'
            END AS cst_gndr,

            cst_create_date

        FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER
                (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
                ) AS flag_last

            FROM bronze.crm_cust_info

            WHERE cst_id IS NOT NULL

        ) t

        WHERE flag_last = 1;


        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '>> -------------';


        /*
        ================================================================
        2. Loading silver.crm_prd_info
        ================================================================
        */

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.crm_prd_info';

        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';


        /*
        Product transformation is first placed into a derived table.

        prd_key_original:
            Keeps the original product key for extracting cat_id
            and calculating product versions.

        prd_start_date:
            Converts the product start date to DATE.

        cat_id:
            Example:
                AC-HE -> AC_HE
        */

        INSERT INTO silver.crm_prd_info
        (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )

        SELECT
            p.prd_id,

            /*
            ============================================================
            CATEGORY ID
            ============================================================

            Example:
                AC-HE-...  -> AC_HE

            TRIM removes accidental spaces.
            LEFT(..., 5) takes the first 5 characters.
            REPLACE changes '-' to '_'.
            */

            CASE
                WHEN p.prd_key_original IS NULL
                     OR TRIM(p.prd_key_original) = ''
                    THEN 'n/a'

                ELSE
                    REPLACE(
                        LEFT(
                            TRIM(p.prd_key_original),
                            5
                        ),
                        '-',
                        '_'
                    )
            END AS cat_id,

            /*
            Product key

            Example:
                AC-HE-HL-U509-R

            Result:
                HL-U509-R
            */

            CASE
                WHEN p.prd_key_original IS NULL
                    THEN NULL

                ELSE
                    SUBSTRING(
                        TRIM(p.prd_key_original),
                        7,
                        LEN(TRIM(p.prd_key_original))
                    )
            END AS prd_key,

            TRIM(p.prd_nm) AS prd_nm,

            ISNULL(p.prd_cost, 0) AS prd_cost,

            /*
            Product line normalization
            */

            CASE

                WHEN UPPER(TRIM(p.prd_line)) = 'M'
                    THEN 'Mountain'

                WHEN UPPER(TRIM(p.prd_line)) = 'R'
                    THEN 'Road'

                WHEN UPPER(TRIM(p.prd_line)) = 'S'
                    THEN 'Other Sales'

                WHEN UPPER(TRIM(p.prd_line)) = 'T'
                    THEN 'Touring'

                ELSE 'n/a'

            END AS prd_line,

            p.prd_start_date AS prd_start_dt,

            /*
            ============================================================
            PRODUCT END DATE

            The next product version starts on LEAD(prd_start_date).

            Therefore:

                next start date - 1 day = current end date

            For the latest/current version:

                LEAD() = NULL
                therefore prd_end_dt = NULL

            This is intentional.
            ============================================================
            */

            DATEADD
            (
                DAY,
                -1,

                LEAD(p.prd_start_date) OVER
                (
                    PARTITION BY p.prd_key_original
                    ORDER BY p.prd_start_date
                )
            ) AS prd_end_dt

        FROM
        (
            SELECT
                prd_id,

                prd_key AS prd_key_original,

                prd_nm,

                prd_cost,

                prd_line,

                CAST(prd_start_dt AS DATE) AS prd_start_date

            FROM bronze.crm_prd_info

        ) p;


        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '>> -------------';


        /*
        ================================================================
        3. Loading silver.crm_sales_details
        ================================================================
        */

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.crm_sales_details';

        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';


        INSERT INTO silver.crm_sales_details
        (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )

        SELECT

            sls_ord_num,

            sls_prd_key,

            sls_cust_id,

            /*
            Order Date
            */

            CASE
                WHEN sls_order_dt = 0
                     OR LEN(sls_order_dt) <> 8
                    THEN NULL

                ELSE
                    TRY_CONVERT(
                        DATE,
                        CAST(sls_order_dt AS VARCHAR(8)),
                        112
                    )
            END AS sls_order_dt,

            /*
            Ship Date
            */

            CASE
                WHEN sls_ship_dt = 0
                     OR LEN(sls_ship_dt) <> 8
                    THEN NULL

                ELSE
                    TRY_CONVERT(
                        DATE,
                        CAST(sls_ship_dt AS VARCHAR(8)),
                        112
                    )
            END AS sls_ship_dt,

            /*
            Due Date
            */

            CASE
                WHEN sls_due_dt = 0
                     OR LEN(sls_due_dt) <> 8
                    THEN NULL

                ELSE
                    TRY_CONVERT(
                        DATE,
                        CAST(sls_due_dt AS VARCHAR(8)),
                        112
                    )
            END AS sls_due_dt,

            /*
            Sales

            If sales is missing, zero, negative or incorrect,
            calculate:

                quantity * absolute price
            */

            CASE
                WHEN sls_sales IS NULL
                     OR sls_sales <= 0
                     OR sls_sales <> sls_quantity * ABS(sls_price)

                    THEN sls_quantity * ABS(sls_price)

                ELSE sls_sales

            END AS sls_sales,

            sls_quantity,

            /*
            Price

            If price is missing or invalid,
            calculate:

                sales / quantity
            */

            CASE
                WHEN sls_price IS NULL
                     OR sls_price <= 0

                    THEN sls_sales / NULLIF(sls_quantity, 0)

                ELSE sls_price

            END AS sls_price

        FROM bronze.crm_sales_details;


        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '>> -------------';


        /*
        ================================================================
        ERP TABLES
        ================================================================
        */

        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';


        /*
        ================================================================
        4. Loading silver.erp_cust_az12
        ================================================================
        */

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.erp_cust_az12';

        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';


        INSERT INTO silver.erp_cust_az12
        (
            cid,
            bdate,
            gen
        )

        SELECT

            CASE
                WHEN cid LIKE 'NAS%'
                    THEN SUBSTRING(
                        cid,
                        4,
                        LEN(cid)
                    )

                ELSE cid
            END AS cid,

            CASE
                WHEN bdate > GETDATE()
                    THEN NULL

                ELSE bdate
            END AS bdate,

            CASE

                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE')
                    THEN 'Female'

                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')
                    THEN 'Male'

                ELSE 'n/a'

            END AS gen

        FROM bronze.erp_cust_az12;


        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '>> -------------';


        /*
        ================================================================
        5. Loading silver.erp_loc_a101
        ================================================================
        */

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.erp_loc_a101';

        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';


        INSERT INTO silver.erp_loc_a101
        (
            cid,
            cntry
        )

        SELECT

            REPLACE(
                TRIM(cid),
                '-',
                ''
            ) AS cid,

            CASE

                WHEN TRIM(cntry) = 'DE'
                    THEN 'Germany'

                WHEN TRIM(cntry) IN ('US', 'USA')
                    THEN 'United States'

                WHEN TRIM(cntry) = ''
                     OR cntry IS NULL
                    THEN 'n/a'

                ELSE TRIM(cntry)

            END AS cntry

        FROM bronze.erp_loc_a101;


        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '>> -------------';


        /*
        ================================================================
        6. Loading silver.erp_px_cat_g1v2
        ================================================================
        */

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';


        INSERT INTO silver.erp_px_cat_g1v2
        (
            id,
            cat,
            subcat,
            maintenance
        )

        SELECT

            id,
            cat,
            subcat,
            maintenance

        FROM bronze.erp_px_cat_g1v2;


        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '>> -------------';


        /*
        ================================================================
        COMPLETED
        ================================================================
        */

        SET @batch_end_time = GETDATE();

        PRINT '==========================================';

        PRINT 'Loading Silver Layer is Completed';

        PRINT '   - Total Load Duration: '
            + CAST(
                DATEDIFF(
                    SECOND,
                    @batch_start_time,
                    @batch_end_time
                ) AS NVARCHAR
            )
            + ' seconds';

        PRINT '==========================================';


    END TRY

    BEGIN CATCH

        PRINT '==========================================';

        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';

        PRINT 'Error Message: '
            + ERROR_MESSAGE();

        PRINT 'Error Number: '
            + CAST(ERROR_NUMBER() AS NVARCHAR);

        PRINT 'Error State: '
            + CAST(ERROR_STATE() AS NVARCHAR);

        PRINT '==========================================';

        /*
        Important:
        THROW returns the actual SQL error instead of silently
        allowing the procedure to appear successful.
        */

        THROW;

    END CATCH

END;
GO
