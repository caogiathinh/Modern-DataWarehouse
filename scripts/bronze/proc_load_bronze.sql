/*=================================================================
Stored Procedure: Nạp dữ liệu Lớp Bronze (Nguồn -> Bronze)
===================================================================

Mục đích Script:
Stored procedure này nạp dữ liệu vào schema 'bronze' từ các file CSV bên ngoài.
Nó thực hiện các hành động sau:
- Xóa sạch (truncate) các bảng bronze trước khi nạp dữ liệu.
- Sử dụng lệnh 'BULK INSERT' để nạp dữ liệu từ các file CSV vào các bảng bronze.

Tham số:
Không có.
Stored procedure này không chấp nhận bất kỳ tham số nào hoặc trả về bất kỳ giá trị nào.

Ví dụ sử dụng:
EXEC bronze.load_bronze; 
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE(); 
		PRINT '============================================'
		PRINT 'Loading Bronze Layer'
		PRINT '============================================'

		PRINT '--------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------------'

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE [bronze].[crm_cust_info];
		PRINT '>> Inserting Data Into Table: bronze.crm_cust_info'
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'D:\Modern_DataWarehouse\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW  = 2,			--bỏ qua phần tiêu đề
			FIELDTERMINATOR = ',',  --Kí tự phân tách cột
			TABLOCK					--Khóa bảng để tăng hiệu suất
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: [bronze].[crm_prd_info]'
		TRUNCATE TABLE [bronze].[crm_prd_info];
		PRINT '>> Inserting Data Into Table: [bronze].[crm_prd_info]'
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'D:\Modern_DataWarehouse\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW  = 2,			--bỏ qua phần tiêu đề 
			FIELDTERMINATOR = ',',  --Kí tự phân tách cột
			TABLOCK					--Khóa bảng để tăng hiệu suất
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: [bronze].[crm_sales_details]'
		TRUNCATE TABLE [bronze].[crm_sales_details];
		PRINT '>> Inserting Data Into Table:  [bronze].[crm_sales_details]'
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'D:\Modern_DataWarehouse\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW  = 2,			--bỏ qua phần tiêu đề
			FIELDTERMINATOR = ',',	--Kí tự phân tách cột
			TABLOCK					--Khóa bảng để tăng hiệu suất
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------'

		PRINT '--------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '--------------------------------------------'

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: [bronze].[erp_cust_az12]' 
		TRUNCATE TABLE [bronze].[erp_cust_az12];
		PRINT '>> Inserting Data Into Table:  [bronze].[erp_cust_az12]'
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'D:\Modern_DataWarehouse\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW  = 2,			--bỏ qua phần tiêu đề 
			FIELDTERMINATOR = ',',  --Kí tự phân tách cột
			TABLOCK					--Khóa bảng để tăng hiệu suất
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------'

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: [bronze].[erp_px_cat_g1v2]' 
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
		PRINT '>> Inserting Data Into Table: [bronze].[erp_px_cat_g1v2]'
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'D:\Modern_DataWarehouse\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW  = 2,			--bỏ qua phần tiêu đề
			FIELDTERMINATOR = ',',  --Kí tự phân tách cột
			TABLOCK					--Khóa bảng để tăng hiệu suất
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------'

		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: [bronze].[erp_loc_a101]' 
		TRUNCATE TABLE [bronze].[erp_loc_a101];
		PRINT '>> Inserting Data Into Table: [bronze].[erp_loc_a101]'
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'D:\Modern_DataWarehouse\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW  = 2,			--bỏ qua phần tiêu đề
			FIELDTERMINATOR = ',',  --Kí tự phân tách cột
			TABLOCK					--Khóa bảng để tăng hiệu suất
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------'

		SET @batch_end_time = GETDATE(); 
		PRINT '============================================='
		PRINT 'Loading Bronze Layer Is Completed'
		PRINT '		- Total Load Duration: ' +  CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '============================================='

	END TRY
	BEGIN CATCH
		PRINT '============================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error Message ' + CAST(ERROR_STATE() AS NVARCHAR)
		PRINT '============================================='
	END CATCH
END
