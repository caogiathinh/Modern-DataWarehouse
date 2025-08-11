/*
============================================================
Tạo Cơ sở dữ liệu và Lược đồ
============================================================

Mục đích của Script:
	Script này tạo một cơ sở dữ liệu mới tên là 'Datawarehouse' sau khi kiểm tra xem nó đã tồn tại hay chưa.
	Nếu cơ sở dữ liệu đã tồn tại, nó sẽ bị xóa và tạo lại. Ngoài ra, script sẽ thiết lập ba lược đồ
	trong cơ sở dữ liệu: 'bronze', 'silver', và 'gold'.

CẢNH BÁO:
	Việc chạy script này sẽ xóa toàn bộ cơ sở dữ liệu 'Datawarehouse' nếu nó đã tồn tại.
	Tất cả dữ liệu trong cơ sở dữ liệu sẽ bị xóa vĩnh viễn. Hãy tiến hành một cách thận trọng
	và đảm bảo bạn đã có các bản sao lưu phù hợp trước khi chạy script này.
*/

USE master
GO

-- Xóa cơ sở dữ liệu DataWarehouse nếu đã tồn tại
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END

-- Tạo cơ sở dữ liệu Dataarehouse
CREATE DATABASE DataWarehouse; 

USE DataWarehouse
GO

CREATE SCHEMA bronze; 
GO

CREATE SCHEMA silver; 
GO

CREATE SCHEMA gold; 
GO
