/********************************************************************************************
 Script Name : Create DataWarehouse Database with Layered Schemas
 Author      : Mohamed Ghanaym
 Date        : [2025-09-27]

 Description : 
   This script drops the existing 'DataWarehouse' database (if it exists) and recreates it. 
   It also sets up a layered schema structure (bronze, silver, gold) for data warehousing.

 WARNING:
   - This script is DESTRUCTIVE. Running it will:
       * Force the 'DataWarehouse' database into SINGLE_USER mode.
       * Rollback and terminate existing connections.
       * DROP the database and all existing objects permanently.
   - Make sure to BACK UP your database before running this script in any environment.

 Steps:
   1. Drop existing 'DataWarehouse' if it exists (force single-user mode).
   2. Recreate the 'DataWarehouse' database.
   3. Create the following schemas:
        - bronze : Raw/ingested data layer.
        - silver : Cleansed/standardized data layer.
        - gold   : Curated/business-ready data layer.
********************************************************************************************/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END
GO

-- Create Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

