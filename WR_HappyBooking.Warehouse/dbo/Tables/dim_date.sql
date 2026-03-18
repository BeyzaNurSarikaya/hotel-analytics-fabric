CREATE TABLE [dbo].[dim_date] (

	[date_key] date NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[month_name] varchar(50) NULL, 
	[quarter] int NULL, 
	[week_of_year] int NULL, 
	[day_of_week_name] varchar(50) NULL, 
	[is_weekend] int NOT NULL, 
	[year_month] varchar(10) NULL
);