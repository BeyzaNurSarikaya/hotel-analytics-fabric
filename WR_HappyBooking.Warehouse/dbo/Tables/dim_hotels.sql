CREATE TABLE [dbo].[dim_hotels] (

	[hotel_id] varchar(8000) NULL, 
	[hotel_name] varchar(8000) NULL, 
	[hotel_type] varchar(8000) NULL, 
	[star_rating] int NULL, 
	[hotel_facilities] varchar(8000) NULL, 
	[nearby_attractions] varchar(8000) NULL, 
	[hotel_description] varchar(8000) NULL, 
	[country_name] varchar(8000) NULL, 
	[city] varchar(8000) NULL, 
	[address] varchar(8000) NULL, 
	[postal_code] varchar(8000) NULL, 
	[latitude] varchar(8000) NULL, 
	[longitude] varchar(8000) NULL
);