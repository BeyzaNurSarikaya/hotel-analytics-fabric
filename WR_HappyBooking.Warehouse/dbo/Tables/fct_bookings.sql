CREATE TABLE [dbo].[fct_bookings] (

	[booking_id] varchar(8000) NULL, 
	[customer_id] varchar(8000) NULL, 
	[hotel_id] varchar(8000) NULL, 
	[total_price_usd] float NULL, 
	[room_price] float NULL, 
	[tax_amount] float NULL, 
	[service_fee] float NULL, 
	[discount_amount] float NULL, 
	[checkin_date] date NULL, 
	[checkout_date] date NULL, 
	[nights] varchar(8000) NULL, 
	[booking_date] date NULL, 
	[full_name] varchar(8000) NULL, 
	[hotel_name] varchar(8000) NULL, 
	[country_name] varchar(8000) NULL, 
	[booking_status] varchar(8000) NULL, 
	[temp_max] float NULL, 
	[is_rainy] bit NULL, 
	[price_category] varchar(8000) NULL, 
	[star_rating] int NULL
);