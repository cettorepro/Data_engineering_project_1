create stage housingstage
url = "s3://melbourne-housing-c-ettore-pro"
credentials = (AWS_KEY_ID = '---' AWS_SECRET_KEY = '---')


create or replace table HOUSING(
Suburb varchar,
Address varchar,
rooms integer,
typeh varchar,
price float, 
methodh varchar,
seller varchar, 
dateh varchar,
distance float,
postcode varchar,
bedroom2 integer,
bathroom integer,
car integer,
landsize integer,
building_area float,
year_built varchar,
council_area varchar,
lattitude float,
longtitude float,
regionh varchar,
property_count integer
)

copy into raw.housing
from '@housingstage/melb_housing.csv'
FILE_FORMAT = (Type = 'csv' skip_header = 1 field_optionally_enclosed_by = '"')