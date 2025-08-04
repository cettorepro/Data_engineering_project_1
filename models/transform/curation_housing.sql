--{{config(materialized = 'table')}}

with curation_housing as (
    select row_number() over (order by h.lattitude,h.longtitude,h.seller,h.dateh) as idh,
    h.suburb,
    h.address,
    h.lattitude,
    h.longtitude,
    cast(h.postcode as integer) as postcode,
    h.regionh,
    h.building_area as building_area,
    h.council_area,
    h.property_count,
    h.distance,
    h.landsize,
    case 
    when h.typeh = 'br' THEN 'bedroom(s)'
    when h.typeh = 'h' then 'house'
    when h.typeh = 'u' then 'unit'
    when h.typeh = 't' then 'townhouse'
    when h.typeh = 'dev site' then 'development site'
    when h.typeh = 'o res' then 'other residential'
    END as typeh,
     case 
    when h.methodh = 'S' THEN 'property sold'
    when h.methodh = 'SP' then 'property sold prior'
    when h.methodh = 'PI' then 'property passed in'
    when h.methodh = 'PN' then 'sold prior not disclosed'
    when h.methodh = 'SN' then 'sold not disclosed'
    when h.methodh = 'NB' then 'no bid'
    when h.methodh = 'VB' then 'vendor bid'
    when h.methodh = 'W' then 'withdrawn prior to auction'
    when h.methodh = 'SA' then 'sold after auction'
    when h.methodh = 'SS' then 'sold after auction price not disclosed'
    when h.methodh = 'N/A' then 'price or highest bid not available'
    END as methodh,
    h.year_built,
    h.bathroom,
    h.bedroom2,
    h.rooms,
    h.car,
    h.seller,
    h.price,
    h.dateh,
    SPLIT_PART(h.dateh,'/',1) as dayh,
    SPLIT_PART(h.dateh,'/',2) as monthh,
    SPLIT_PART(h.dateh,'/',3) as yearh,
    from raw.housing as h 
    group by  h.suburb,
    h.address,
    h.lattitude,
    h.longtitude,
    cast(h.postcode as integer) ,
    h.regionh,
    h.building_area,
    h.council_area,
    h.property_count,
    h.distance,
    h.landsize,
    h.typeh,
    h.methodh,
    h.postcode,
    h.year_built,
    h.bathroom,
    h.bedroom2,
    h.rooms,
    h.car,
    h.seller,
    h.price,
    h.dateh)
select *
 from curation_housing