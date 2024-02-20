{{config(materialized="table")}}

select 
    cast(locationid as STRING) as locationid,
    cast(borough as STRING) as borough,
    cast(zone as STRING) as zone,
    cast(replace(service_zone, 'Boro', 'Green') as STRING) as service_zone
from {{ref('taxi_zone_lookup')}}
