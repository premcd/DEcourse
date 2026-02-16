with source as (
    select * from {{ source('raw_data', 'fhv_tripdata_2019') }}
),

renamed as (
    select 
        --identifiers
            cast (dispatching_base_num as string) as dispatching_base_nb,
            cast (Affiliated_base_number as string) as affiliated_base_nb,
            cast (PUlocationID as string) as pickup_location_id,
            cast (DOlocationID as integer) as dropoff_location_id,
            cast (SR_Flag as string) as sr_flag,

       -- timestamps
            cast (pickup_datetime as  timestamp) as pickup_datetime,
            cast (dropOff_datetime as timestamp) as dropoff_datetime


    from source
    -- Filter out records with null dipatching_base_num (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed


