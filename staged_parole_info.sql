{{
  config(
    materialized = 'table'
  )
}}

with parole_data as (

    select * from {{ ref('source__parole_results') }}

)

, parole_conditions as (

    select * from {{ref('parole_conditions')}}

)

, most_recent_inmate_records as (

    select
        ais
        ,inmate_name
        ,birth_year
        ,race
        ,sex
        ,height
        ,weight
        ,hair_color
        ,eye_color
        ,institution
        ,institution_type
        ,code
        ,release_date
        ,custody
        ,offense
        ,parole_status
        ,admit_date
        ,min_release_date
        ,parole_consideration_date
        ,total_term
        ,total_term_days
        ,time_served
        ,time_served_days
        ,good_time_received
        ,good_time_received_days
        ,good_time_revoked
        ,good_time_revoked_days
        ,last_scraped_at
        ,week_scraped
        ,is_missing_inmate_details
    from {{ref('stg__most_recent_inmate_records')}}

)

, parole_conditions_fanned as (
    
    select
        parole_conditions.code || ': ' || parole_conditions.special_conditions as special_conditions
        ,parole_data.result
        ,parole_data.parole_key
    from parole_data 
    left join parole_conditions on
      parole_data.result like '%' || parole_conditions.code || '%'

)

, parole_conditions_collapsed as (

    select 
	    parole_conditions_fanned.parole_key
	    ,string_agg(parole_conditions_fanned.special_conditions, ' ') as special_conditions
    from parole_conditions_fanned
    group by parole_key

)


, combined as (

    select
        parole_data.parole_key
        ,parole_data.ais
        ,parole_data.scraped_at
        ,parole_data.result
        ,parole_data.hearing_date 
        ,parole_data._fivetran_batch
        ,parole_data._fivetran_index
        -------
        ,parole_conditions_collapsed.special_conditions
        -------
        ,most_recent_inmate_records.inmate_name
        ,most_recent_inmate_records.birth_year
        ,most_recent_inmate_records.race
        ,most_recent_inmate_records.sex
        ,most_recent_inmate_records.height
        ,most_recent_inmate_records.weight
        ,most_recent_inmate_records.hair_color
        ,most_recent_inmate_records.eye_color
        ,most_recent_inmate_records.institution
        ,most_recent_inmate_records.institution_type
        ,most_recent_inmate_records.code
        ,most_recent_inmate_records.release_date
        ,most_recent_inmate_records.custody
        ,most_recent_inmate_records.offense
        ,most_recent_inmate_records.parole_status
        ,most_recent_inmate_records.admit_date
        ,most_recent_inmate_records.min_release_date
        ,most_recent_inmate_records.parole_consideration_date
        ,most_recent_inmate_records.total_term
        ,most_recent_inmate_records.total_term_days
        ,most_recent_inmate_records.time_served
        ,most_recent_inmate_records.time_served_days
        ,most_recent_inmate_records.good_time_received
        ,most_recent_inmate_records.good_time_received_days
        ,most_recent_inmate_records.good_time_revoked
        ,most_recent_inmate_records.good_time_revoked_days
        ,most_recent_inmate_records.last_scraped_at
        ,most_recent_inmate_records.week_scraped
        ,most_recent_inmate_records.is_missing_inmate_details

        from parole_data
        left join parole_conditions_collapsed on
        parole_data.parole_key = parole_conditions_collapsed.parole_key
        left join most_recent_inmate_records on
        parole_data.ais = right(most_recent_inmate_records.ais, 6)

)

select * from combined
