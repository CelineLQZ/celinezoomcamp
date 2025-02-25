{% macro get_quarter_from_pickup_datetime(pickup_datetime) -%}
    case
        when EXTRACT(MONTH FROM {{ pickup_datetime }}) between 1 and 3 then 'Q1'
        when EXTRACT(MONTH FROM {{ pickup_datetime }}) between 4 and 6 then 'Q2'
        when EXTRACT(MONTH FROM {{ pickup_datetime }}) between 7 and 9 then 'Q3'
        when EXTRACT(MONTH FROM {{ pickup_datetime }}) between 10 and 12 then 'Q4'
        else 'UNKNOWN'
    end
{%- endmacro %}
