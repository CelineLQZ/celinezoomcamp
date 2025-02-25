{% macro get_year_from_pickup_datetime(pickup_datetime) -%}
    case
        when EXTRACT(YEAR FROM {{ pickup_datetime }}) = 2019 then 2019
        when EXTRACT(YEAR FROM {{ pickup_datetime }}) = 2020 then 2020
        else 'UNKNOWN'
    end
{%- endmacro %}
