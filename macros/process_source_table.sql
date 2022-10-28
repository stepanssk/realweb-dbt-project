{%- macro process_source_table(table_name) -%}

    {#- создаем переменные для отслеживания наличия столбцов -#}
    {%- set ns = namespace(has_adset=false, has_installs=false) -%}

    {#- получаем список столбов таблицы -#}
    {%- set columns = dbt_utils.get_filtered_columns_in_relation(from=ref('stg_' + table_name)) -%}
    
    {#- проверяем наличие adset_name и installs -#}
    {%- for column in columns -%}
        {%- if column == 'adset_name' -%}
            {%- set ns.has_adset = true -%}
        {%- elif column == 'installs' -%}
            {%- set ns.has_installs = true -%}
        {%- endif -%}
    {%- endfor -%}

{#- заменим отсутствующие столбцы значением '-' для adset и 'null' для installs #}
{{ table_name }} AS (
    SELECT
        date,
        campaign_name,
        {%- if ns.has_adset %}
        adset_name,
        {%- else %}
        '-' as adset_name,
        {%- endif %}
        platform,
        '{{table_name}}' AS source,
        clicks,
        costs,
        {%- if ns.has_installs %}
        installs,
        {%- else %}
        NULL as installs,
        {%- endif %}
        impressions
    FROM {{ ref('stg_' + table_name) }}
    WHERE is_realweb AND NOT is_ret_campaign AND platform IN ('ios', 'android')
)

{%- endmacro -%}