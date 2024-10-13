{% snapshot snsh_novi_data %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='api10_hkey',
          check_cols=['novi_data_dhiff'],
        )
    }}

    select * from {{ ref('stg_novi_data') }}

{% endsnapshot %}