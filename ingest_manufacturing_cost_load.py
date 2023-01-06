from airflow.models.dag import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime

dag = DAG("ingest_manufacturing_cost_load",schedule_interval=None, start_date=datetime(2023, 1, 1))


start_raw_table_loads = DummyOperator(
    task_id="start_raw_table_loads",
    dag=dag
)

end_raw_table_loads = DummyOperator(
    task_id="end_raw_table_loads",
    dag=dag
)


load_base_data = GCSToBigQueryOperator(
    task_id="load_base_data",
    dag=dag,
    bucket="braided-storm-373701-car-inbound-raw",
    source_objects=["base_data.csv"],
    destination_project_dataset_table="braided-storm-373701.car_inbound_raw.base_data",
    allow_quoted_newlines=True,
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    #schema_object="base_data.json",
    schema_fields=[
      {
        "mode": "nullable",
        "name": "vin",
        "type": "string"
      },
      {
        "mode": "nullable",
        "name": "option_quantities",
        "type": "integer"
      },
      {
        "mode": "nullable",
        "name": "options_code",
        "type": "string"
      },
      {
        "mode": "nullable",
        "name": "option_desc",
        "type": "string"
      },
      {
        "mode": "nullable",
        "name": "model_text",
        "type": "string"
      },
      {
        "mode": "nullable",
        "name": "sales_price",
        "type": "numeric"
      }
    ],
    autodetect=False
)

load_option_data = GCSToBigQueryOperator(
    task_id="load_option_data",
    dag=dag,
    bucket="braided-storm-373701-car-inbound-raw",
    source_objects=["options_data.csv"],
    destination_project_dataset_table="braided-storm-373701.car_inbound_raw.options_data",
    allow_quoted_newlines=True,
    skip_leading_rows=1,
    # schema_object="options_data.json",
    schema_fields=[
      {
        "mode": "nullable",
        "name": "model",
        "type": "string"
      },
      {
        "mode": "nullable",
        "name": "option_code",
        "type": "string"
      },
      {
        "mode": "nullable",
        "name": "option_desc",
        "type": "string"
      },
      {
        "mode": "nullable",
        "name": "material_cost",
        "type": "numeric"
      }
    ]
    ,
    write_disposition='WRITE_TRUNCATE',
    autodetect=False
)


vehicle_line_mapping = GCSToBigQueryOperator(
    task_id="vehicle_line_mapping",
    dag=dag,
    bucket="braided-storm-373701-car-inbound-raw",
    source_objects=["vehicle_line_mapping.csv"],
    destination_project_dataset_table="braided-storm-373701.car_inbound_raw.vehicle_line_mapping",
    allow_quoted_newlines=True,
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    # schema_object="vehicle_line_mapping.json",
    schema_fields=[
    {
      "mode": "nullable",
      "name": "nameplate_code",
      "type": "string"
    },
    {
      "mode": "nullable",
      "name": "brand",
      "type": "string"
    },
    {
      "mode": "nullable",
      "name": "platform",
      "type": "string"
    },
    {
      "mode": "nullable",
      "name": "nameplate_display",
      "type": "string"
    }
  ]
  ,
    autodetect=False
)

enrich_data_load = BigQueryExecuteQueryOperator(
    task_id = "enrich_data_load",
    use_legacy_sql=False,
    sql="""
    CREATE TABLE IF NOT EXISTS `braided-storm-373701.car_inbound_raw.base_data_enriched`
    (
      vehicle_identification_number STRING,
      model_id STRING,
      model_short_text STRING,
      model_brand STRING,
      model_platform STRING,
      nameplate_display STRING,
      option_id STRING,
      option_desc STRING,
      option_quantities INT64,
      option_sales_price NUMERIC,
      option_production_cost NUMERIC,
      profit_amount NUMERIC,
      is_profitable_option BOOL,
      created_ts TIMESTAMP,
      updated_ts TIMESTAMP 
    );

    merge `braided-storm-373701.car_inbound_raw.base_data_enriched` t using (select 
    vehicle_identification_number,
    model_id,
    model_short_text,
    model_brand,
    model_platform,
    nameplate_display,
    option_id,
    option_desc,
    option_quantities,
    option_sales_price,
    option_production_cost,
    option_sales_price-option_production_cost as profit_amount,
    case when option_sales_price-option_production_cost > 0 then True else False end as is_profitable_option
    from (
    select 
    base.vin as vehicle_identification_number,
    base.model_id,
    base.model_text as model_short_text,
    vinfo.brand as model_brand,
    vinfo.platform as model_platform,
    vinfo.nameplate_display,
    base.option_id,
    base.option_desc,
    base.option_quantities,
    cast(base.sales_price as numeric) as option_sales_price,
    case when base.sales_price <= 0 then 0 
    when base.sales_price > 0 then
    cast(round(coalesce(coalesce(
    mod_opt.material_cost,
    opt.material_cost),sales_price*0.45),2) as numeric) else 0 end as option_production_cost
    from (
    select vin,
    sum(option_quantities) as option_quantities,
    trim(options_code) as option_id,
    option_desc,
    model_text,
    sum(sales_price) as sales_price, trim(split(model_text," ")[safe_offset(0)]) as model_id from `braided-storm-373701.car_inbound_raw.base_data`
    group by 1,3,4,5,7
    ) base left outer join (
      SELECT
      TRIM(model) AS model_id,
      TRIM(option_code) AS option_id,
      round(avg(material_cost)) as material_cost,
    FROM
      `braided-storm-373701.car_inbound_raw.options_data`
      group by model_id, option_id
    ) mod_opt
    on base.model_id = mod_opt.model_id
    and   base.option_id = mod_opt.option_id
    left outer join (
    SELECT
      TRIM(option_code) AS option_id,
      round(avg(material_cost),2) as material_cost,
    FROM
      `braided-storm-373701.car_inbound_raw.options_data`
      group by option_id
    ) opt on   base.option_id = opt.option_id
    left outer join `braided-storm-373701.car_inbound_raw.vehicle_line_mapping` vinfo
    on base.model_id = vinfo.nameplate_code
    ) where 
    vehicle_identification_number is not null) s on 
    coalesce(t.vehicle_identification_number,"") = coalesce(s.vehicle_identification_number,"")
    and  coalesce(t.model_id,"") = coalesce(s.model_id,"")
    and coalesce(t.option_id,"") = coalesce(s.option_id,"")
    and coalesce(t.model_short_text,"") = coalesce(s.model_short_text,"")
    when matched and 
    (
    coalesce(cast(t.model_brand as string),"") <> coalesce(cast(s.model_brand as string),"") or  
    coalesce(cast(t.model_platform as string),"") <> coalesce(cast(s.model_platform as string),"") or  
    coalesce(cast(t.nameplate_display as string),"") <> coalesce(cast(s.nameplate_display as string),"") or  
    coalesce(cast(t.option_desc as string),"") <> coalesce(cast(s.option_desc as string),"") or  
    coalesce(cast(t.option_quantities as string),"") <> coalesce(cast(s.option_quantities as string),"") or  
    coalesce(cast(t.option_sales_price as string),"") <> coalesce(cast(s.option_sales_price as string),"") or  
    coalesce(cast(t.option_production_cost as string),"") <> coalesce(cast(s.option_production_cost as string),"") or  
    coalesce(cast(t.profit_amount as string),"") <> coalesce(cast(s.profit_amount as string),"") or  
    coalesce(cast(t.is_profitable_option as string),"") <> coalesce(cast(s.is_profitable_option as string),"")
    )
    then update 
    set t.model_brand = s.model_brand ,
    t.model_platform = s.model_platform ,
    t.nameplate_display = s.nameplate_display ,
    t.option_desc = s.option_desc ,
    t.option_quantities = s.option_quantities ,
    t.option_sales_price = s.option_sales_price ,
    t.option_production_cost = s.option_production_cost ,
    t.profit_amount = s.profit_amount ,
    t.is_profitable_option = s.is_profitable_option ,
    t.updated_ts = current_timestamp()
    when not matched then 
    insert 
    values(
    s.vehicle_identification_number,
    s.model_id,
    s.model_short_text,
    s.model_brand,
    s.model_platform,
    s.nameplate_display,
    s.option_id,
    s.option_desc,
    s.option_quantities,
    s.option_sales_price,
    s.option_production_cost,
    s.profit_amount,
    s.is_profitable_option,
    current_timestamp(),
    current_timestamp()
    );

"""
)


start_raw_table_loads >> load_base_data
start_raw_table_loads >> load_option_data
start_raw_table_loads >> vehicle_line_mapping


enrich_data_load << load_base_data
enrich_data_load << load_option_data
enrich_data_load << vehicle_line_mapping


enrich_data_load >> end_raw_table_loads