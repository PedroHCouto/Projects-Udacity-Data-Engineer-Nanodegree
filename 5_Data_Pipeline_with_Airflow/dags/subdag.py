import datetime

from airflow import DAG

from operators.create_tables import CreateTablesOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator


def create_load_quality(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        source_schema,
        target_schema,
        table,
        append_mode,
        primary_key, 
        check_quality_queries,
        failure_results,
        *args, **kwargs):

    dag = DAG(
        f'{parent_dag_name}.{task_id}',
        **kwargs)


    create_table_task = CreateTablesOperator(
        task_id = f'Create_{table}_table',
        dag = dag,
        redshift_conn_id = redshift_conn_id,
        target_schema = target_schema,
        table = table)

    load_dimension_task = LoadDimensionOperator(
        task_id = f'Load_dimension_data_into_{table}_table',
        dag = dag,
        redshit_conn_id = redshift_conn_id,
        source_schema = source_schema,
        table = table,
        append_mode = append_mode,
        primary_key = primary_key)

    quality_check_task = DataQualityOperator(
        task_id = f'Check_the_data_quality_of_{table}',
        dag = dag,
        redshift_conn_id = redshift_conn_id,
        target_schema = target_schema,
        table = table,
        check_quality_queries = check_quality_queries,
        failure_results = failure_results,
    )

    create_table_task >> load_dimension_task >> quality_check_task

    return dag