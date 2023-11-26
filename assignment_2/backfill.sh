airflow dags backfill -y \
    --reset-dagruns \
    -s 2023-10-27   \
    -e 2023-10-29   \
    daily_portfolio_dag
