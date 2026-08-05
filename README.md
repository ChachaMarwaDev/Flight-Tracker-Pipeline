# Flight-Tracker-Pipeline
Flight tracker pipeline that polls the OpenSky Network API for live flight data, lands raw JSON in a bronze layer, and uses an Airflow DAG to transform it into clean flight position and per-airline stat tables — built with failure-tolerant retry/rate-limit handling. Stack: Python, Airflow, PostgreSQL, Docker.

commands to remember
`docker logs juypter container`