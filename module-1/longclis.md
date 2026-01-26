docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18

uv run python ingest_data.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips \
  --year=2021 \
  --month=12 \
  --chunksize=100000

  docker run -it --rm \
    --network=pipeline-default \
    --name ingest \
    taxi_ingest:v001\
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips \
    --year=2021 \
    --month=12 \
    --chunksize=100000

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4


sql request to manually join 2 tables:
  SELECT
tpep_pickup_datetime,
tpep_dropoff_datetime,
trip_distance,
CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
CONCAT(zdo."Borough", ' | ', zdo."Zone") AS "dropoff_loc"


FROM yellow_taxi_trips t,
zones zpu,
zones zdo

WHERE
   t."PULocationID" = zpu."LocationID" AND
   t."DOLocationID" = zdo."LocationID"



