
Homework
Module 1 Homework: Docker & SQL

Question 1:
```
docker run --entrypoint=bash  python:3.13
pip --version
```
Answer:
25.3

Question 2:
```
docker compose up
```
connect to pgadmin on localhost:8080 enter user credentials
register server with hostname:port -> db:5432 or postgres:5432



Prepare the Data
To download the green taxi trips, create a docker container with a postgresql database:

```
docker run -it --rm \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="green_taxi" \
    -v green_taxi_postgres_data:/var/lib/postgresql \
    -p 5432:5432 \
    postgres:18
```

Question 3. Counting short trips
```
SELECT
*
FROM
green_taxi_data

WHERE 
lpep_pickup_datetime >= '2025-11-01' AND
lpep_pickup_datetime < '2025-12-01'AND
trip_distance <= 1
```

====> 8007


Question 4. Longest trip for each day
```
SELECT
*

FROM
green_taxi_data

WHERE 
trip_distance = (SELECT MAX(trip_distance) FROM green_taxi_data WHERE trip_distance <= 100)
```

=========>2025-11-14

Question 5. Biggest pickup zone

```
SELECT
zpu."Zone",
COUNT(1) AS "total_amount"

FROM
green_taxi_data g JOIN zones zpu
ON g."PULocationID" = zpu."LocationID"

WHERE 
g.lpep_pickup_datetime >= '2025-11-18 00:00:00' AND
g.lpep_pickup_datetime <=  '2025-11-19 00:00:00'

GROUP BY
zpu."Zone"

ORDER BY
"total_amount" DESC

LIMIT 100
```

Answer ======> East Harlem North



Question 6. Largest tip
```
SELECT
g."lpep_dropoff_datetime",
g.tip_amount,
dpo."Zone"

FROM
green_taxi_data g JOIN zones zpu
ON g."PULocationID" = zpu."LocationID"
JOIN zones dpo
ON g."DOLocationID" = dpo."LocationID"

WHERE 
lpep_pickup_datetime >= '2025-11-01' AND
lpep_pickup_datetime <  '2025-12-01' AND
zpu."Zone" = 'East Harlem North'

ORDER BY
g.tip_amount DESC
```

====> answer: Yorkville West


Question 7. Terraform Workflow
Answer: terraform init, terraform apply -auto-approve, terraform destroy
