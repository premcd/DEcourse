Week 2 of Data Engineering Zoomcamp by @DataTalksClub and @Will Russell complete!

Just finished Module 2 - Workflow Orchestration with @Kestra. Learned how to:

✅ Orchestrate data pipelines with Kestra flows

✅ Use variables and expressions for dynamic workflows

✅ Implement backfill for historical data

✅ Schedule workflows with timezone support

✅ Process NYC taxi data (Yellow & Green) for 2019-2021


Quiz Questions

Complete the quiz shown below. It's a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra, and ETL pipelines.


    Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?

```
   run the 04_postgres_taxi.yaml pipeline in Kestra with year 2020/month 12, go to execution/output -> select the file
```

```
    Answer is 128.3 MiB
    134.5 MiB
    364.7 MiB
    692.6 MiB
```


    What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?

    {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
    green_tripdata_2020-04.csv
    Answer is: green_tripdata_04_2020.csv
    green_tripdata_2020.csv

```
Use https://kestra.io/docs/how-to-guides/google-credentials to setup the secret key to connect to GCP.
```

    How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
    > Launch the GCP flow schedule (see attachment) with backfill for all month of year 2020 for the yellow taxis:
    13,537.299
    Answer is: 24,648,499
    18,324,219
    29,430,127

    How many rows are there for the Green Taxi data for all CSV files in the year 2020?
    > Launch the GCP flow schedule with backfill for all month of year 2020 for the green taxis:
    5,327,301
    936,199
    Answer is: 1,734,051
    1,342,034

    How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
    > Launch the GCP flow schedule with backfill for month 03 of year 2021 for the yellow taxis:
    1,428,092
    706,911
    Answer is: 1,925,152
    2,561,031

    How would you configure the timezone to New York in a Schedule trigger?

    Add a timezone property set to EST in the Schedule trigger configuration
    Answer: is Add a timezone property set to America/New_York in the Schedule trigger configuration
    Add a timezone property set to UTC-5 in the Schedule trigger configuration
    Add a location property set to New_York in the Schedule trigger configuration
