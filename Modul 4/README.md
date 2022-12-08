# DBT Project
In this analytics engineering practice case, I used my previous work on dbt which includes using green taxi and yellow taxi trip data.

## DBT Test
DBT Test is done on several tables, here is some examples of the test that is applied.

- **Table**: stg_green_tripdata </br>
    - **Column**: tripid </br>
    - **Tests**: not null and unique </br>
    - **Explanation**: column trip id is like the primary key of the table, so it needs to always be unique and have not null values </br>

- **Table**: stg_yellow_tripdata </br>
    - **Column**: Pickup_locationid </br>
    - **Tests**: relationships               
        - to: ref('taxi_zone_lookup')
        - field: locationid </br>
    - **Explanation**: this test ensures that values in column Pickup_locationid does exist as locationid in model taxi_zone_lookup</br>

- **Table**: stg_yellow_tripdata </br>
    - **Column**: Payment_type </br>
    - **Tests**: accepted_values
        - values: {{ var('payment_type_values') }}
    - **Explanation**: it means that value of column payment_type should always be like variabel payment_type_values, which is [1,2,3,4,5,6] </br>

## DBT Docs
Here is a snippet of the dbt docs. You can find the full version in path `dbt/taxi_rides_ny/target/index.html`

![dbt docs 1](dbt-docs-1.jpg)
</br>
</br>
![dbt docs 2](dbt-docs-2.jpg)

