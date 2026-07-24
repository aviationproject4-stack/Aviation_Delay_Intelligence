#!/bin/bash

hive -f db_creation.hive
hive -f airline_dim_ddl.hive
hive -f airport_dim.hive
hive -f delay_fact.hive
hive -f flight_fact.hive
