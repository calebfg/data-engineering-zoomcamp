#!/bin/bash

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

# Green taxi 2020 and 2021
for YEAR in 2020 2021; do
    for MONTH in $(seq -w 1 12); do
        URL="${BASE_URL}/green_tripdata_${YEAR}-${MONTH}.parquet"
        LOCAL_PATH="data/pq/green/${YEAR}/${MONTH}"
        mkdir -p ${LOCAL_PATH}
        wget -q ${URL} -O ${LOCAL_PATH}/green_tripdata_${YEAR}-${MONTH}.parquet
        echo "Downloaded green ${YEAR}-${MONTH}"
    done
done

# Yellow taxi 2020 and 2021
for YEAR in 2020 2021; do
    for MONTH in $(seq -w 1 12); do
        URL="${BASE_URL}/yellow_tripdata_${YEAR}-${MONTH}.parquet"
        LOCAL_PATH="data/pq/yellow/${YEAR}/${MONTH}"
        mkdir -p ${LOCAL_PATH}
        wget -q ${URL} -O ${LOCAL_PATH}/yellow_tripdata_${YEAR}-${MONTH}.parquet
        echo "Downloaded yellow ${YEAR}-${MONTH}"
    done
done

echo "All done!"