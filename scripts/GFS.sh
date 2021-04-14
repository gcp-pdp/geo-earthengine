#!/bin/sh
# https://developers.google.com/earth-engine/datasets/catalog/NOAA_GFS0P25
# https://gis.stackexchange.com/questions/335566/understanding-noaa-gfs0p25-dataset-of-google-earth-engine
# WGS 84

CREDENTIALS="/root/gee-export.json"
CREDENTIALS="/Users/mbg/gee-export.json"
COLLECTION="projects/earthengine-public/assets/NOAA/GFS0P25"
DATE="2021/04/13"
INTERVAL=384

export "GOOGLE_APPLICATION_CREDENTIALS=$CREDENTIALS"

IMAGES=$(ogrinfo -ro -al "EEDA:" -oo "COLLECTION=$COLLECTION" -where "startTime>='$DATE' and forecast_hours=$INTERVAL" \
    | grep 'gdal_dataset (String) = ' | cut -d '=' -f2 | tr -d ' ')
for IMAGE in $IMAGES
do
    NAME=$(basename "$IMAGE")
    if [ -f "${NAME}.tif" ]
    then
        echo "Image already downloaded: ${NAME}.tif"
    else
        echo "Fetching Image: $IMAGE -> ${NAME}"
        gdal_translate --config GDAL_CACHEMAX 30000 --config GDAL_HTTP_RETRY_DELAY 600 -oo BLOCK_SIZE=2000 "$IMAGE" "${NAME}.tif"
    fi
done
