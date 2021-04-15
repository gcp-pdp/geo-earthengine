#!/bin/sh
# WGS 84

CREDENTIALS="/root/gee-export.json"
COLLECTION="projects/earthengine-public/assets/WorldPop/GP/100m/pop"
YEAR=2020

export "GOOGLE_APPLICATION_CREDENTIALS=$CREDENTIALS"

IMAGES=$(ogrinfo -ro -al "EEDA:" -oo "COLLECTION=$COLLECTION" -where "year=$YEAR" | grep 'gdal_dataset (String) = ' | cut -d '=' -f2 | tr -d ' ')
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
