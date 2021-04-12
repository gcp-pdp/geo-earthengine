#!/bin/sh

CREDENTIALS="/root/gee-export.json"
COLLECTION="projects/earthengine-public/assets/MODIS/006/MOD17A3HGF"

export "GOOGLE_APPLICATION_CREDENTIALS=$CREDENTIALS"

IMAGES=$(ogrinfo -ro -al "EEDA:" -oo "COLLECTION=$COLLECTION" | grep 'gdal_dataset (String) = ' | cut -d '=' -f2 | tr -d ' ')
for IMAGE in $IMAGES
do
    NAME=$(basename "$IMAGE")
    if [ -f "${NAME}.tif" ]
    then
        echo "Image already downloaded: ${NAME}.tif"
    else
        echo "Fetching Image: $IMAGE -> ${NAME}"
        gdal_translate --config GDAL_CACHEMAX 30000 --config GDAL_HTTP_MAX_RETRY 100 --config GDAL_HTTP_RETRY_DELAY 600 -oo BLOCK_SIZE=2000 "$IMAGE" "${NAME}.tif"
    fi
done
