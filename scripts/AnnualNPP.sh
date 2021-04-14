#!/bin/sh
# Convert to WGS84 from Global MODIS Sinusoidal projection
# See GDAL issue:
# https://github.com/OSGeo/gdal/issues/3677

CREDENTIALS="/root/gee-export.json"
COLLECTION="projects/earthengine-public/assets/MODIS/006/MOD17A3HGF"

export "GOOGLE_APPLICATION_CREDENTIALS=$CREDENTIALS"

# use the recent project definition
cat << EOF >> modis.prj
PROJCS["MODIS Sinusoidal",
    GEOGCS["WGS 84",
        DATUM["WGS_1984",
            SPHEROID["WGS 84",6378137,298.257223563,
                AUTHORITY["EPSG","7030"]],
            AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.01745329251994328,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]],
    PROJECTION["Sinusoidal"],
    PARAMETER["false_easting",0.0],
    PARAMETER["false_northing",0.0],
    PARAMETER["central_meridian",0.0],
    PARAMETER["semi_major",6371007.181],
    PARAMETER["semi_minor",6371007.181],
    UNIT["m",1.0],
    AUTHORITY["SR-ORG","6974"]]
EOF

IMAGES=$(ogrinfo -ro -al "EEDA:" -oo "COLLECTION=$COLLECTION" | grep 'gdal_dataset (String) = ' | cut -d '=' -f2 | tr -d ' ')
for IMAGE in $IMAGES
do
    NAME=$(basename "$IMAGE")
    if [ -f "${NAME}.tif" ]
    then
        echo "Image already downloaded: ${NAME}.tif"
    else
        echo "Fetching and Reproject Image: $IMAGE -> ${NAME}"
        gdalwarp --config GDAL_CACHEMAX 30000 --config GDAL_HTTP_RETRY_DELAY 600 -oo BLOCK_SIZE=2000 \
        -s_srs modis.prj -t_srs WGS84 \
        "$IMAGE" "${NAME}.tif"
    fi
done
