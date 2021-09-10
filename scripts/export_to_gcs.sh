#!/usr/bin/env bash

while getopts o:p:d:i:y:f:e: flag
do
  case "${flag}" in
    o) BUCKET=${OPTARG};;
    p) PREFIX=${OPTARG};;
    d) DATE=${OPTARG};;
    i) HOURS=${OPTARG};;
    y) YEAR=${OPTARG};;
    f) TASK=${OPTARG};;
    e) EXCLUDE=${OPTARG};;
  esac
done

if [ -z "$TASK" ] ; then
    echo "task must be supplied"
fi

if [ -z "$DATA_DIR" ] ; then
    DATA_DIR=$(pwd)
fi

echo "BUCKET=${BUCKET}"
echo "PREFIX=${PREFIX}"
echo "DATE=${DATE}"
echo "HOURS=${HOURS}"
echo "YEAR=${YEAR}"
echo "TASK=${TASK}"
echo "EXCLUDE=${EXCLUDE}"

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

list_images() {
  case "${TASK}" in
    gfs)
      CREATION_TIME=$(date -u -d"${DATE}" +%s%3N)
      FORECAST_TIME=$(($CREATION_TIME + $HOURS * 3600000))
      COLLECTION="projects/earthengine-public/assets/NOAA/GFS0P25"
      IMAGES=$(ogrinfo -ro -al "EEDA:" -oo "COLLECTION=$COLLECTION" -where "startTime='$DATE' and endTime='$DATE' and (forecast_hours=$HOURS or forecast_time=$FORECAST_TIME)" \
      | grep 'gdal_dataset (String) = ' | cut -d '=' -f2 | tr -d ' ')
      ;;
    world_pop)
      COLLECTION="projects/earthengine-public/assets/WorldPop/GP/100m/pop"
      IMAGES=$(ogrinfo -ro -al "EEDA:" -oo "COLLECTION=$COLLECTION" -where "year=$YEAR" | grep 'gdal_dataset (String) = ' | cut -d '=' -f2 | tr -d ' ')
      ;;
    annual_npp)
      COLLECTION="projects/earthengine-public/assets/MODIS/006/MOD17A3HGF"
      IMAGES=$(ogrinfo -ro -al "EEDA:" -oo "COLLECTION=$COLLECTION" | grep 'gdal_dataset (String) = ' | cut -d '=' -f2 | tr -d ' ')
      ;;
  esac
  echo $IMAGES
}

file_path() {
  FILE_NAME=$1
  EXTENSION="${FILE_NAME##*.}"
  echo "$DATA_DIR/$PREFIX/$EXTENSION/$FILE_NAME"
}

fetch_image() {
  IMAGE=$1
  FILE=$2
  echo "Fetching Image: $IMAGE -> ${FILE}"
  case "${TASK}" in
    gfs)
      gdal_translate --config GDAL_CACHEMAX 30000 --config GDAL_HTTP_RETRY_DELAY 600 -oo BLOCK_SIZE=1000 "$IMAGE" "$FILE"
      ;;
    world_pop)
      gdal_translate --config GDAL_CACHEMAX 30000 --config GDAL_HTTP_RETRY_DELAY 600 -oo BLOCK_SIZE=2000 "$IMAGE" "$FILE"
      ;;
    annual_npp)
      gdalwarp --config GDAL_CACHEMAX 30000 --config GDAL_HTTP_RETRY_DELAY 600 \
       -oo BLOCK_SIZE=2000 -s_srs modis.prj -t_srs WGS84 "$IMAGE" "$FILE"
      # add metadata
      NAME=$(basename "$IMAGE")
      YEAR=$(echo "$NAME" | cut -f1 -d "_")
      gdal_edit.py -mo year="$YEAR" "$FILE"
      ;;
  esac
}

# Convert tif to csv
convert_tif_to_csv() {
  TIF_FILE=$1
  CSV_FILE=$2
  if [ -s "${CSV_FILE}" ]
  then
    echo "File ${CSV_FILE} is already exist"
  elif [ -s "${TIF_FILE}" ]
  then
    echo "Converting file: $TIF_FILE -> $CSV_FILE"
    ./geotif-to-bqcsv.py $TIF_FILE $CSV_FILE
  else
    echo "${TIF_FILE} does not exist"
    exit 1
  fi
}

# main
IMAGES=$(list_images)
if [ -z "$IMAGES" ] ; then
    echo "No GeoEarth image found"
    exit 1
fi

for IMAGE in $IMAGES
do
  NAME=$(basename "$IMAGE")
  if [[ -n "$EXCLUDE" && ",$EXCLUDE," = *",$NAME,"* ]]; then
    echo "Skip image $NAME"
    continue
  fi
  TIF_FILE=$(file_path "$NAME.tif")
  CSV_FILE=$(file_path "$NAME.csv")
  mkdir -p $(dirname "$TIF_FILE")
  mkdir -p $(dirname "$CSV_FILE")
  if [ -s "${TIF_FILE}" ]; then
    echo "Image already downloaded: ${TIF_FILE}"
  else
    fetch_image $IMAGE $TIF_FILE
  fi
  convert_tif_to_csv $TIF_FILE $CSV_FILE
done
