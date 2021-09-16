#!/usr/bin/env bash

OVERWRITE=false

while getopts p:j:d:y:f:e:r flag
do
  case "${flag}" in
    p) PREFIX=${OPTARG};;
    j) JOBS=${OPTARG};;
    d) DATE=${OPTARG};;
    y) YEAR=${OPTARG};;
    f) TASK=${OPTARG};;
    e) EXCLUDE=${OPTARG};;
    r) OVERWRITE=true;;
  esac
done

if [ -z "$TASK" ] ; then
    echo "task must be supplied"
fi

if [ -z "$DATA_DIR" ] ; then
    DATA_DIR=$(pwd)
fi

if [ -z "$JOBS" ] ; then
    JOBS=1
fi

echo "JOBS=${JOBS}"
echo "PREFIX=${PREFIX}"
echo "DATE=${DATE}"
echo "YEAR=${YEAR}"
echo "TASK=${TASK}"
echo "EXCLUDE=${EXCLUDE}"
echo "OVERWRITE=${OVERWRITE}"

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
      COLLECTION="projects/earthengine-public/assets/NOAA/GFS0P25"
      IMAGES=$(ogrinfo -ro -al "EEDA:" -oo "COLLECTION=$COLLECTION" -where "startTime='$DATE' and endTime='$DATE'" \
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

convert_tif_to_parquet() {
  TIF_FILE=$1
  PQ_FILE=$2
  if [[ -s "${PQ_FILE}" && "${OVERWRITE}" = false ]]
  then
    echo "Skip converting file ${PQ_FILE}: file already exist (use -r flag to overwrite)"
  elif [ -s "${TIF_FILE}" ]
  then
    echo "Converting file: $TIF_FILE -> $PQ_FILE"
    ./geotif_to_bqparquet.py $TIF_FILE $PQ_FILE
  else
    echo "Error converting to parquet: ${TIF_FILE} does not exist"
    exit 1
  fi
}

process_image() {
  IMAGE=$1
  NAME=$(basename "$IMAGE")
  if [[ -n "$EXCLUDE" && ",$EXCLUDE," = *",$NAME,"* ]]; then
    echo "Exclude image $NAME"
    continue
  fi
  TIF_FILE=$(file_path "$NAME.tif")
  PQ_FILE=$(file_path "$NAME.parquet")
  mkdir -p $(dirname "$TIF_FILE")
  mkdir -p $(dirname "$PQ_FILE")
  if [[ -s "${TIF_FILE}" && "${OVERWRITE}" = false ]]; then
    echo "Skip downloading ${TIF_FILE}: image already exists (use -r flag to overwrite)"
  else
    fetch_image $IMAGE $TIF_FILE
  fi
  convert_tif_to_parquet $TIF_FILE $PQ_FILE
}

# main
IMAGES=$(list_images)
if [ -z "$IMAGES" ] ; then
    echo "No GeoEarth image found"
    exit 1
fi

export DATA_DIR
export TASK
export PREFIX
export EXCLUDE
export OVERWRITE
export -f process_image
export -f file_path
export -f fetch_image
export -f convert_tif_to_parquet

parallel --jobs $JOBS process_image ::: $IMAGES