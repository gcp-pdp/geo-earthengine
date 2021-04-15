# Google Earth Engine raster to BigQuery SQL convertion project

To extract big rasters like to dozens of gygabytes each from Google Earth Engine (GEE) that's required to have enough RAM+SWAP space for rasters fetching.
Per-chunk downloading process doesn't require lots of memory while there are no network or server errors. Otherwise, memory
intensive process of reorginizing downloaded chunks and re-downloading missed ones is using. Usually, we have 1-2 transfer
error for every 10 GB of downloaded rasters. 32 GB of RAM+SWAP is the right value for the scripts below where raster cache
size is equal to 30 000 MB (GDAL_CACHEMAX=30000). For 3 GB RAM plus 30 GB SWAP we are able to download 500 GB rasters
in 12-24 hours.

For Google Earth Engine (GEE) access the service account key required (it's named /root/gee-export.json in the scripts), to create your own one
follow the link [Create and register a service account to use Earth Engine](https://developers.google.com/earth-engine/guides/service_account)

## GeoTIFF to BigQuery CSV convertion tool

Use provided script [geotif-to-bqcsv.py](scripts/geotif-to-bqcsv.py) to convert WGS 84 GeoTIFF files to BigQuery CSV data and table schema:
```
./geotif-to-bqcsv.py GeoTIFF_file [CSV_file]
```
With just mandatory first argument the script returns corresponding BigQuery table schema only. With the optional second argument this script
converts the entire GeoTIFF file to CSV output into the specified file and also prints the schema too.

## [WorldPop Global Project Population Data: Estimated Residential Population per 100x100m Grid Square](https://developers.google.com/earth-engine/datasets/catalog/WorldPop_GP_100m_pop)

See [WorldPop.sh](scripts/WorldPop.sh) to extract data for 2020 year in WGS84 coordinates.

![](https://mw1.google.com/ges/dd/images/WorldPop_GP_100m_pop_sample.png)

## [MOD17A3HGF.006: Terra Net Primary Production Gap-Filled Yearly Global 500m](https://developers.google.com/earth-engine/datasets/catalog/MODIS_006_MOD17A3HGF)

See [AnnualNPP.sh](scripts/AnnualNPP.sh) to extract the entire dataset and convert it into WGS84 coordinates.

![](https://mw1.google.com/ges/dd/images/MODIS_006_MOD17A3HGF_sample.png)

## [GFS: Global Forecast System 384-Hour Predicted Atmosphere Data](https://developers.google.com/earth-engine/datasets/catalog/NOAA_GFS0P25)

See [GFS.sh](scripts/GFS.sh) to extract data for date "2021/04/13" and forecasting interval 384 hours in WGS84 coordinates.

![](https://mw1.google.com/ges/dd/images/NOAA_GFS0P25_sample.png)
