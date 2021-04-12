# Google Earth Engine raster datasets extraction project

To extract big rasters like to dozens of gygabytes each that's required to have enough RAM+SWAP space for rasters fetching.
Per-chunk downloading process doesn't require lots of memory while there are no network or server errors. Otherwise, memory
intensive process of reorginizing downloaded chunks and re-downloading missed ones is using. Usually, we have 1-2 transfer
error for every 10 GB of downloaded rasters. 32 GB of RAM+SWAP is the right value for the scripts below where raster cache
size is equal to 30 000 MB (GDAL_CACHEMAX=30000). For 3 GB RAM plus 30 GB SWAP we are able to download 500 GB rasters
in 12-24 hours.

## [WorldPop Global Project Population Data: Estimated Residential Population per 100x100m Grid Square](https://developers.google.com/earth-engine/datasets/catalog/WorldPop_GP_100m_pop)

See [WorldPop.sh](scripts/WorldPop.sh)

## [MOD17A3HGF.006: Terra Net Primary Production Gap-Filled Yearly Global 500m](https://developers.google.com/earth-engine/datasets/catalog/MODIS_006_MOD17A3HGF)

See [AnnualNPP.sh](scripts/AnnualNPP.sh)
