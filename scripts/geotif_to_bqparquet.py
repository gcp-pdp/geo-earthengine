#!/usr/bin/env python3
# Install Python modules
# python3 -m pip install gdal numpy --upgrade
# Use as
# ./geotif-to-bqcsv.py 2000_02_18.tif 2000_02_18.csv && echo OK || echo ERR
# or
# python3 geotif-to-bqcsv.py 2000_02_18.tif 2000_02_18.csv && echo OK || echo ERR
# or
# /usr/local/opt/python@3.7/bin/python3.7 geotif-to-bqcsv.py 2000_02_18.tif 2000_02_18.csv && echo OK || echo ERR
# Load as
# bq load --source_format=CSV --schema="as printed by the script" mydataset.mytable filename.csv
# https://cloud.google.com/bigquery/docs/gis-data#bq
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import box
from shapely.wkt import dumps
from osgeo import gdal
import numpy as np
from datetime import datetime

# default GDAL block size for chunk-by-chunk processing
block_size = 256

# just open the file and print BigQuery schema
if len(sys.argv) <= 1:
    print(f"Use as: \n{sys.argv[0]} GeoTIFF_file [CSV_file]")
    exit(1)
infile = sys.argv[1]

ds = gdal.Open(infile)
xsize, ysize = ds.RasterXSize, ds.RasterYSize

# print schema (GDAL bands datatype is the same always)
dt = ds.GetRasterBand(1).DataType
if dt in [
    gdal.GDT_Byte,
    gdal.GDT_Int16,
    gdal.GDT_UInt16,
    gdal.GDT_Int32,
    gdal.GDT_UInt32,
]:
    outtype = "INTEGER"
    pq_type = pa.int32()
    fmt = "%d"
else:
    outtype = "FLOAT"
    pq_type = pa.float64()
    fmt = "%g"

mds = ds.GetMetadata()
if "AREA_OR_POINT" in mds:
    del mds["AREA_OR_POINT"]

attr_values = []
bq_attr_fields = []
pq_attr_fields = []

for md in mds:
    if "country" == md:
        attr_values.append(mds[md])
        bq_attr_fields.append(f"{md}:STRING")
        pq_attr_fields.append((md, pa.string()))
    if "_time" in md:
        attr_values.append(int(mds[md]))
        bq_attr_fields.append(f"{md}:DATETIME")
        pq_attr_fields.append((md, pa.timestamp('ms')))
    elif "hours" in md or "year" in md:
        attr_values.append(int(mds[md]))
        bq_attr_fields.append(f"{md}:INTEGER")
        pq_attr_fields.append((md, pa.int32()))


print(
    ",".join(
        ["geography:GEOGRAPHY", "geography_polygon:GEOGRAPHY"]
        + bq_attr_fields
        + [
            f"{ds.GetRasterBand(iband+1).GetDescription()}:{outtype}"
            for iband in range(ds.RasterCount)
        ]
    )
)

# create parquet schema
fields = [
             ("geography", pa.string()),
             ("geography_polygon", pa.string())
         ] \
         + pq_attr_fields \
         + [(ds.GetRasterBand(iband + 1).GetDescription(), pq_type) for iband in range(ds.RasterCount)]
pq_schema = pa.schema(fields)
# Pandas dataframe column names
columns = [field_name for (field_name, field_type) in fields]

# process the file only when the output file specified
if len(sys.argv) <= 2:
    exit(0)
outfile = sys.argv[2]

# prepare transfrom from pixel to raster coordinates
gt = ds.GetGeoTransform()

# with open(outfile, "w") as fd:
with pq.ParquetWriter(outfile, pq_schema) as writer:
    for ix, iy in np.ndindex(
        (
            int(np.round(xsize / block_size + 0.5)),
            int(np.round(ysize / block_size + 0.5)),
        )
    ):
        xblock = int(np.min([xsize - ix * block_size, block_size]))
        yblock = int(np.min([ysize - iy * block_size, block_size]))
        array = ds.ReadAsArray(ix * block_size, iy * block_size, xblock, yblock)
        if array.size == 0:
            continue
        # unify dimentions for single band raster
        if len(array.shape) == 2:
            array = np.expand_dims(array, axis=0)
        array = array.reshape(ds.RasterCount, -1).T
        xs = ix * block_size + np.arange(xblock)
        ys = iy * block_size + np.arange(yblock)
        xs, ys = np.meshgrid(xs, ys)

        geo_xs = gt[0] + (xs + 0.5) * gt[1] + (ys + 0.5) * gt[2]
        geo_ys = gt[3] + (xs + 0.5) * gt[4] + (ys + 0.5) * gt[5]

        max_xs = (geo_xs + (0.5 * gt[1])).ravel().clip(-180, 180).round(8)
        min_xs = (geo_xs - (0.5 * gt[1])).ravel().clip(-180, 180).round(8)
        max_ys = (geo_ys + (0.5 * -gt[5])).ravel().clip(-90, 90).round(8)
        min_ys = (geo_ys - (0.5 * -gt[5])).ravel().clip(-90, 90).round(8)

        boxes = zip(min_xs, min_ys, max_xs, max_ys)

        rows = [
            ["POINT(%.10g %.10g)" % (x, y), dumps(box(*p), trim=True), *attr_values, *vals]
            for (x, y, p, vals) in zip(geo_xs.ravel(), geo_ys.ravel(), boxes, array)
        ]

        df = pd.DataFrame(rows, columns=columns)
        table = pa.Table.from_pandas(df, schema=pq_schema)
        writer.write_table(table)
