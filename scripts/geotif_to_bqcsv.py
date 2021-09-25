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

from shapely.geometry import Polygon, box
from shapely.wkt import dumps
from osgeo import gdal, osr
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
    fmt = "%d"
else:
    outtype = "FLOAT"
    fmt = "%g"

mds = ds.GetMetadata()
if "AREA_OR_POINT" in mds:
    del mds["AREA_OR_POINT"]
mfmt = [""]
mfld = []
mtps = []
for md in mds:
    if "country" == md:
        mfmt.append("%s")
        mfld.append(mds[md])
        mtps.append(f"{md}:STRING")
    if "_time" in md:
        dt = datetime.utcfromtimestamp(float(mds[md]) / 1000).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        mfmt.append("%s")
        mfld.append(dt)
        mtps.append(f"{md}:DATETIME")
    elif "hours" in md or "year" in md:
        mfmt.append("%d")
        mfld.append(int(mds[md]))
        mtps.append(f"{md}:INTEGER")

fmt = (
    '"POINT(%.10g %.10g)"'
    + ',"%s"'
    + ",".join(mfmt)
    + ","
    + ((fmt + ",") * ds.RasterCount).rstrip(",")
)
print(
    ",".join(
        ["geography:GEOGRAPHY", "geography_polygon:GEOGRAPHY"]
        + mtps
        + [
            f"{ds.GetRasterBand(iband+1).GetDescription()}:{outtype}"
            for iband in range(ds.RasterCount)
        ]
    )
)

# process the file only when the output file specified
if len(sys.argv) <= 2:
    exit(0)
outfile = sys.argv[2]

# prepare transfrom from pixel to raster coordinates
gt = ds.GetGeoTransform()

with open(outfile, "w") as fd:
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

        fd.write(
            "\n".join(
                [
                    fmt % (x, y, dumps(box(*p), trim=True), *mfld, *vals)
                    for (x, y, p, vals) in zip(
                        geo_xs.ravel(), geo_ys.ravel(), boxes, array
                    )
                ]
            )
            + "\n"
        )