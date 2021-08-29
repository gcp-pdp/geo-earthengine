from geoetl_airflow.exporter.base_exporter import BaseExporter
import os.path


class WorldPopExporter(BaseExporter):
    def __init__(self, **kwargs):
        super(WorldPopExporter, self).__init__(**kwargs)

    def build_cmds(self):
        year = '{{ execution_date.strftime("%Y") }}'
        yield (
            "export-world-pop",
            "export_to_gcs.sh -f {type} -y {year} -o {bucket} -e {exclude} -p {path}".format(
                type="world_pop",
                year=year,
                bucket=self.output_bucket,
                exclude=self.excluded_images,
                path=os.path.join(
                    self.output_path_prefix,
                    "world_pop",
                    "year={year}".format(year=year),
                ),
            ),
        )
