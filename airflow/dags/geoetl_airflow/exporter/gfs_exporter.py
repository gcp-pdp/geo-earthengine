from geoetl_airflow.exporter.base_exporter import BaseExporter
import os.path


class GFSExporter(BaseExporter):
    def __init__(self, **kwargs):
        super(GFSExporter, self).__init__(**kwargs)

    def build_cmds(self):
        date = '{{ execution_date.strftime("%Y-%m-%d") }}'
        date_time = '{{ (execution_date - macros.timedelta(hours=4)).strftime("%Y-%m-%dT%H:00:00") }}'
        for i in list(range(1, 121)) + list(range(123, 385, 3)):
            yield (
                "export-gfs-{interval}".format(interval=i),
                "export_to_gcs.sh -f {type} -d {date} -i {interval} -o {bucket} -e {exclude} -p {path}".format(
                    type="gfs",
                    date=date_time,
                    interval=i,
                    bucket=self.output_bucket,
                    exclude=self.excluded_images,
                    path=os.path.join(
                        self.output_path_prefix, "gfs", "date={date}".format(date=date)
                    ),
                ),
            )
