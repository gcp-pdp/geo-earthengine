from geoetl_airflow.exporter.base_exporter import BaseExporter
import os.path


class GFSExporter(BaseExporter):
    def __init__(self, **kwargs):
        super(GFSExporter, self).__init__(export_type="gfs", **kwargs)

    def build_cmd(self):
        date = '{{ execution_date.strftime("%Y-%m-%d") }}'
        date_time = '{{ (execution_date - macros.timedelta(hours=4)).strftime("%Y-%m-%dT%H:00:00") }}'
        hour = '{{ (execution_date - macros.timedelta(hours=4)).strftime("%H") }}'
        return "export_to_gcs.sh -f {type} -d {date} -j {jobs} -e {exclude} -p {path} {overwrite}".format(
            type="gfs",
            date=date_time,
            exclude=self.excluded_images,
            jobs=self.export_parallel_jobs,
            overwrite="-r" if self.export_overwrite else "",
            path=os.path.join(
                self.output_path_prefix,
                "gfs",
                "date={date}".format(date=date),
                hour,
            ),
        )
