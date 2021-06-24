import logging
import os

from airflow import AirflowException

MEGABYTE = 1024 * 1024
WILDCARD = '*'


def upload_to_gcs(gcs_hook, bucket, object, filename, mime_type='application/octet-stream'):
    """Upload a file to GCS. Helps avoid OverflowError:
    https://stackoverflow.com/questions/47610283/cant-upload-2gb-to-google-cloud-storage,
    https://developers.google.com/api-client-library/python/guide/media_upload#resumable-media-chunked-upload
    """
    from apiclient.http import MediaFileUpload
    from googleapiclient import errors

    service = gcs_hook.get_conn()

    if os.path.getsize(filename) > 10 * MEGABYTE:
        media = MediaFileUpload(filename, mime_type, resumable=True)

        try:
            request = service.objects().insert(bucket=bucket, name=object, media_body=media)
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    logging.info("Uploaded %d%%." % int(status.progress() * 100))

            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise
    else:
        media = MediaFileUpload(filename, mime_type)

        try:
            service.objects().insert(bucket=bucket, name=object, media_body=media).execute()
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise


def download_from_gcs(gcs_hook, source_bucket, source_object, destination_dir, destination_file=None):
    if WILDCARD in source_object:
        total_wildcards = source_object.count(WILDCARD)
        if total_wildcards > 1:
            error_msg = "Only one wildcard '*' is allowed in source_object parameter. " \
                        "Found {} in {}.".format(total_wildcards, source_object)

            raise AirflowException(error_msg)

        prefix, delimiter = source_object.split(WILDCARD, 1)
        objects = gcs_hook.list(source_bucket, prefix=prefix, delimiter=delimiter)

        for source_object in objects:
            if destination_file is None:
                destination_file = source_object
            else:
                destination_file = source_object.replace(prefix,
                                                         destination_file, 1)
            download_single_object(gcs_hook=gcs_hook, source_bucket=source_bucket, source_object=source_object,
                                   destination_dir=destination_dir, destination_file=destination_file)
    else:
        download_single_object(gcs_hook=gcs_hook, source_bucket=source_bucket, source_object=source_object,
                               destination_dir=destination_dir, destination_file=destination_file)


def download_single_object(gcs_hook, source_bucket, source_object, destination_dir, destination_file):
    logging.info('Executing copy of gs://%s/%s to file://%s/%s',
                 source_bucket, source_object,
                 destination_dir, destination_file)

    gcs_hook.download(source_bucket, source_object, destination_file)
