import os
import sys
import logging
import argparse
import datetime
import requests
import subprocess
from urllib.parse import urlparse
from google.cloud import storage

# log config
logging.basicConfig(format='[%(asctime)s - %(levelname)s]: %(message)s', level=logging.DEBUG)
log = logging.getLogger(__name__)

# Create global argument parser for the script
parser = argparse.ArgumentParser(description="A cron job that backups mongodb to a specific folder in google drive")

parser.add_argument("--bucket_name", help="GCS bucket name")
parser.add_argument("--output_dir", help="Local output directory", default="./")
parser.add_argument("--kube", help="Set this flags if the job in running on kubernetes", action="store_true")
parser.add_argument("--mongo_url", help="A mongodb uri that needs to be connected", default="mongodb://localhost:27017")


def backup_mongo(netloc: str, username: str, password: str,
                hostname: str, port: int, db: str,
                output_dir: str) -> bool:
  """
    backup_mongo is a function that uses the subprocess library to
    run the mongodump command and collects the output from the
    output_dir.

    Args:
    :netloc     string
    :username   string
    :password   string
    :hostname   string
    :port:      int
    :db         string
    :output_dir string

    :return bool that indicates when the function has
                 returned successfully
  """
  backup_output = subprocess.check_output([
    "mongodump",
    "--host", "{}".format(hostname),
    "-u", "{}".format(username),
    "-p", "{}".format(password),
    "-d", "{}".format(db),
    "--port", "{}".format(port),
    "-o", output_dir
    ])

  log.info(backup_output)

  return True

def zip_backup(archive_name: str, output_dir: str) -> bool:
  """
  zip_backup takes zips the archive into a tar gzip format that can
  be used to upload to a cloud location.

  Args:
  :archive_name string
  :output_dir   string

  :return bool
  """
  zip_output = subprocess.check_output([
    "tar", "-zcvf", "{}.tar.gz".format(archive_name), "{}".format(output_dir)
  ])
  log.info(zip_output)
  return True

def cleanup(archive_loc: str, output_dir: str) -> bool:
  log.info("clean up archive {}".format(archive_loc))
  rm_archive = subprocess.check_output([
    "rm", "-rf", "{}.tar.gz".format(archive_loc)
  ])
  log.info(rm_archive)
  log.info("cleaning up backup directory: {}".format(output_dir))
  rm_output_dir = subprocess.check_output([
    "rm", "-rf", "{}".format(output_dir)
  ])
  log.info(rm_output_dir)
  return True

def cloud_upload(bucket_name: str, archive_name: str, archive_loc: str) -> bool:
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(archive_name)

  log.info("uploading the archive from: {}".format(archive_loc))

  blob.upload_from_filename("{}.tar.gz".format(archive_loc))

  log.info("file {} uploaded".format(archive_name))
  return True

def save_cred_file() -> str:
  gcs_data = os.environ['GCS_SECRETS']
  wd = os.getcwd()
  cred_file_dir = os.path.join(wd, 'auburn-hacks-gcs.json')
  fhandle = open(cred_file_dir, 'w')
  fhandle.write(gcs_data)
  fhandle.close()
  return cred_file_dir

def send_success_email() -> bool:
    payload = {
        'to_emails': [
            'szd0053@tigermail.auburn.edu',
            'tzj0019@tigermail.auburn.edu'
        ],
        'subject': '[AuburnHacks Cluster] - MongoDB update successful',
        'email_text': 'Hello admins,\n Just finished mongoDB backup. All is well!\nRegards,\nmongobackup',
    }
    requests.post("http://postman-svc/email/send_now", data=payload)
    return True


def main():
  args = parser.parse_args()
  today = datetime.datetime.now()
  log.info("starting mongo-backup at {}".format(today))
  log.info("mongo instance provided: {}".format(args.mongo_url))

  if args.kube:
    log.info("job running in kubernetes mode.")
    log.info("saving credentials file...")
    cred_file_dir = save_cred_file()
    if len(cred_file_dir) > 0:
      log.info("credentials downloaded successfully")
  else:
    log.info("job running in normal mode.")

  url = urlparse(args.mongo_url)

  if not args.bucket_name:
    parser.print_help()
    sys.exit(1)

  # setting the google cloud storage variable
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./auburn-hacks-gcs.json"
  if args.kube:
    log.info("setting the google application env variable")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cred_file_dir

  if url.scheme != "mongodb":
    log.error("url must be in the form mongodb://*")
    sys.exit(1)


  output_dir = os.path.abspath(
              os.path.join(args.output_dir,
              "{}_{}".format(url.path[1:], today.strftime('%Y_%m_%d_%H%M%S')))
  )

  archive_name = "{}_{}".format(url.path[1:], today.strftime('%Y_%m_%d_%H%M%S'))
  cloud_filename = "{}.tar.gz".format(archive_name)
  archive_name = os.path.join(args.output_dir, archive_name)



  log.info("will save to file: {}".format(output_dir))

  try:
      is_backed = backup_mongo(
        url.netloc,
        url.username,
        url.password,
        url.hostname,
        url.port,
        url.path[1:],
        output_dir
      )
      if is_backed:
        log.info("backup successful startedAt: {} endedAt: {}".format(today, datetime.datetime.now()))

      if zip_backup(archive_name, output_dir):
        log.info("backup zipped successfully")

      cloud_upload(args.bucket_name, cloud_filename, archive_name)

      if cleanup(archive_name, output_dir):
        log.info("sucessfully cleaned up everything")

  except subprocess.CalledProcessError as e:
    log.error("error processing command exited with: {}".format(e.returncode))
    log.error("output from error: {}".format(e.output))
    sys.exit(1)
  if args.kube:
    send_success_email()
if __name__ == "__main__":
  main()
