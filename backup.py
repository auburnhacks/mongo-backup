import os
import sys
import logging
import argparse
import datetime
import subprocess
from urllib.parse import urlparse

# log config
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
log = logging.getLogger(__name__)

# Create global argument parser for the script
parser = argparse.ArgumentParser(description="A cron job that backups mongodb to a specific folder in google drive")

parser.add_argument("--output_dir", help="Local output directory", default="./")
parser.add_argument("--folder_id", help="Google drive folderId", default="23r2d23d")
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

def main():
  args = parser.parse_args()
  today = datetime.datetime.now()
  log.info("starting mongo-backup at {}".format(today))
  log.info("mongo instance provided: {}".format(args.mongo_url))
  log.info("google drive folderId: {}".format(args.folder_id))

  url = urlparse(args.mongo_url)
  
  if url.scheme != "mongodb":
    log.error("url must be in the form mongodb://*")
    sys.exit(1)


  output_dir = os.path.abspath(
              os.path.join(args.output_dir,
              "{}_{}".format(url.path[1:], today.strftime('%Y_%m_%d_%H%M%S')))
  )

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
      log.info("backed up: {}".format(is_backed))
  except subprocess.CalledProcessError as e:
    log.error("error processing command exited with: {}".format(e.returncode))
    log.error("output from error: {}".format(e.output))
    sys.exit(1)

if __name__ == "__main__":
  main()