import sys, getopt
import time
import psycopg2
import subprocess
import requests
import json
import argparse
import logging

class ImportersDeployer(object):
    _db_name = None
    _db_user = None
    _db_host = None
    _db_password = None
    _git_remote = None
    _db_conn = None
    _heroku_app_name = None

    def check_args(self, argv):
        parser = argparse.ArgumentParser()
        parser.add_argument("--db-name", help="Importers database name on Postgres database server.")
        parser.add_argument("--db-user", help="Database user which have read rights for importers database.")
        parser.add_argument("--db-host", help="Database server URL.")
        parser.add_argument("--db-password", help="Password for provided database user.")
        parser.add_argument("--git-remote", help="Remote Git repository where you wanna push code (deploy).")
        parser.add_argument("--heroku-app-name", 
            help="Heroku app for which you're pushing deploy (this app's clock and web dyno will be stoped during deploy).")

        args = parser.parse_args()
        self._db_name = args.db_name
        self._db_user = args.db_user
        self._db_host = args.db_host
        self._db_password = args.db_password
        self._git_remote = args.git_remote
        self._heroku_app_name = args.heroku_app_name
        
        if self._db_name and self._db_user and self._db_host and self._db_password and self._git_remote and self._heroku_app_name:
            return True

        return False

    def init_db_conn(self):
        try:
            cnn_string = "dbname='{}' user='{}' host='{}' password='{}'".format(self._db_name, self._db_user, self._db_host, self._db_password)
            self._db_conn = psycopg2.connect(cnn_string)
        except:
            logging.exception("ERROR")
            return False

        return True

    def stop_services(self):
        return self.set_service_run_state("web", True) and self.set_service_run_state("clock", True)

    def start_services(self):
        return self.set_service_run_state("web", False) and self.set_service_run_state("clock", False)

    def set_service_run_state(self, resource_name, stop_service):
        try:
            formation_url = "apps/{}/formation/{}".format(self._heroku_app_name, resource_name)
            url = urlparse.urljoin("https://api.heroku.com", formation_url)
            quantity = 0 if stop_service else 1
            data = {"quantity": quantity}
            request = requests.patch(url, data=json.dumps(data), headers={"content-type": "application/json", "accept": "application/vnd.heroku+json; version=3"})
            response = json.loads(request.text)
            print request.text
            return response["quantity"] == quantity
        except:
            logging.exception("ERROR")
            return False

    def running_tasks_exist(self):
        try:
            cur = self._db_conn.cursor()
            cur.execute("select id from jobsqueue_job where status in ('running', 'queued')")
            rows = cur.fetchall()

            if rows:
                return True
        except:
            logging.exception("ERROR")
            
        return False

    def push_to_heroku(self):
        subprocess.call(["git", "push", self._git_remote, "HEAD:master"])


if __name__ == "__main__":
    deploya = ImportersDeployer()
    
    if not deploya.check_args(sys.argv[1:]):
        print "Invalid command"
        print "Usage: python deploy.py --db-name='<db-name>' --db-user='<db-user>' --db-host='<db-host>' --db-password='<db-password>' --git-remote='<git-remote>' --heroku-app-name='<heroku-app-name>'"
        sys.exit(2)

    if not deploya.init_db_conn():
        print("Failed to connect to database.")
        sys.exit(2)

    print "Stopping Heroku services"
    if not deploya.stop_services():
        print "Can't stop services. Check Heroku dashboard if all services are in correct state!!!"
        sys.exit(2)

    running_tasks_exist = True

    for x in range(0, 4):
        if not deploya.running_tasks_exist():
            running_tasks_exist = False
            break

        print ("There are tasks in running or queued state. Waiting for a while ........")
        time.sleep(5)

    if running_tasks_exist:
        print("There are unfinnished tasks (queued, running). Can't deploy")
    else:
        print "Pushing codes to Heroku"
        deploya.push_to_heroku()

    print "Starting Heroku services"
    if not deploya.start_services():
        print "Can't start services. Start services manually through Heroku dashboard"
    else:
        if running_tasks_exist:
            print "Unsuccessful deploy. Check for possible stuck tasks in database"
        else:
            print "Deploy finnished"
