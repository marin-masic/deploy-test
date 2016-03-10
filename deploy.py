import sys, getopt
import time
import psycopg2
from subprocess
import requests
import json
import urlparse
import os
import logging

class ImportersDeployer(object):
    _dbname = None
    _dbuser = None
    _dbhost = None
    _dbpassword = None
    _gitremote = None
    _dbconn = None
    _heroku_app_name = None

    def check_args(self, argv):
        try:
            opts, args = getopt.getopt(argv, "hn:u:h:p:r:a", ["dbname=", "dbuser=", "dbhost=", "dbpassword=", "gitremote=", "heroku_app_name="])
        except getopt.GetoptError:
            logging.exception("ERROR")
            return False

        for opt, arg in opts:
            if opt in ("-n", "--dbname"):
                self._dbname = arg
            elif opt in ("-u", "--dbuser"):
                self._dbuser = arg
            elif opt in ("-h", "--dbhost"):
                self._dbhost = arg
            elif opt in ("-p", "--dbpassword"):
                self._dbpassword = arg
            elif opt in ("-r", "--gitremote"):
                self._gitremote = arg
            elif opt in ("a", "--heroku_app_name"):
                self._heroku_app_name = arg

        if self._dbname and self._dbuser and self._dbhost and self._dbpassword and self._gitremote and self._heroku_app_name:
            return True

        return False

    def init_db_conn(self):
        try:
            cnn_string = "dbname='{}' user='{}' host='{}' password='{}'".format(self._dbname, self._dbuser, self._dbhost, self._dbpassword)
            self._dbconn = psycopg2.connect(cnn_string)
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
            formation_url = os.environ["HEROKU_FORMATION_API_URL"].format(self._heroku_app_name, resource_name)
            url = urlparse.urljoin(os.environ["HEROKU_API_URL"], formation_url)
            quantity = 0 if stop_service else 1
            data = {"quantity": quantity}
            request = requests.patch(url, data=json.dumps(data), headers={"content-type": "application/json", "accept": "application/vnd.heroku+json; version=3"})
            response = json.loads(request.text)
            print request.text
            return response["quantity"] == 0
        except:
            logging.exception("ERROR")
            return False

    def running_tasks_exist(self):
        cur = self._dbconn.cursor()
        cur.execute("select id from jobsqueue_job where status in ('running', 'queued')")
        rows = cur.fetchall()

        if rows:
            return False

        return True

    def push_to_heroku(self):
        subprocess.call(["git", "push", self._gitremote, "head:master"])


if __name__ == "__main__":
    deploya = ImportersDeployer()
    
    if not deploya.check_args(sys.argv[1:]):
        print "Invalid command"
        print "Usage: python deploy.py --dbname='<dbname>' --dbuser='<dbuser>' --dbhost='<dbhost>' --dbpassword='<dbpassword>' --gitremote='<gitremote>' --heroku_app_name='<heroku_app_name>'"
        sys.exit(2)

    if not deploya.init_db_conn():
        print("Failed to connect to database.")
        sys.exit(2)

    print "Stopping Heroku services"
    if not deploya.stop_services():
        print "Can't stop services. Check Heroku dashboard if all services are in correct state!!!"
        sys.exit(2)

    running_tasks_exist = True

    for x in range(0, 20):
        if not deploya.running_tasks_exist():
            running_tasks_exist = False
            break

        time.sleep(3)

    if running_tasks_exist:
        print("There are unfinnished tasks (queued, running). Can't deploy")
    else:
        print "Pushing codes to Heroku"
        deploya.push_to_heroku()

    print "Starting Heroku services"
    if not deploya.stop_services():
        print "Can't start services. Start services manually through Heroku dashboard"
    else:
        print "Deploy finnished"
