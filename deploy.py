from subprocess import call

#call(["heroku", "ps:scale", "worker2=0", "--app", "importers-test"])

import psycopg2

try:
    conn = psycopg2.connect("dbname='importers' user='postgres' host='localhost' password='123456'")
except:
    print("can't connect to db")

cur = conn.cursor()

cur.execute("select id, status from jobsqueue_job")
rows = cur.fetchall()

for row in rows:
    print "   ", row[1]