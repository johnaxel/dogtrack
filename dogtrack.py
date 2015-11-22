#!/usr/bin/env python

import requests
import sqlite3
import time
from calendar import timegm
from datadog import initialize, api
import json

settings = json.load(open('/home/john/dogtrack/conf/settings.json'))

url = 'https://www.strava.com/api/v3/clubs/%s/activities' % settings['strava']['club_id']
headers = {'Authorization': 'Bearer %s' % settings['strava']['token']}

options = {
    'api_key': settings['datadog']['api_key'],
    'app_key': settings['datadog']['app_key']
}

initialize(**options)

r = requests.get(url, headers=headers)
conn = sqlite3.connect('/home/john/dogtrack/runlog.db')
c = conn.cursor()

for run in r.json():
    activity_id = str(run['id'])
    runner = run['athlete']['firstname'].lower()
    distance = float(run['distance']) / 1609.34
    time_start = time.strptime(run['start_date_local'], '%Y-%m-%dT%H:%M:%SZ')
    timestamp = timegm(time_start)
    c.execute('SELECT * FROM runs WHERE run_id=?', (activity_id,))
    if c.fetchone() is None:
        c.execute('INSERT INTO runs VALUES (?,?,?,?)', 
            (activity_id, runner, distance, timestamp,))
        conn.commit()
    else:
        pass

seven_days = time.time() - 604800

c.execute('SELECT runner, sum(miles) FROM runs WHERE time > ? GROUP BY runner;', 
    (seven_days,))
seven_day_mileage = c.fetchall()

c.execute('SELECT runner, sum(miles) FROM runs GROUP BY runner;')
all_time_mileage = c.fetchall()

c.execute('SELECT runner, miles FROM runs ORDER BY miles DESC LIMIT 5;')
high_scores = c.fetchall()

conn.close()

for dog in seven_day_mileage:
    host = str(dog[0])
    point = round(dog[1], 3)
    api.Metric.send(metric='dogtrack.running.seven_day_mileage', points=point,
        host="runclub", tags=["runner:%s" % host])

for dog in all_time_mileage:
    host = str(dog[0])
    point = round(dog[1], 3)
    api.Metric.send(metric='dogtrack.running.alltime_mileage', points=point,
        host="runclub", tags=["runner:%s" % host])

for dog in high_scores:
    host = str(dog[0])
    point = round(dog[1], 3)
    api.Metric.send(metric='dogtrack.running.high_scores', points=point,
        host="runclub", tags=["runner:%s" % host])

