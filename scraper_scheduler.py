#!/usr/bin/env python2

import logging
import os
import schedule
import sys
import threading
import time
import yaml

from docker import Client, utils
from Queue import Queue

logging.basicConfig(format="%(asctime)s - %(levelname)s : %(message)s",
                    level=logging.INFO)


class Scheduler(object):

    def __init__(self):
        self.configfile = "/opt/bernie/config.yml"
        self.config = self.config()["docker"]
        self.con = self.docker_connect()

    def docker_connect(self):
        host = self.config["host"]
        port = self.config["port"]
        base_url = ['tcp://', host, ':', port]
        con = Client(base_url=''.join(base_url))
        return con

    def config(self):
        try:
            with open(self.configfile, 'r') as f:
                conf = yaml.load(f)
        except IOError:
            msg = "Could not open config file: {0}"
            logging.info(msg.format(self.configfile))
            sys.exit(1)
        else:
            return conf

    def start(self, folder, name, command):
        name = ["scraper", folder, name, str(int(time.time()))]
        container = self.con.create_container(
            name='_'.join(name),
            image='bernie_scraper:prod',
            command="".join(("python ", command)),
            labels={
                "scraper": "True"
            },
            detach=True,
            host_config=utils.create_host_config(
                binds={
                    '/opt/bernie': {
                        'bind': '/opt/bernie',
                        'ro': True
                    }
                }
            )
        )
        msg = "Starting {0}..."
        logging.info(msg.format("_".join(name)))
        self.con.start(container=container.get('Id'))
        self.con.wait(container)

    def go(self):
        d = "/opt/bernie/bernie_scraper/scrapers/"
        subdirs = filter(os.path.isdir,
                         [os.path.join(d, f) for f in os.listdir(d)])
        for subdir in subdirs:
            for root, _, names in os.walk(subdir):
                for name in names:
                    if "__init__.py" not in name:
                        folder = root.split("/")[-1]
                        item = name.split(".")[0]
                        command = os.path.join(root, name)
                        job = (self.start, folder, item, command)
                        jobqueue.put(job)

    def clear_scrapers(self):
        expiration = int(time.time()) - 24 * 360
        filters = {"label": "scraper", "status": "exited"}
        for scraper in self.con.containers(filters=filters):
            if scraper["Created"] < expiration:
                msg = "Clearing Finished Scraper {0}."
                logging.info(msg.format(scraper['Names'][0]))
                self.con.remove_container(scraper)


def worker():
    while 1:
        items = jobqueue.get()
        job = items[0]
        args = items[1:]
        job(*args)
        time.sleep(1)

jobqueue = Queue()
s = Scheduler()

schedule.every(1).hour.do(s.go).run()
schedule.every(24).hours.do(s.clear_scrapers).run()

for i in range(2):
    logging.debug('starting thread {}'.format(i))
    t = threading.Thread(target=worker)
    t.daemon = True
    t.start()

while 1:
    schedule.run_pending()
    time.sleep(5)
