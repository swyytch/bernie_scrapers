
import logging
import requests
import sys
import time
import types
import yaml

from abc import ABCMeta, abstractmethod
from BeautifulSoup import BeautifulSoup, Comment
from pymongo import MongoClient


class Scraper(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self.configfile = "/opt/bernie/config.yml"
        self.config = self.get_config()
        self.db = self.mongo().bernie

    def sanitize_soup(self, soup):
        # inspired by https://chase-seibert.github.io/blog/2011/01/28/sanitize-html-with-beautiful-soup.html  
        blacklist = ["script", "noscript", "video"]

        for tag in soup.findAll():
            if tag.name.lower() in blacklist:
                tag.extract()

        comments = soup.findAll(text=lambda text:isinstance(text, Comment))
        for comment in comments:
            comment.extract()

        return soup

    def remove_style(self, soup):
        for style in soup.findAll("style"):
            style.extract()
        return soup

    def replace_with_newlines(self, element):
        text = ''
        for elem in element.recursiveChildGenerator():
            if isinstance(elem, types.StringTypes):
                text += elem.rstrip("\n")
            elif elem.name == 'br':
                text += '\n'
        return text

    def get_config(self):
        try:
            with open(self.configfile, 'r') as f:
                conf = yaml.load(f)
        except IOError:
            msg = "Could not open config file: {0}"
            logging.info(msg.format(self.configfile))
            sys.exit(1)
        else:
            return conf

    def mongo(self):
        c = self.config["mongo"]
        db = MongoClient(c["host"], c["port"])
        db.admin.authenticate(
            c["username"],
            c["password"],
            mechanism='SCRAM-SHA-1'
        )
        return db

    def get(self, url, params=False, result_format="html"):
        for x in range(3):
            if params:
                r = requests.get(url, params=params)
            else:
                r = requests.get(url)
            if r.status_code == 200:
                if result_format in ("html", "xml"):
                    return BeautifulSoup(r.text)
                elif result_format == "json":
                    return r.json()
            time.sleep(5)
        msg = "Received {0} from {1} after 3 attempts."
        logging.critical(msg.format(r.status_code, r.url))

    @abstractmethod
    def go(self):
        pass
