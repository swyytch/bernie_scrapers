#!/usr/bin/env python2

import logging
import types

from BeautifulSoup import BeautifulSoup
from datetime import datetime
from dateutil import parser
from HTMLParser import HTMLParser

logging.basicConfig(format="%(asctime)s - %(levelname)s : %(message)s",
                    level=logging.INFO)

if __name__ == "__main__":
    if __package__ is None:
        import sys
        from os import path
        sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
        from scraper import Scraper
    else:
        from ..scraper import Scraper


class IssuesScraper(Scraper):

    def __init__(self):
        Scraper.__init__(self)
        self.url = "https://berniesanders.com/issues/feed/"
        self.html = HTMLParser()

    def collect_urls(self):
        recs = []
        items = self.get(self.url).findAll("item")
        for item in items:
            rec = {
                "inserted_at": datetime.now(),
                "title": self.html.unescape(item.title.text),
                "created_at": parser.parse(item.pubdate.text),
                "site": "berniesanders.com",
                "lang": "en",
                "article_type": "Issues",
                "description_html": item.description.text,
                "description": self.html.unescape(
                    BeautifulSoup(item.description.text).p.text),
                "url": item.link.nextSibling
            }
            recs.append(rec)
        return recs

    def retrieve(self, rec):
        soup = self.get(rec["url"]).find("section", {"id": "content"})
        while soup.article.style is not None:
            soup.article.style.extract()
        rec["body_html"] = str(soup.article)
        text = []
        for elem in soup.article.recursiveChildGenerator():
            if isinstance(elem, types.StringTypes):
                text.append(self.html.unescape(elem.strip()))
            elif elem.name == 'br':
                text.append("")
        rec["body"] = "\n".join(text)
        return rec

    def go(self):
        urls = self.collect_urls()
        if not urls:
            logging.critical("Could not retrieve issues.")
            sys.exit(1)
        for url in urls:
            rec = self.retrieve(url)
            query = {
                "title": rec["title"],
                "article_type": rec["article_type"]
            }
            if not self.db.articles.find(query).limit(1).count():
                msg = "Inserting '{0}', created {1}"
                logging.info(msg.format(
                    rec["title"].encode("utf8"),
                    str(rec["created_at"])
                ))
                self.db.articles.insert_one(rec)

if __name__ == "__main__":
    i = IssuesScraper()
    i.go()
