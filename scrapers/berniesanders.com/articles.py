#!/usr/bin/env python2

import logging
import requests

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


class ArticlesScraper(Scraper):

    def __init__(self):
        Scraper.__init__(self)
        self.url = "https://berniesanders.com/daily/"
        self.html = HTMLParser()

    def retrieve_article(self, url):
        for x in range(3):
            r = requests.get(url)
            if "https://berniesanders.com" not in r.url:
                return r.url, False, False
            if r.status_code == 200:
                soup = BeautifulSoup(r.text)
                soup = self.sanitize_soup(soup)
                image = soup.find('meta', {'property': 'og:image'})['content']
                content = soup.article
                paragraphs = [self.html.unescape(self.replace_with_newlines(p))
                              for p in content.findAll("p")]
                text = "\n\n".join(paragraphs)
                html = "".join([str(p) for p in content.findAll("p")])
                return text, html, image
        return False, False, False

    def go(self):
        soup = self.get(self.url)
        content = soup.find("section", {"id": "content"})
        for article in content.findAll("article"):
            rec = {
                "inserted_at": datetime.now(),
                "created_at": parser.parse(article.time["datetime"]),
                "site": "berniesanders.com",
                "lang": "en",
                "article_type": "DemocracyDaily",
                "excerpt_html": str(article.find(
                    "div", {"class": "excerpt"}).p),
                "excerpt": self.html.unescape(
                    article.find(
                        "div", {"class": "excerpt"}).p.text),
                "title": article.h2.text,
                "article_category": article.h1.string.strip(),
                "url": article.h2.a["href"]
            }
            if article.img is not None:
                rec["image_url"] = article.img["src"]

            query = {
                "title": rec["title"],
                "article_type": rec["article_type"]
            }
            if not self.db.articles.find(query).limit(1).count():
                text, html, image = self.retrieve_article(rec["url"])
                if text and not html:
                    rec["body"], rec["body_html"] = text, text
                    rec['article_type'] = "ExternalLink"
                elif text and html:
                    rec["body"], rec["body_html"] = text, text
                    try:
                        article["image_url"]
                    except KeyError:
                        article["image_url"] = image
                msg = "Inserting '{0}', created {1}"
                logging.info(msg.format(
                    rec["title"].encode("utf8"),
                    str(rec["created_at"])
                ))
                self.db.articles.insert_one(rec)

if __name__ == "__main__":
    bernie = ArticlesScraper()
    bernie.go()
