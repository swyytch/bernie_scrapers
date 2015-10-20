#!/usr/bin/env python2

import logging
import requests
import json
import os

from datetime import datetime
from dateutil import parser

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


class Bernie2016VideosScraper(Scraper):

    def __init__(self):
        Scraper.__init__(self)
        api_key = os.getenv("YOUTUBE_API_KEY")
        self.url = "https://www.googleapis.com/youtube/v3/search"
        self.params = {
          "order": "date",
          "maxResults": 10,
          "channelId": "UCH1dpzjCEiGAt8CXkryhkZg",
          "key": api_key,
          "type": "upload",
          "part": "snippet"
        }

    def translate(self, json):
      idJson = json["id"]
      snippetJson = json["snippet"]

      record = {
        "site": "youtube.com",
        "videoId": idJson["videoId"],
        "title": snippetJson["title"],
        "description": snippetJson["description"],
        "thumbnail_url": snippetJson["thumbnails"]["high"]["url"],
        "created_at": snippetJson["publishedAt"]
      }

      return record

    def go(self):
        r = self.get(self.url, params=self.params, result_format="json")
        for item in r["items"]:
          idJson = item["id"]
          
          query = {
            "site": "youtube.com",
            "videoId": idJson["videoId"]
          }
          
          record = self.translate(item)
          if self.db.videos.find(query).count() > 0:
            msg = "Updating record for '{0}'."
            logging.info(msg.format(record["title"]))
            self.db.videos.update_one(query, {"$set": record})
          else:
            msg = "Inserting record for {0}."
            logging.info(msg.format(record["title"]))
            record["inserted_at"] = datetime.now()
            self.db.videos.insert_one(record)



if __name__ == "__main__":
    bernie = Bernie2016VideosScraper()
    bernie.go()
