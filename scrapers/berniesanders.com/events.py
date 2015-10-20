#!/usr/bin/env python2

import logging

from datetime import datetime
from dateutil import parser
from HTMLParser import HTMLParser

if __name__ == "__main__":
    if __package__ is None:
        import sys
        from os import path
        sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
        from scraper import Scraper
    else:
        from ..scraper import Scraper

allowed_keys = [
    "original_id",
    "id_obfuscated",
    "url",
    "name",
    "start_time",
    "timezone",
    "description",
    "venue",
    "lat",
    "lon",
    "is_official",
    "attendee_count",
    "capacity",
    "site",
    "lang",
    "event_type_name"
]

logging.basicConfig(format="%(asctime)s - %(levelname)s : %(message)s",
                    level=logging.INFO)


class EventScraper(Scraper):

    def __init__(self):
        Scraper.__init__(self)
        self.url = "https://go.berniesanders.com/page/event/search_results"
        self.html = HTMLParser()
        self.params = {
            'orderby': 'zip_radius',
            'zip_radius[1]': '6000',
            'zip_radius[0]': '78218',
            'radius_unit': 'mi',
            'country': 'US',
            'format': 'json'
        }
        self.map = {
            "id": "original_id",
            "start_dt": "start_time"
        }

    def translate(self, result):
        # Translate normal key names based on map
        result = dict((self.map.get(k, k), v) for (k, v) in result.items())

        # Compile Venue
        address_map = {
            "venue_addr1": "address1",
            "venue_addr2": "address2",
            "venue_addr3": "address3"
        }
        result["venue"] = {
            "name": result["venue_name"],
            "city": result["venue_city"],
            "state": result["venue_state_cd"],
            "zip": result["venue_zip"],
            "location": {
                "lon": float(result["longitude"]),
                "lat": float(result["latitude"])
            }
        }
        # map any available address fields
        for k, v in address_map.iteritems():
            try:
                result["venue"][v] = result[k]
            except KeyError:
                pass

        # set sitename
        result["site"] = "berniesanders.com"
        result["lang"] = "en"
        # convert capacity and attendee_count to int's
        for x in ["capacity", "attendee_count"]:
            try:
                result[x] = int(result[x])
            except KeyError:
                pass

        # Convert str to datetime
        result["start_time"] = parser.parse(result["start_time"])
        result["is_official"] = result["is_official"] == "1"
        # remove any unneeded keys
        keys = result.keys()
        for k in keys:
            if k not in allowed_keys:
                result.pop(k)
        return result

    def go(self):
        r = self.get(
            self.url,
            params=self.params,
            result_format="json"
        )
        for result in r["results"]:
            rec = self.translate(result)
            query = {
                "original_id": rec["original_id"],
                "site": "berniesanders.com"
            }
            if self.db.events.find(query).count() > 0:
                msg = "Updating record for '{0}'."
                logging.info(msg.format(rec["name"]))
                self.db.events.update_one(query, {"$set": rec})
            else:
                msg = "Inserting record for {0}."
                logging.info(msg.format(rec["name"]))
                rec["inserted_at"] = datetime.now()
                self.db.events.insert_one(rec)

if __name__ == "__main__":
    bernie = EventScraper()
    bernie.go()
