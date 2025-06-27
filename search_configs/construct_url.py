import os
from datetime import datetime, timedelta
import re
import yaml
import math
import json


base_url = "https://cian.ru"
# base_url = os.getenv("BASE_URL")


def generate_search_page_urls(base_url, total_pages):
    return [f"{base_url}&p={i+1}" for i in range(total_pages)]


def generate_listing_page_urls(offer_ids):
    return [f"{base_url}/rent/flat/{offer_id}" for offer_id in offer_ids]


def construct_search_url(config):

    url = f"{base_url}/cat.php?currency=2&engine_version=2&type=4&deal_type=rent&sort=creation_date_desc&"

    for key in config:
        if key == "district":
            if config["district"]:
                for i, district in enumerate(config["district"]):
                    url += f"district[{i}]={district}&"
        elif key == "street":
            if config["street"]:
                for i, street in enumerate(config["street"]):
                    url += f"street[{i}]={street}&"
        elif key == "metro":
            if config["metro"]:
                for i, metro in enumerate(config["metro"]):
                    url += f"metro[{i}]={metro}&"
        elif key == "rooms":
            if config["rooms"]:
                for room in config["rooms"]:
                    url += f"room{room}=1&"
        else:
            url += f"{key}={config[key]}&"

    return url.rstrip("&")