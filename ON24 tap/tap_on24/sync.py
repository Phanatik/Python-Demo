#!/usr/bin/env python3
import os
import json
import singer
import requests
import time
import pandas as pd
from datetime import timedelta, datetime
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.bookmarks import get_bookmark, write_bookmark
from singer.messages import write_state

def make_request(config, stream, tokens, query):
    return requests.get("https://api.on24.com/v2/client/" + str(config["client_id"]) + "/" + stream.tap_stream_id, headers = tokens, params = query)


def check_response(response):
    if not response:
        raise ValueError(str(response.status_code) + ":" + response.text)


def append_data(response, state, config, stream, tokens, query):
    ref = get_ref()
    key = "currentpage"
    tap_data = []
    if not response:
        return tap_data, state
    tap_data = response.json()[ref[stream.tap_stream_id]]
    if "pagecount" in response.json():
        tap_data = response.json()[ref[stream.tap_stream_id]]
        for i in range(0, response.json()["pagecount"]):
            query["pageoffset"] = i
            response = make_request(config, stream, tokens, query)
            if not response:
                state = write_bookmark(state, stream.tap_stream_id, key, i)
                return tap_data, state
            table = response.json()[ref[stream.tap_stream_id]]
            tap_data = tap_data + table
    
    return tap_data, state


def sync(config, state, catalog):
    LOGGER = singer.get_logger()
    ref = get_ref()
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream: " + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        #print(config)
        singer.write_schema(stream_name=stream.tap_stream_id, schema=stream.schema.to_dict(), key_properties=stream.key_properties)
        
        s_date = singer.get_bookmark(state, stream.tap_stream_id, "startdate")
        e_date = singer.get_bookmark(state, stream.tap_stream_id, "lastrun")
        offset = (pd.to_datetime(e_date) - pd.to_datetime(s_date)).days
        
        if offset <= 0:
            offset = 1
        
        tokens = {"accessTokenKey": config["token_key"], "accessTokenSecret": config["token_secret"]}
        query = {"datefiltermode": config["datefiltermode"], "filterorder": config["filterorder"], "dateinterval": config["interval"], "dateintervaloffset": offset }
        
        # TODO: delete and replace this inline function with your own data retrieval process:
        response = make_request(config, stream, tokens, query)
        
        tap_data, state = append_data(response, state, config, stream, tokens, query)
        for row in tap_data:
            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
        new_offset = query["dateintervaloffset"] - config["interval"]
        new_date = (datetime.now() - timedelta(new_offset)).strftime("%Y-%m-%d")
        state = write_bookmark(state, stream.tap_stream_id, "startdate", new_date)
        state = write_bookmark(state, stream.tap_stream_id, "lastrun", datetime.now().strftime("%Y-%m-%d"))
    state = singer.write_state(state)
    return

def get_ref():
    return {
        "event": "events",
        "registrant": "registrants",
        "attendee": "attendees",
        "attendeeviewingsession": "attendeesession"
        }
