import json
import os
from pathlib import Path
from math import log

class maps():
    def __init__(self):
        utils_path=str(Path(__file__).parent)
        self.worldmap=json.load(open(utils_path+"/etc/world_map.json","r"))

    # Map of procedence of coauthors
    def get_coauthorship_world_map(self,data):
        countries={}
        for work in data:
            if not "country_code" in work["affiliation"]["addresses"].keys():
                continue
            if work["affiliation"]["addresses"]["country_code"] and work["affiliation"]["addresses"]["country"]:
                alpha2=work["affiliation"]["addresses"]["country_code"]
                country_name=work["affiliation"]["addresses"]["country"]
                if alpha2 in countries.keys():
                    countries[alpha2]["count"]+=work["count"]
                else:
                    countries[alpha2]={
                        "count":work["count"],
                        "name":country_name
                    }
        for key,val in countries.items():
            countries[key]["log_count"]=log(val["count"])
        for i,feat in enumerate(self.worldmap["features"]):
            if feat["properties"]["country_code"] in countries.keys():
               alpha2=feat["properties"]["country_code"]
               self.worldmap["features"][i]["properties"]["count"]=countries[alpha2]["count"]
               self.worldmap["features"][i]["properties"]["log_count"]=countries[alpha2]["log_count"]

        return self.worldmap