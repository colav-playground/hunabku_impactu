import json
from pandas import read_csv
from pathlib import Path
from math import log

class maps():
    def __init__(self):
        utils_path=str(Path(__file__).parent)
        self.worldmap=json.load(open(utils_path+"/etc/world_map.json","r"))
        self.colombiamap=json.load(open(utils_path+"/etc/colombia_map.json","r"))
        self.municipios_departamentos=read_csv(utils_path+"/etc/Municipios_por_departamento.csv")

    # Map of world procedence of coauthors
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

    #map of colombian coauthors
    def get_coauthorship_colombia_map(self,data):
        departments={}
        for work in data:
            if not "country_code" in work["affiliation"]["addresses"].keys():
                continue
            if work["affiliation"]["addresses"]["country_code"] and work["affiliation"]["addresses"]["city"]:
                city=work["affiliation"]["addresses"]["city"]
                department=self.municipios_departamentos[self.municipios_departamentos["MUNICIPIO"]==city]["DEPARTAMENTO"]
                if len(department)==0:
                    continue
                department=department.iloc[0]
                if department in departments.keys():
                    departments[department]["count"]+=work["count"]
                else:
                    departments[department]={
                        "count":work["count"],
                        "name":department
                    }
        for key,val in departments.items():
            departments[key]["log_count"]=log(val["count"])
        for i,feat in enumerate(self.colombiamap["features"]):
            dep_name=feat["properties"]["NOMBRE_DPT"].capitalize() if "bogota" not in feat["properties"]["NOMBRE_DPT"].lower() else "Bogotá D.C."
            if dep_name in departments.keys():
                self.colombiamap["features"][i]["properties"]["count"]=departments[dep_name]["count"]
                self.colombiamap["features"][i]["properties"]["log_count"]=departments[dep_name]["log_count"]
            else:
                self.colombiamap["features"][i]["properties"]["count"]=0
                self.colombiamap["features"][i]["properties"]["log_count"]=0

        return self.colombiamap