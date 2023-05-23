from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from bson import ObjectId
from pymongo import MongoClient,ASCENDING,DESCENDING
from hunabku.Config import Config, Param
from hunabku_impactu.utils.encoder import JsonEncoder
from math import nan


class WorkApp(HunabkuPluginBase):
    config=Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config+=Param(colav_db="colombia_udea")
    config+=Param(impactu_db="colombia_impactu")
    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.client=MongoClient(self.config.db_uri)
        self.colav_db=self.client[self.config.colav_db]
        self.impactu_db=self.client[self.config.impactu_db]

    def get_info(self,idx):
        document = self.colav_db['works'].find_one({"_id":ObjectId(idx)})
        if document:
            entry={"id":document["_id"],
                "title":document["titles"][0]["title"],
                "source":{},
                "year_published":document["year_published"],
                "language":"",
                "volume":"",
                "issue": "",
                "authors":[],
                "policies":{},
                "open_access_status": "",
                "citations_count":0,
                "external_ids":[],
                "external_urls":document["external_urls"]
            }
            if "language" in document.keys():
                entry["language"]=document["languages"][0] if len(document["languages"])>0 else ""
            if "bibliographic_info" in document.keys():
                if "volume" in document["bibliographic_info"].keys():
                    entry["volume"]=document["bibliographic_info"]["volume"]
                if "issue" in document["bibliographic_info"].keys():
                    entry["issue"]=document["bibliographic_info"]["issue"]
                if "open_access_status" in document["bibliographic_info"].keys():
                    entry["open_access_status"]=document["bibliographic_info"]["open_access_status"]
            index_list=[]
            if "citations_count" in document.keys():
                for cite in document["citations_count"]:
                    if cite["source"]=="scholar":
                        entry["citations_count"]=cite["count"]
                        break
                    elif cite["source"]=="openalex":
                        entry["citations_count"]=cite["count"]
            if "source" in document.keys():
                source=self.colav_db["sources"].find_one({"_id":document["source"]["id"]})
                entry_source={
                    "name":document["source"]["names"][0] if "names" in document["source"].keys() else document["source"]["name"],
                    "serials":{}
                }
                for serial in source["external_ids"]:
                    if not serial["source"] in entry_source["serials"].keys():
                        entry_source["serials"][serial["source"]]=serial["id"]
                entry["source"]=entry_source

            for author in document["authors"]:
                author_entry={}
                author_entry["name"]=author["full_name"]
                author_entry["id"]=author["id"]
                author_entry["affiliation"]={}
                group_name = ""
                group_id = ""
                inst_name=""
                inst_id=""
                if "affiliations" in author.keys():
                    if len(author["affiliations"])>0:
                        for aff in author["affiliations"]:
                            if "types" in aff.keys():
                                for typ in aff["types"]:
                                    if typ["type"]=="group":
                                        group_name=aff["names"][0]["name"]
                                        group_id=aff["id"]
                                    else:   
                                        inst_name=aff["names"][0]["name"]
                                        inst_id=aff["id"]  
                author_entry["affiliation"]={"institution":{"name":inst_name,"id":inst_id},
                                              "group":{"name":group_name,"id":group_id}}  

                entry["authors"].append(author_entry)
            
            for ext in document["external_ids"]:
                if ext["source"]=="doi":
                    entry["external_ids"].append({
                        "id":ext["id"],
                        "source":"doi",
                        "url":"https://doi.org/"+ext["id"]
                        
                    })
                if ext["source"]=="lens":
                    entry["external_ids"].append({
                        "id":ext["id"],
                        "source":"lens",
                        "url":"https://www.lens.org/lens/scholar/article/"+ext["id"]
                    })
                if ext["source"]=="scholar":
                    entry["external_ids"].append({
                        "id":ext["id"],
                        "source":"scholar",
                        "url":"https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q=info%3A"+ext["id"]+
                            "%3Ascholar.google.com"
                    })
                if ext["source"]=="minciencias":
                    entry["external_ids"].append({
                        "id":ext["id"],
                        "source":"minciencias",
                        "url":""
                    })   
            
            return {"data":entry,"filters":{}}
        else:
            return None
    
    @endpoint('/app/work', methods=['GET'])
    def app_person(self):
        section = self.request.args.get('section')
        idx = self.request.args.get('id')
        
        result = None

        if section=="info":
            result = self.get_info(idx)
        else:
            result=None

        if result:
            response = self.app.response_class(
            response=self.json.dumps(result,cls=JsonEncoder),
            status=200,
            mimetype='application/json'
            )
        else:
            response = self.app.response_class(
            response=self.json.dumps({},cls=JsonEncoder),
            status=204,
            mimetype='application/json'
            )
        
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response