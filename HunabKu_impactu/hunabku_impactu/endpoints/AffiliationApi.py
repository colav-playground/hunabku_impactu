from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from bson import ObjectId
from pymongo import MongoClient,ASCENDING,DESCENDING
from hunabku.Config import Config, Param
from hunabku_impactu.utils.encoder import JsonEncoder

class AffiliationApi(HunabkuPluginBase):
    config=Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config+=Param(colav_db="colombia_udea")
    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.client=MongoClient(self.config.db_uri)
        self.colav_db=self.client[self.config.colav_db]

    def get_production(self,idx=None,max_results=100,page=1,start_year=None,end_year=None,sort=None,direction=None):
        total = 0
        papers=[]
        if start_year:
            try:
                start_year=int(start_year)
            except:
                print("Could not convert start year to int")
                return None
        if end_year:
            try:
                end_year=int(end_year)
            except:
                print("Could not convert end year to int")
                return None

        search_dict={}

        if idx:
            search_dict["authors.affiliations.id"]=ObjectId(idx)
                
        if start_year or end_year:
            search_dict["year_published"]={}
        if start_year:
            search_dict["year_published"]["$gte"]=start_year
        if end_year:
            search_dict["year_published"]["$lte"]=end_year
        
        cursor=self.colav_db["works"].find(search_dict)
        total=self.colav_db["works"].count_documents(search_dict)

        if not page:
            page=1
        else:
            try:
                page=int(page)
            except:
                print("Could not convert end page to int")
                return None
        if not max_results:
            max_results=100
        else:
            try:
                max_results=int(max_results)
            except:
                print("Could not convert end max to int")
                return None
        if max_results>250:
            max_results=250
        
        if sort=="citations" and direction=="ascending":
            cursor.sort([("citations_count.count",ASCENDING)])
        if sort=="citations" and direction=="descending":
            cursor.sort([("citations_count.count",DESCENDING)])
        if sort=="year" and direction=="ascending":
            cursor.sort([("year_published",ASCENDING)])
        if sort=="year" and direction=="descending":
            cursor.sort([("year_published",DESCENDING)])

        cursor=cursor.skip(max_results*(page-1)).limit(max_results)

        if cursor:
            for paper in cursor:
                entry=paper.copy()
                subjects=[]
                for subject in entry["subjects"]:
                    sub_entry=subject.copy()
                    sub_entry["subjects"]=[]
                    for sub in subject["subjects"]:
                        name=None
                        lang=None
                        for n in sub["names"]:
                            if n["lang"]=="en":
                                name=n["name"]
                                lang=n["lang"]
                                break
                        
                        sub["names"]=[{"name":name,"lang":lang}]
                        sub_entry["subjects"].append(sub)
                    subjects.append(sub_entry)
                source=None
                if "id" in paper["source"].keys():
                    source=self.colav_db["sources"].find_one({"_id":paper["source"]["id"]})
                if source:
                    entry["source"]={
                        "id":source["_id"],
                        "names":source["names"],
                        "external_ids":source["external_ids"],
                        "ranking":source["ranking"],
                        "licenses":source["licenses"],
                        "apc":source["apc"],
                        "waiver":source["waiver"],
                        "addresses":source["addresses"]
                    }
                else:
                    entry["source"]={}
                authors=[]
                for author in paper["authors"]:
                    au_entry=author.copy()
                    if not "affiliations" in au_entry.keys():
                        au_entry["affiliations"]=[]
                    author_db=None
                    if "id" in author.keys():
                        author_db=self.colav_db["person"].find_one({"_id":author["id"]})
                    if author_db:
                        au_entry={
                            "id":author_db["_id"],
                            "full_name":author_db["full_name"],
                            "first_names":author_db["first_names"],
                            "last_names":author_db["last_names"],
                            "external_ids":[ext for ext in author_db["external_ids"] if not ext["source"] in ["Cédula de Ciudadanía","Cédula de Extranjería","Passport"]],
                            "ranking":author_db["ranking"],
                            "sex":author_db["sex"]
                        }
                    affiliations=[]
                    aff_ids=[]
                    aff_types=[]
                    for aff in author["affiliations"]:
                        if "id" in aff.keys():
                            if aff["id"]:
                                aff_db=self.colav_db["affiliations"].find_one({"_id":aff["id"]})
                                if aff_db:
                                    aff_ids.append(aff["id"])
                                    aff_entry={
                                        "id":aff_db["_id"],
                                        "name":"",
                                        "external_ids":aff_db["external_ids"],
                                        "types":aff_db["types"],
                                        "addresses":aff_db["addresses"],
                                        "start_date":None,
                                        "end_date":None
                                    }
                                    if author_db:
                                        for aff_au in author_db["affiliations"]:
                                            if aff_au["id"]==aff["id"]:
                                                if "start_date" in aff_au.keys():
                                                    aff_entry["start_date"]=aff_au["start_date"]
                                                if "end_date" in aff_au.keys():
                                                    aff_entry["end_date"]=aff_au["end_date"]
                                                break
                                    name=aff_db["names"][0]["name"]
                                    lang=""
                                    for n in aff_db["names"]:
                                        if "lang" in n.keys():
                                            if n["lang"]=="es":
                                                name=n["name"]
                                                lang=n["lang"]
                                                break
                                            elif n["lang"]=="en":
                                                name=n["name"]
                                                lang=n["lang"]
                                    del(aff["names"])
                                    aff["name"]=name
                                    if "types" in aff.keys():
                                        for typ in aff["types"]:
                                            if "type" in typ.keys():
                                                if not typ["type"] in aff_types:
                                                    aff_types.append(typ["type"])
                                    affiliations.append(aff)
                    if author_db:
                        for aff in author_db["affiliations"]:
                            if aff["id"] in aff_ids:
                                continue
                            if aff["id"]:
                                aff_db=self.colav_db["affiliations"].find_one({"_id":aff["id"]})
                                inst_already=False
                                if aff_db:
                                    if "types" in aff_db.keys():
                                        for typ in aff_db["types"]:
                                            if "type" in typ.keys():
                                                if typ["type"] in aff_types:
                                                    inst_already=True
                                    if inst_already:
                                        continue
                                    aff_ids.append(aff["id"])
                                    aff_entry={
                                        "id":aff_db["_id"],
                                        "name":"",
                                        "external_ids":aff_db["external_ids"],
                                        "types":aff_db["types"],
                                        "addresses":aff_db["addresses"],
                                        "start_date":None,
                                        "end_date":None
                                    }
                                    if author_db:
                                        for aff_au in author_db["affiliations"]:
                                            if aff_au["id"]==aff["id"]:
                                                if "start_date" in aff_au.keys():
                                                    aff_entry["start_date"]=aff_au["start_date"]
                                                if "end_date" in aff_au.keys():
                                                    aff_entry["end_date"]=aff_au["end_date"]
                                                break
                                    name=aff_db["names"][0]["name"]
                                    lang=""
                                    for n in aff_db["names"]:
                                        if "lang" in n.keys():
                                            if n["lang"]=="es":
                                                name=n["name"]
                                                lang=n["lang"]
                                                break
                                            elif n["lang"]=="en":
                                                name=n["name"]
                                                lang=n["lang"]
                                    aff["name"]=name
                                    affiliations.append(aff)
                    au_entry["affiliations"]=affiliations
                    authors.append(au_entry)
                entry["authors"]=authors
                papers.append(entry)

            return {"data":papers,
                    "count":len(papers),
                    "page":page,
                    "total_results":total
                }
        else:
            return None

    def get_info(self):
        pass

    @endpoint('/api/affiliation', methods=['GET'])
    def api_affiliation(self):
        data = self.request.args.get('data')

        if data=="info":
            idx = self.request.args.get('id')
            result = self.get_info(idx)
        elif data=="production":
            idx = self.request.args.get('id')
            max_results=self.request.args.get('max')
            page=self.request.args.get('page')
            start_year=self.request.args.get('start_year')
            end_year=self.request.args.get('end_year')
            sort=self.request.args.get('sort')
            result=self.get_production(
                idx=idx,
                max_results=max_results,
                page=page,
                start_year=start_year,
                end_year=end_year,
                sort=sort,
                direction="ascending"
            )
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