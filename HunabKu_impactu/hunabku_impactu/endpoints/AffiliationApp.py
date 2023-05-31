from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from bson import ObjectId
from pymongo import MongoClient,ASCENDING,DESCENDING
from hunabku.Config import Config, Param
from hunabku_impactu.utils.encoder import JsonEncoder
from hunabku_impactu.utils.bars import bars
from hunabku_impactu.utils.pies import pies
from hunabku_impactu.utils.maps import maps
from math import nan


class AffiliationApp(HunabkuPluginBase):
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
        self.bars=bars()
        self.pies=pies()
        self.maps=maps()

    def get_info(self,idx,start_year=None,end_year=None):
        initial_year=9999
        final_year = 0

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

        affiliation = self.colav_db['affiliations'].find_one({"_id":ObjectId(idx)})
        if affiliation:
            name=""
            for n in affiliation["names"]:
                if n["lang"]=="es":
                    name=n["name"]
                    break
                elif n["lang"]=="en":
                    name=n["name"]
            logo=""
            for ext in affiliation["external_urls"]:
                if ext["source"]=="logo":
                    logo=ext["url"]

            entry={"id":affiliation["_id"],
                "name":name,
                "citations":affiliation["citations_count"] if "citations_count" in affiliation.keys() else None,
                "external_urls":[ext for ext in affiliation["external_urls"] if ext["source"]!="logo"],
                "logo":logo
            }
            index_list=[]
        
            filters={"years":{}}
            for reg in self.colav_db["works"].find({"authors.affiliations.id":ObjectId(idx),"year_published":{"$exists":1}}).sort([("year_published",ASCENDING)]).limit(1):
                filters["years"]["start_year"]=reg["year_published"]
            for reg in self.colav_db["works"].find({"authors.affiliations.id":ObjectId(idx),"year_published":{"$exists":1}}).sort([("year_published",DESCENDING)]).limit(1):
                filters["years"]["end_year"]=reg["year_published"]
            filters["types"]=[]
            
            return {"data": entry, "filters": filters }
        else:
            return None

    def get_affiliations(self,idx):
        data={"departments":[],"faculties":[],"groups":[]}

        for aff in self.colav_db['affiliations'].find({"relations.id":ObjectId(idx)},{"types":1,"names":1}):
            if aff["types"]:
                for typ in aff["types"]:
                    if typ["type"]=="group":
                        data["groups"].append({"id":aff["_id"],"name":aff["names"][0]["name"]})
                        break
                    elif typ["type"]=="department":
                        data["departments"].append({"id":aff["_id"],"name":aff["names"][0]["name"]})
                        break
                    elif typ["type"]=="faculty":
                        data["faculties"].append({"id":aff["_id"],"name":aff["names"][0]["name"]})
                        break

        return data
        
        
    def get_research_products(self,idx,typ=None,start_year=None,end_year=None,page=None,max_results=None,sort=None):
        papers=[]
        total=0
        open_access=[]
        
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
            search_dict={"authors.affiliations.id":ObjectId(idx)}     
        if start_year or end_year:
            search_dict["year_published"]={}
        if start_year:
            search_dict["year_published"]["$gte"]=start_year
        if end_year:
            search_dict["year_published"]["$lte"]=end_year
        if typ:
            if typ!="institution":
                search_dict["types.type"]=typ
        
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
                entry={
                    "id":paper["_id"],
                    "title":paper["titles"][0]["title"],
                    "authors":[],
                    "source":"",
                    "open_access_status":paper["bibliographic_info"]["open_access_status"] if "open_access_status" in paper["bibliographic_info"] else "",
                    "year_published":paper["year_published"],
                    "citations_count":paper["citations_count"] if "citations_count" in paper.keys() else 0,
                    "subjects":[]
                }

                for subs in paper["subjects"]:
                    if subs["source"]=="openalex":
                        for sub in subs["subjects"]:
                            name=sub["names"][0]["name"]
                            for n in sub["names"]:
                                if n["lang"]=="es":
                                    name=n["name"]
                                    break
                                if n["lang"]=="en":
                                    name=n["name"]
                            entry["subjects"].append({"name":name,"id":sub["id"]})
                        break

                if "source" in paper.keys():
                    if "id" in paper["source"].keys():
                        if "name" in paper["source"].keys():
                            entry["source"]={"name":paper["source"]["name"],"id":paper["source"]["id"]}
                        elif "names" in paper["source"].keys():
                            entry["source"]={"name":paper["source"]["names"][0]["name"],"id":paper["source"]["id"]}
                
                authors=[]
                for author in paper["authors"]:
                    au_entry=author.copy()
                    if not "affiliations" in au_entry.keys():
                        au_entry["affiliations"]=[]
                    author_db=None
                    if "id" in author.keys():
                        if author["id"]=="":
                            continue
                        author_db=self.colav_db["person"].find_one({"_id":author["id"]})
                    else:
                        continue
                    if author_db:
                        au_entry={
                            "id":author_db["_id"],
                            "full_name":author_db["full_name"],
                            "external_ids":[ext for ext in author_db["external_ids"] if not ext["source"] in ["Cédula de Ciudadanía","Cédula de Extranjería","Passport"]]
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
                                        "name":""
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
                                        "name":""
                                    }
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

    def get_products_by_year_by_type(self,idx,typ=None):
        data = []
        if typ in ["group","department","faculty"]:
            for author in self.colav_db["person"].find({"affiliations.id":ObjectId(idx)},{"affiliations":1}):
                for work in self.colav_db["works"].find({"authors.id":author["_id"],"year_published":{"$exists":1}},{"year_published":1,"types":1}):
                    data.append(work)
        else:
            for work in self.colav_db["works"].find({"authors.affiliations.id":ObjectId(idx),"year_published":{"$exists":1}},{"year_published":1,"types":1}):
                data.append(work)
        result=self.bars.products_by_year_by_type(data)
        if result:
            return {"plot":result}
        else:
            return {"plot":None}

    def get_products_by_affiliation_by_type(self,idx,typ):
        affiliations=[]
        aff_ids=[]
        if not typ in ["group","department","faculty"]:
            return None
        for aff in self.colav_db["affiliations"].find({"relations.id":ObjectId(idx),"types.type":typ}):
            name=aff["names"][0]["name"]
            for n in aff["names"]:
                if n["lang"]=="es":
                    name=n["name"]
                    break
                if n["lang"]=="en":
                    name=n["name"]
            affiliations.append((aff["_id"],name))
        data={}
        for aff_id,name in affiliations:
            data[name]=[]
            for author in self.colav_db["person"].find({"affiliations.id":aff_id}):
                aff_start_date=None
                aff_end_date=None
                for aff in author["affiliations"]:
                    if aff["id"]==aff_id:
                        aff_start_date=aff["start_date"] if aff["start_date"]!=-1 else 9999999999
                        aff_end_date=aff["end_date"] if aff["end_date"]!=-1 else 9999999999
                        break
                query_dict={
                    "authors.id":author["_id"],
                    "types":{"$ne":[]},
                    "$and":[{"date_published":{"$lte":aff_end_date}},{"date_published":{"$gte":aff_start_date}}]
                }
                
                for work in self.colav_db["works"].find(query_dict,{"types":1}):
                    data[name].append(work)

        return {"plot":self.bars.products_by_affiliation_by_type(data)}


    def get_citations_by_year(self,idx,typ=None):
        data = []
        if typ in ["group","department","faculty"]:
            for author in self.colav_db["person"].find({"affiliations.id":ObjectId(idx)},{"affiliations":1}):
                for work in self.colav_db["works"].find({"authors.id":author["_id"],"citations_by_year":{"$ne":[]},"year_published":{"$exists":1}},{"year_published":1,"citations_by_year":1}):
                    data.append(work)
        else:
            for work in self.colav_db["works"].find({"authors.affiliations.id":ObjectId(idx),"citations_by_year":{"$ne":[]},"year_published":{"$exists":1}},{"year_published":1,"citations_by_year":1}):
                data.append(work)
        result=self.bars.citations_by_year(data)
        if result:
            return {"plot":result}
        else:
            return {"plot":None}
        
    def get_apc_by_year(self,idx,typ=None):
        data = []
        if typ in ["group","department","faculty"]:
            for author in self.colav_db["person"].find({"affiliations.id":ObjectId(idx)},{"affiliations":1}):
                for work in self.colav_db["works"].find({"authors.id":author["_id"],"year_published":{"$exists":1},"source.id":{"$exists":1}},{"year_published":1,"source":1}):
                    if not "source" in work.keys():
                        continue
                    if not "id" in work["source"].keys():
                        continue
                    source_db=self.colav_db["sources"].find_one({"_id":work["source"]["id"]})
                    if source_db:
                        if source_db["apc"]:
                            data.append({"year_published":work["year_published"],"apc":source_db["apc"]})
        else:
            for work in self.colav_db["works"].find({"authors.affiliations.id":ObjectId(idx),"year_published":{"$exists":1},"source.id":{"$exists":1}},{"year_published":1,"source":1}):
                if not "source" in work.keys():
                    continue
                if not "id" in work["source"].keys():
                    continue
                source_db=self.colav_db["sources"].find_one({"_id":work["source"]["id"]})
                if source_db:
                    if source_db["apc"]:
                        data.append({"year_published":work["year_published"],"apc":source_db["apc"]})
        result=self.bars.apc_by_year(data,2022)
        if result:
            return {"plot":result}
        else:
            return {"plot":None}

    def get_oa_by_year(self,idx,typ=None):
        data=[]
        if typ in ["group","department","faculty"]:
            for author in self.colav_db["person"].find({"affiliations.id":ObjectId(idx)},{"affiliations":1}):
                for work in self.colav_db["works"].find(
                    {
                        "authors.id":author["_id"],
                        "year_published":{"$exists":1},
                        "bibliographic_info.is_open_acess":{"$exists":1}
                    },
                    {
                        "year_published":1,"bibliographic_info.is_open_acess":1
                    }
                ):
                    data.append(work)
        else:
            for work in self.colav_db["works"].find(
                {
                    "authors.affiliations.id":ObjectId(idx),
                    "year_published":{"$exists":1},
                    "bibliographic_info.is_open_acess":{"$exists":1}
                },
                {
                    "year_published":1,"bibliographic_info.is_open_acess":1
                }
            ):
                data.append(work)
        
        result=self.bars.oa_by_year(data)
        if result:
            return {"plot":result}
        else:
            return {"plot":None}

    def get_products_by_year_by_publisher(self,idx,typ=None):
        data=[]
        if typ in ["group","department","faculty"]:
            for author in self.colav_db["person"].find({"affiliations.id":ObjectId(idx)},{"affiliations":1}):
                for work in self.colav_db["works"].find(
                    {
                        "authors.id":author["_id"],
                        "year_published":{"$exists":1},
                        "source.id":{"$exists":1}
                    },
                    {
                        "year_published":1,"source.id":1
                    }
                ):
                    if not "source" in work.keys():
                        continue
                    if not "id" in work["source"].keys():
                        continue
                    source_db=self.colav_db["sources"].find_one({"_id":work["source"]["id"]})
                    if source_db:
                        if source_db["publisher"]:
                            data.append({"year_published":work["year_published"],"publisher":source_db["publisher"]})
        else:
            for work in self.colav_db["works"].find(
                {
                    "authors.affiliations.id":ObjectId(idx),
                    "year_published":{"$exists":1},
                    "source.id":{"$exists":1}
                },
                {
                    "year_published":1,"source.id":1
                }
            ):
                if not "source" in work.keys():
                    continue
                if not "id" in work["source"].keys():
                    continue
                source_db=self.colav_db["sources"].find_one({"_id":work["source"]["id"]})
                if source_db:
                    if source_db["publisher"]:
                        data.append({"year_published":work["year_published"],"publisher":source_db["publisher"]})
        
        result=self.bars.products_by_year_by_publisher(data)
        if result:
            return {"plot":result}
        else:
            return {"plot":None}

    def get_h_by_year(self,idx,typ=None):
        data = []
        if typ in ["group","department","faculty"]:
            for author in self.colav_db["person"].find({"affiliations.id":ObjectId(idx)},{"affiliations":1}):
                for work in self.colav_db["works"].find({"authors.id":author["_id"],"citations_by_year":{"$ne":[]}},{"citations_by_year":1}):
                    data.append(work)
        else:
            for work in self.colav_db["works"].find({"authors.affiliations.id":ObjectId(idx),"citations_by_year":{"$ne":[]}},{"citations_by_year":1}):
                data.append(work)
        result=self.bars.h_index_by_year(data)
        if result:
            return {"plot":result}
        else:
            return {"plot":None}

    def get_products_by_year_by_researcher_category(self,idx,typ=None):
        data=[]
        if typ in ["group","department","faculty"]:
            for author in self.colav_db["person"].find({"affiliations.id":ObjectId(idx)},{"affiliations":1}):
                pipeline=[
                    {"$match":{"authors.id":author["_id"]}},
                    {"$project":{"year_published":1,"authors":1}},
                    {"$unwind":"$authors"},
                    {"$lookup":{"from":"person","localField":"authors.id","foreignField":"_id","as":"researcher"}},
                    {"$project":{"year_published":1,"researcher.ranking":1,"researcher._id":1}},
                    {"$match":{"researcher.ranking.source":"scienti"}}
                ]
                for work in self.colav_db["works"].aggregate(pipeline):
                    for researcher in work["researcher"]:
                        for rank in researcher["ranking"]:
                            if rank["source"]=="scienti":
                                data.append({"year_published":work["year_published"],"rank":rank["rank"]})
        else:
            pipeline=[
                {"$match":{"authors.affiliations.id":ObjectId(idx)}},
                {"$project":{"year_published":1,"authors":1}},
                {"$unwind":"$authors"},
                {"$match":{"authors.affiliations.id":ObjectId(idx)}},
                {"$lookup":{"from":"person","localField":"authors.id","foreignField":"_id","as":"researcher"}},
                {"$project":{"year_published":1,"researcher.ranking":1}},
                {"$match":{"researcher.ranking.source":"scienti"}}
            ]
            for work in self.colav_db["works"].aggregate(pipeline):
                for researcher in work["researcher"]:
                    for rank in researcher["ranking"]:
                        if rank["source"]=="scienti":
                            data.append({"year_published":work["year_published"],"rank":rank["rank"]})
        result=self.bars.products_by_year_by_researcher_category(data)
        if result:
            return {"plot":result}
        else:
            return {"plot":None}

    def get_products_by_year_by_group_category(self,idx):
        data=[]
        info_db=self.colav_db["affiliations"].find_one({"_id":ObjectId(idx)},{"types":1,"relations":1,"ranking":1})
        db_type=""
        for typ in info_db["types"]:
            if typ["type"]=="group":
                db_type=typ["type"]
                break
            elif typ["type"]=="department":
                db_type=typ["type"]
                break
            elif typ["type"]=="faculty":
                db_type=typ["type"]
                break
            else:
                db_type="institution"
                break

        if db_type=="group":
            for work in self.colav_db["works"].find({"authors.affiliations.id":ObjectId(idx),"year_published":{"$exists":1}},{"year_published":1}):
                work["ranking"]=info_db["ranking"]
                data.append(work)
        else:
            groups=self.colav_db["affiliations"].find({"relations.id":ObjectId(idx),"types.type":"group"},{"_id":1,"ranking":1})
            for group in groups:
                authors=self.colav_db["person"].find({"affiliations.id":group["_id"]},{"affiliations":1})
                for author in authors:
                    aff_start_date=None
                    aff_end_date=None
                    for aff in author["affiliations"]:
                        if aff["id"]==group["_id"]:
                            aff_start_date=aff["start_date"] if aff["start_date"]!=-1 else 9999999999
                            aff_end_date=aff["end_date"] if aff["end_date"]!=-1 else 9999999999
                            break
                    query_dict={
                        "authors.id":author["_id"],
                        "ranking":{"$ne":[]},
                        "$and":[{"date_published":{"$lte":aff_end_date}},{"date_published":{"$gte":aff_start_date}}]
                    }
                    for work in self.colav_db["works"].find(query_dict,{"year_published":1,"date_published":1}):
                        work["ranking"]=group["ranking"]
                        data.append(work)
        #print(data)
        result=self.bars.products_by_year_by_group_category(data)
        return{"plot":result}

    def get_title_words(self,idx):
        data=self.impactu_db["affiliations"].find_one({"_id":ObjectId(idx)},{"top_words":1})
        if data:
            if not "top_words" in data.keys():
                return {"plot":None}
            data=data["top_words"]
            return {"plot":data}
        else:
            return {"plot":None}
    
    def get_citations_by_affiliations(self,idx,typ):
        affiliations=[]
        aff_ids=[]
        if not typ in ["group","department","faculty"]:
            return None
        for aff in self.colav_db["affiliations"].find({"relations.id":ObjectId(idx),"types.type":typ}):
            name=aff["names"][0]["name"]
            for n in aff["names"]:
                if n["lang"]=="es":
                    name=n["name"]
                    break
                if n["lang"]=="en":
                    name=n["name"]
            affiliations.append((aff["_id"],name))

        data={}
        for aff_id,name in affiliations:
            data[name]=[]
            for author in self.colav_db["person"].find({"affiliations.id":aff_id}):
                aff_start_date=None
                aff_end_date=None
                for aff in author["affiliations"]:
                    if aff["id"]==aff_id:
                        aff_start_date=aff["start_date"] if aff["start_date"]!=-1 else 9999999999
                        aff_end_date=aff["end_date"] if aff["end_date"]!=-1 else 9999999999
                        break
                query_dict={
                    "authors.id":author["_id"],
                    "citations_count":{"$ne":[]},
                    "$and":[{"date_published":{"$lte":aff_end_date}},{"date_published":{"$gte":aff_start_date}}]
                }
                
                for work in self.colav_db["works"].find(query_dict,{"citations_count":1}):
                    data[name].append(work)

        return {"plot":self.pies.citations_by_affiliation(data)}

    def get_products_by_affiliations(self,idx,typ):
        affiliations=[]
        aff_ids=[]
        if not typ in ["group","department","faculty"]:
            return None
        for aff in self.colav_db["affiliations"].find({"relations.id":ObjectId(idx),"types.type":typ}):
            name=aff["names"][0]["name"]
            for n in aff["names"]:
                if n["lang"]=="es":
                    name=n["name"]
                    break
                if n["lang"]=="en":
                    name=n["name"]
            affiliations.append((aff["_id"],name))

        data={}
        for aff_id,name in affiliations:
            data[name]=0
            for author in self.colav_db["person"].find({"affiliations.id":aff_id}):
                aff_start_date=None
                aff_end_date=None
                for aff in author["affiliations"]:
                    if aff["id"]==aff_id:
                        aff_start_date=aff["start_date"] if aff["start_date"]!=-1 else 9999999999
                        aff_end_date=aff["end_date"] if aff["end_date"]!=-1 else 9999999999
                        break
                query_dict={
                    "authors.id":author["_id"],
                    "$and":[{"date_published":{"$lte":aff_end_date}},{"date_published":{"$gte":aff_start_date}}]
                }
                
                data[name]+=self.colav_db["works"].count_documents(query_dict)
                    
        return {"plot":self.pies.products_by_affiliation(data)}

    def get_apc_by_affiliations(self,idx,typ):
        affiliations=[]
        aff_ids=[]
        if not typ in ["group","department","faculty"]:
            return None
        for aff in self.colav_db["affiliations"].find({"relations.id":ObjectId(idx),"types.type":typ}):
            name=aff["names"][0]["name"]
            for n in aff["names"]:
                if n["lang"]=="es":
                    name=n["name"]
                    break
                if n["lang"]=="en":
                    name=n["name"]
            affiliations.append((aff["_id"],name))

        data={}
        for aff_id,name in affiliations:
            data[name]=[]
            for author in self.colav_db["person"].find({"affiliations.id":aff_id}):
                aff_start_date=None
                aff_end_date=None
                for aff in author["affiliations"]:
                    if aff["id"]==aff_id:
                        aff_start_date=aff["start_date"] if aff["start_date"]!=-1 else 9999999999
                        aff_end_date=aff["end_date"] if aff["end_date"]!=-1 else 9999999999
                        break
                query_dict={
                    "authors.id":author["_id"],
                    "source":{"$ne":[]},
                    "$and":[{"date_published":{"$lte":aff_end_date}},{"date_published":{"$gte":aff_start_date}}]
                }
                
                for work in self.colav_db["works"].find(query_dict,{"source":1,"year_published":1}):
                    if not "id" in work["source"].keys():
                        continue
                    source_db=self.colav_db["sources"].find_one({"_id":work["source"]["id"]})
                    if source_db:
                        if source_db["apc"]:
                            source_db["apc"]["year_published"]=work["year_published"]
                            data[name].append(source_db["apc"])

        return {"plot":self.pies.apc_by_affiliation(data,2022)}

    def get_h_by_affiliations(self,idx,typ):
        affiliations=[]
        aff_ids=[]
        if not typ in ["group","department","faculty"]:
            return None
        for aff in self.colav_db["affiliations"].find({"relations.id":ObjectId(idx),"types.type":typ}):
            name=aff["names"][0]["name"]
            for n in aff["names"]:
                if n["lang"]=="es":
                    name=n["name"]
                    break
                if n["lang"]=="en":
                    name=n["name"]
            affiliations.append((aff["_id"],name))
        
        data={}
        for aff_id,name in affiliations:
            data[name]=[]
            for author in self.colav_db["person"].find({"affiliations.id":aff_id}):
                aff_start_date=None
                aff_end_date=None
                for aff in author["affiliations"]:
                    if aff["id"]==aff_id:
                        aff_start_date=aff["start_date"] if aff["start_date"]!=-1 else 9999999999
                        aff_end_date=aff["end_date"] if aff["end_date"]!=-1 else 9999999999
                        break
                query_dict={
                    "authors.id":author["_id"],
                    "citation_count":{"$ne":[]},
                    "$and":[{"date_published":{"$lte":aff_end_date}},{"date_published":{"$gte":aff_start_date}}]
                }
                
                for work in self.colav_db["works"].find(query_dict,{"citations_count":1}):
                    citations=0
                    for count in work["citations_count"]:
                        if count["source"]=="scholar":
                            citations=count["count"]
                            break
                        elif count["source"]=="openalex":
                            citations=count["count"]
                            break
                    if citations==0:
                        continue
                    data[name].append(citations)
                    
        return {"plot":self.pies.hindex_by_affiliation(data)}

    def get_products_by_publisher(self,idx,typ=None):
        data=[]
        if typ in ["group","department","faculty"]:
            for author in self.colav_db["person"].find({"affiliations.id":ObjectId(idx)},{"affiliations":1}):
                for work in self.colav_db["works"].find(
                    {
                        "authors.id":author["_id"],
                        "source.id":{"$exists":1}
                    },{"source.id":1}
                ):
                    if not "source" in work.keys():
                        continue
                    if not "id" in work["source"].keys():
                        continue
                    source_db=self.colav_db["sources"].find_one({"_id":work["source"]["id"],"publisher.name":{"$ne":nan}})
                    if source_db:
                        if source_db["publisher"]:
                            data.append({"publisher":source_db["publisher"]})
        else:
            for work in self.colav_db["works"].find(
                {
                    "authors.affiliations.id":ObjectId(idx),
                    "source.id":{"$exists":1}
                },{"source.id":1}
            ):
                if not "source" in work.keys():
                    continue
                if not "id" in work["source"].keys():
                    continue
                source_db=self.colav_db["sources"].find_one({"_id":work["source"]["id"],"publisher.name":{"$ne":nan}})
                if source_db:
                    if source_db["publisher"]:
                        data.append({"publisher":source_db["publisher"]})
        
        result=self.pies.products_by_publisher(data)
        if result:
            return {"plot":result}
        else:
            return {"plot":None}
    
    def get_products_by_subject(self,idx,level=0):
        if not level:
            level=0
        data=[]
        for work in self.colav_db["works"].find(
            {
                "authors.affiliations.id":ObjectId(idx),
                "subjects":{"$exists":1}
            },{"subjects":1}
        ):
            if not "subjects" in work.keys():
                continue
            for subjects in work["subjects"]:
                if subjects["source"]!="openalex":
                    continue
                for subject in subjects["subjects"]:
                    if subject["level"]!=level:
                        continue
                    name=subject["names"][0]["name"]
                    for n in subject["names"]:
                        if n["lang"]=="es":
                            name=n["name"]
                            break
                        elif n["lang"]=="en":
                            name=n["name"]
                    data.append({"subject":{"name":name}})
        
        result=self.pies.products_by_subject(data)
        return {"plot":result}

    def get_products_by_database(self,idx):
        data=[]
        for work in self.colav_db["works"].find(
            {
                "authors.affiliations.id":ObjectId(idx)
            },{"updated":1}
        ):
            data.append(work["updated"])
        
        result=self.pies.products_by_database(data)
        return {"plot":result}

    def get_products_by_open_access_status(self,idx):
        data=[]
        for work in self.colav_db["works"].find(
            {
                "authors.affiliations.id":ObjectId(idx),
                "bibliographic_info.open_access_status":{"$exists":1,"$ne":None}
            },{"bibliographic_info.open_access_status":1}
        ):
            data.append(work["bibliographic_info"]["open_access_status"])
        
        result=self.pies.products_by_open_access_status(data)
        return {"plot":result,"openSum":sum([oa["value"] for oa in result if oa["name"]!="closed"])}
    
    def get_products_by_author_sex(self,idx):
        data=[]
        pipeline=[
            {"$match":{"authors.affiliations.id":ObjectId(idx)}},
            {"$project":{"authors":1}},
            {"$unwind":"$authors"},
            {"$lookup":{"from":"person","localField":"authors.id","foreignField":"_id","as":"author"}},
            {"$project":{"author.sex":1}},
            {"$match":{"author.sex":{"$ne":"","$exists":1}}}
        ]
        for work in self.colav_db["works"].aggregate(pipeline):
            data.append(work)
        result=self.pies.products_by_sex(data)
        return {"plot":result}

    def get_products_by_author_age(self,idx):
        data=[]
        pipeline=[
            {"$match":{"authors.affiliations.id":ObjectId(idx)}},
            {"$project":{"authors":1,"date_published":1,"year_published":1}},
            {"$unwind":"$authors"},
            {"$match":{"authors.affiliations.id":ObjectId(idx)}},
            {"$lookup":{"from":"person","localField":"authors.id","foreignField":"_id","as":"author"}},
            {"$project":{"author.birthdate":1,"date_published":1,"year_published":1}},
            {"$match":{"author.birthdate":{"$ne":-1,"$exists":1}}}
        ]
        for work in self.colav_db["works"].aggregate(pipeline):
            data.append(work)
        result=self.pies.products_by_age(data)
        return {"plot":result}

    def get_products_by_scienti_rank(self,idx):
        data=[]
        for work in self.colav_db["works"].find({"authors.affiliations.id":ObjectId(idx),"ranking":{"$ne":[]}},{"ranking":1}):
            data.append(work)
        result=self.pies.products_by_scienti_rank(data)
        return {"plot":result}

    def get_products_by_scimago_rank(self,idx):
        data=[]
        pipeline=[
            {"$match":{"authors.affiliations.id":ObjectId(idx)}},
            {"$project":{"source":1,"date_published":1}},
            {"$lookup":{"from":"sources","localField":"source.id","foreignField":"_id","as":"source"}},
            {"$unwind":"$source"},
            {"$project":{"source.ranking":1,"date_published":1}}
        ]
        for work in self.colav_db["works"].aggregate(pipeline):
            data.append(work)
        result=self.pies.products_by_scimago_rank(data)
        return {"plot":result}

    def get_publisher_same_institution(self,idx):
        data=[]
        institution=self.colav_db["affiliations"].find_one({"_id":ObjectId(idx)},{"names":1})
        pipeline=[
            {"$match":{"authors.affiliations.id":ObjectId(idx)}},
            {"$project":{"source":1}},
            {"$lookup":{"from":"sources","localField":"source.id","foreignField":"_id","as":"source"}},
            {"$unwind":"$source"},
            {"$project":{"source.publisher":1}},
            {"$match":{"source.publisher":{"$ne":nan,"$exists":1,"$ne":""},"source.publisher.name":{"$ne":nan}}}
        ]
        for work in self.colav_db["works"].aggregate(pipeline):
            data.append(work)
        result=self.pies.products_editorial_same_institution(data,institution)
        return {"plot":result}

    def get_coauthorships_worldmap(self,idx):
        data=[]
        pipeline=[
            {"$match":{"authors.affiliations.id":ObjectId(idx)}},
            {"$unwind":"$authors"},
            {"$group":{"_id":"$authors.affiliations.id","count":{"$sum":1}}},
            {"$unwind":"$_id"},
            {"$lookup":{"from":"affiliations","localField":"_id","foreignField":"_id","as":"affiliation"}},
            {"$project":{"count":1,"affiliation.addresses.country_code":1,"affiliation.addresses.country":1}},
            {"$unwind":"$affiliation"},
            {"$unwind":"$affiliation.addresses"}
        ]
        for work in self.colav_db["works"].aggregate(pipeline):
            data.append(work)
        result=self.maps.get_coauthorship_world_map(data)
        return {"plot":result}
    
    def get_coauthorships_colombiamap(self,idx):
        data=[]
        pipeline=[
            {"$match":{"authors.affiliations.id":ObjectId(idx)}},
            {"$unwind":"$authors"},
            {"$group":{"_id":"$authors.affiliations.id","count":{"$sum":1}}},
            {"$unwind":"$_id"},
            {"$lookup":{"from":"affiliations","localField":"_id","foreignField":"_id","as":"affiliation"}},
            {"$project":{"count":1,"affiliation.addresses.country_code":1,"affiliation.addresses.city":1}},
            {"$unwind":"$affiliation"},
            {"$unwind":"$affiliation.addresses"}
        ]
        for work in self.colav_db["works"].aggregate(pipeline):
            data.append(work)
        result=self.maps.get_coauthorship_colombia_map(data)
        return {"plot":result}

    def get_coauthorships_network(self, idx):
        data=self.impactu_db["affiliations"].find_one({"_id":ObjectId(idx)},{"coauthorship_network":1})
        if data:
            if "coauthorship_network" not in data.keys():
                return {"plot":None}
            data=data["coauthorship_network"]
            nodes=sorted(data["nodes"],key=lambda x:x["degree"],reverse=True)[:50]
            nodes_ids=[node["id"] for node in nodes]
            edges=[]
            for edge in data["edges"]:
                if edge["source"] in nodes_ids and edge["target"] in nodes_ids:
                    edges.append(edge)
            return {"plot":{"nodes":nodes,"edges":edges}}
        else:
            return {"plot":None}
    

    @endpoint('/app/affiliation', methods=['GET'])
    def app_affiliation(self):
        section = self.request.args.get('section')
        tab = self.request.args.get('tab')
        data = self.request.args.get('data')
        idx = self.request.args.get('id')
        typ = self.request.args.get('type')
        
        result = None

        if section=="info":
            result = self.get_info(idx)
        elif section=="affiliations":
            result = self.get_affiliations(idx)
        elif section=="research":
            if tab=="products":
                plot=self.request.args.get("plot")
                if plot:
                    if plot=="year_type":
                        result=self.get_products_by_year_by_type(idx,typ=typ)
                    if plot=="faculty_type":
                        result=self.get_products_by_affiliation_by_type(idx,typ="faculty")
                    if plot=="department_type":
                        result=self.get_products_by_affiliation_by_type(idx,typ="department")
                    if plot=="group_type":
                        result=self.get_products_by_affiliation_by_type(idx,typ="group")
                    elif plot=="year_citations":
                        result=self.get_citations_by_year(idx,typ=typ)
                    elif plot=="year_apc":
                        result=self.get_apc_by_year(idx,typ=typ)
                    elif plot=="year_oa":
                        result=self.get_oa_by_year(idx,typ=typ)
                    elif plot=="year_publisher":
                        result=self.get_products_by_year_by_publisher(idx,typ=typ)
                    elif plot=="year_h":
                        result=self.get_h_by_year(idx,typ=typ)
                    elif plot=="year_researcher":
                        result=self.get_products_by_year_by_researcher_category(idx,typ=typ)
                    elif plot=="year_group":
                        result=self.get_products_by_year_by_group_category(idx)
                    elif plot=="title_words":
                        result=self.get_title_words(idx)
                    elif plot=="citations_faculty":
                        result = self.get_citations_by_affiliations(idx,typ="faculty")
                    elif plot=="citations_department":
                        result = self.get_citations_by_affiliations(idx,typ="department")
                    elif plot=="citations_group":
                        result = self.get_citations_by_affiliations(idx,typ="group")
                    elif plot=="products_faculty":
                        result = self.get_products_by_affiliations(idx,typ="faculty")
                    elif plot=="products_department":
                        result = self.get_products_by_affiliations(idx,typ="department")
                    elif plot=="products_group":
                        result = self.get_products_by_affiliations(idx,typ="group")
                    elif plot=="apc_faculty":
                        result = self.get_apc_by_affiliations(idx,typ="faculty")
                    elif plot=="apc_department":
                        result = self.get_apc_by_affiliations(idx,typ="department")
                    elif plot=="apc_group":
                        result = self.get_apc_by_affiliations(idx,typ="group")
                    elif plot=="h_faculty":
                        result = self.get_h_by_affiliations(idx,typ="faculty")
                    elif plot=="h_department":
                        result = self.get_h_by_affiliations(idx,typ="department")
                    elif plot=="h_group":
                        result = self.get_h_by_affiliations(idx,typ="group")
                    elif plot=="products_publisher":
                        result=self.get_products_by_publisher(idx,typ=typ)
                    elif plot=="products_subject":
                        level=self.request.args.get('level')
                        if not level:
                            level=0
                        result=self.get_products_by_subject(idx,level)
                    elif plot=="products_database":
                        result=self.get_products_by_database(idx)
                    elif plot=="products_oa":
                        result=self.get_products_by_open_access_status(idx)
                    elif plot=="products_sex":
                        result=self.get_products_by_author_sex(idx)
                    elif plot=="products_age":
                        result=self.get_products_by_author_age(idx)
                    elif plot=="scienti_rank":
                        result=self.get_products_by_scienti_rank(idx)
                    elif plot=="scimago_rank":
                        result=self.get_products_by_scimago_rank(idx)
                    elif plot=="published_institution":
                        result=self.get_publisher_same_institution(idx)
                    elif plot=="collaboration_worldmap":
                        result=self.get_coauthorships_worldmap(idx)
                    elif plot=="collaboration_colombiamap":
                        result=self.get_coauthorships_colombiamap(idx)
                    elif plot=="collaboration_network":
                        result=self.get_coauthorships_network(idx)
                    
                else:
                    idx = self.request.args.get('id')
                    typ = self.request.args.get('type')
                    start_year = self.request.args.get('start_year')
                    endt_year = self.request.args.get('end_year')
                    page = self.request.args.get('page')
                    max_results = self.request.args.get('max_results')
                    sort = self.request.args.get('sort')
                    result = self.get_research_products(
                        idx=idx,
                        typ=typ,
                        start_year=start_year,
                        end_year=endt_year,
                        page=page,
                        max_results=max_results,
                        sort=sort
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