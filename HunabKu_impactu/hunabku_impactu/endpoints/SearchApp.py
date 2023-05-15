from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from bson import ObjectId
from pymongo import MongoClient,ASCENDING,DESCENDING
from hunabku.Config import Config, Param
from hunabku_impactu.utils.encoder import JsonEncoder


class SearchApp(HunabkuPluginBase):
    config=Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config+=Param(colav_db="colombia_udea")
    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.client=MongoClient(self.config.db_uri)
        self.colav_db=self.client[self.config.colav_db]

    def search_subjects(self,keywords='',max_results=100,page=1,sort="products",direction="descending"):
        search_dict={}
        var_dict={"names":1}
        if keywords:
            search_dict["$text"]={"$search":keywords}

        var_dict["score"]={"$meta":"textScore"}
        cursor=self.colav_db["subjects"].find(search_dict,var_dict)

        total=self.colav_db["subjects"].count_documents(search_dict)

        '''if sort=="citations":
            cursor.sort([("citations_count",DESCENDING)])
        else:
            cursor.sort([("products_count",DESCENDING)])'''

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

        cursor=cursor.skip(max_results*(page-1)).limit(max_results)

        if cursor:
            subjects_list=[]
            for subject in cursor:
                name=""
                for n in subject["names"]:
                    if n["lang"]=="es":
                        name=n["name"]
                        break
                    elif n["lang"]=="en":
                        name=n["name"]
                entry={
                    "name":name,
                    "id":subject["_id"],
                    #"products_count":subject["products_count"],
                    #"citations_count":subject["citations_count"]
                }
                subjects_list.append(entry)

            return {
                    "total_results":total,
                    "count":len(subjects_list),
                    "page":page,
                    "filters":{},
                    "data":subjects_list
                }

        else:
            return None

    def search_person(self,keywords="",institutions="",groups="",country="",max_results=100,page=1,sort="citations"):
        search_dict={"external_ids":{"$ne":[]}}
        var_dict={"full_name":1,"external_ids":1,"affiliations":1,"products_count":1,"citations_count":1}
        aff_list=[]
        if institutions:
            aff_list.extend([ObjectId(inst) for inst in institutions.split()])
        if groups:
            aff_list.extend([ObjectId(grp) for grp in groups.split()])
        if len(aff_list)!=0:
            search_dict["affiliations.id"]={"$in":aff_list}

        if keywords:
            search_dict["$text"]={"$search":keywords}
            filter_cursor=self.colav_db['person'].find({"$text":{"$search":keywords},"external_ids":{"$ne":[]}},{ "score": { "$meta": "textScore" } }).sort([("score", { "$meta": "textScore" } )])
        else:
            filter_cursor=self.colav_db['person'].find({"external_ids":{"$ne":[]}})

        var_dict["score"]={"$meta":"textScore"}

        cursor=self.colav_db['person'].find(search_dict,var_dict)

        institution_filters = []
        group_filters=[]
        institution_ids=[]
        groups_ids=[]

        for author in filter_cursor:
            if "affiliations" in author.keys():
                if len(author["affiliations"])>0:
                    for aff in author["affiliations"]:
                        if "types" in aff.keys():
                            for typ in aff["types"]: 
                                if typ["type"]=="group":
                                    if not str(aff["id"]) in groups_ids:
                                        groups_ids.append(str(aff["id"]))
                                        group_filters.append({
                                            "id":str(aff["id"]),
                                            "name":aff["name"]
                                        })
                                else:
                                    if not str(aff["id"]) in institution_ids:
                                        institution_ids.append(str(aff["id"]))
                                        entry = {"id":str(aff["id"]),"name":aff["name"]}
                                        institution_filters.append(entry)


        
        cursor.sort([("score", { "$meta": "textScore" } )])


        total=self.colav_db["person"].count_documents(search_dict)
        if not page:
            page=1
        else:
            try:
                page=int(page)
            except:
                #print("Could not convert end page to int")
                return None
        if not max_results:
            max_results=100
        else:
            try:
                max_results=int(max_results)
            except:
                #print("Could not convert end max to int")
                return None

        cursor=cursor.skip(max_results*(page-1)).limit(max_results)

        if cursor:
            author_list=[]
            keywords=[]
            group_name = ""
            group_id = ""
            for author in cursor:
                del(author["score"])
                ext_ids=[]
                for ext in author["external_ids"]:
                    if ext["source"] in ["Cédula de Ciudadanía","Cédula de Extranjería","Passport"]:
                        continue
                    ext_ids.append(ext)
                author["external_ids"]=ext_ids
                author_list.append(author)
    
            return {
                    "total_results":total,
                    "count":len(author_list),
                    "page":page,
                    #"filters":{"institutions":institution_filters,"groups":group_filters},
                    "data":author_list
                }
        else:
            return None

    def search_affiliations(self,keywords="",max_results=100,page=1,sort='citations',aff_type=None):
        search_dict={}
        var_dict={"names":1,"relations":1,"addresses":1,"external_ids":1,"external_urls":1,"types":1,"relations":1}
        if aff_type:
            if aff_type=="institution":
                search_dict={"types.type":"Education"}
            else:
                search_dict={"types.type":aff_type}
        if keywords:
            search_dict["$text"]={"$search":keywords}

        var_dict["score"]={"$meta":"textScore"}
        cursor=self.colav_db['affiliations'].find(search_dict,var_dict)
        
        cursor.sort([("score", { "$meta": "textScore" } )])

        total=self.colav_db['affiliations'].count_documents(search_dict)

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

        cursor=cursor.skip(max_results*(page-1)).limit(max_results)
        if cursor:
            affiliation_list=[]
            for affiliation in cursor:
                entry=affiliation.copy()
                del(entry["names"])
                del(entry["relations"])
                entry["relations"]=[]
                name=affiliation["names"][0]["name"]
                for n in affiliation["names"]:
                    if n["lang"]=="es":
                        name=n["name"]
                        break
                    if n["lang"]=="en":
                        name=n["name"]

                entry["name"]=name
                entry["logo"]=""
                for ext in affiliation["external_urls"]:
                    if ext["source"]=="logo":
                        entry["logo"]=ext["url"]
                

                affiliation_list.append(entry)
    
            return {
                    "total_results":total,
                    "count":len(affiliation_list),
                    "page":page,
                    "data":affiliation_list

                }
        else:
            return None

    @endpoint('/app/search', methods=['GET'])
    def app_search(self):
        data = self.request.args.get('data')
        
        if data=="person":
            max_results=self.request.args.get('max') if 'max' in self.request.args else 100
            page=self.request.args.get('page') if 'page' in self.request.args else 1
            keywords = self.request.args.get('keywords') if "keywords" in self.request.args else ""
            sort = self.request.args.get('sort') if "sort" in self.request.args else "citations"
            groups = self.request.args.get('groups') if "groups" in self.request.args else None
            institutions = self.request.args.get('institutions') if "institutions" in self.request.args else None
            result=self.search_person(keywords=keywords,max_results=max_results,page=page,sort=sort,
                groups=groups,institutions=institutions)
        elif data=="affiliations":
            max_results=self.request.args.get('max') if 'max' in self.request.args else 100
            page=self.request.args.get('page') if 'page' in self.request.args else 1
            keywords = self.request.args.get('keywords') if "keywords" in self.request.args else ""
            sort = self.request.args.get('sort') if "sort" in self.request.args else "citations"
            aff_type = self.request.args.get('type')
            if aff_type:
                result=self.search_affiliations(keywords=keywords,aff_type=aff_type,max_results=max_results,page=page,sort=sort)
            else:
                result=self.search_affiliations(keywords=keywords,max_results=max_results,page=page,sort=sort)
        elif data=="subjects":
            max_results=self.request.args.get('max') if 'max' in self.request.args else 100
            page=self.request.args.get('page') if 'page' in self.request.args else 1
            keywords = self.request.args.get('keywords') if "keywords" in self.request.args else ""
            sort=self.request.args.get('sort')
            result=self.search_subjects(keywords=keywords,max_results=max_results,
                page=page,sort=sort,direction="descending")
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
