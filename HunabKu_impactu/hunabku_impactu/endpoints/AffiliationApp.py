from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from bson import ObjectId
from pymongo import MongoClient,ASCENDING,DESCENDING
from hunabku.Config import Config, Param
from hunabku_impactu.utils.encoder import JsonEncoder
from hunabku_impactu.utils.bars import bars



class AffiliationApp(HunabkuPluginBase):
    config=Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config+=Param(colav_db="colombia_udea")
    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.client=MongoClient(self.config.db_uri)
        self.colav_db=self.client[self.config.colav_db]
        self.bars=bars()

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
            
            return {"data": entry, "filters": filters }
        else:
            return None

    def get_products_by_year_by_type(self,idx):
        data = []
        for work in self.colav_db["works"].find({"authors.affiliations.id":ObjectId(idx),"year_published":{"$exists":1}},{"year_published":1,"types":1}):
            data.append(work)
        result=self.bars.products_by_year_by_type(data)
        return result


    @endpoint('/app/affiliation', methods=['GET'])
    def app_affiliation(self):
        section = self.request.args.get('section')
        tab = self.request.args.get('tab')
        data = self.request.args.get('data')
        idx = self.request.args.get('id')

        if section=="info":
            result = self.get_info(idx)
        elif section=="research":
            if tab=="products":
                plot=self.request.args.get("plot")
                if plot:
                    if plot=="year_type":
                        result=self.get_products_by_year_by_type(idx)
                    
                else:
                    idx = self.request.args.get('id')
                    typ = self.request.args.get('type')
                    result = self.get_research_products(idx)
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