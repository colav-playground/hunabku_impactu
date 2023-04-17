from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from bson import ObjectId
from pymongo import MongoClient,ASCENDING,DESCENDING
from hunabku.Config import Config, Param
from hunabku_impactu.utils import JsonEncoder


class OurDataApp(HunabkuPluginBase):
    config=Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config+=Param(colav_db="colombia_udea")
    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.client=MongoClient(self.config.db_uri)
        self.colav_db=self.client[self.config.colav_db]

    def get_our_data(self):
        
        entry={
            "works":self.colav_db["works"].count_documents({})
            "authors":self.colav_db["person"].count_documents({"external_ids":{"$ne":[]}}),
            "affiliations":self.colav_db["affiliations"].count_documents({"external_ids":{"$ne":[]}}),
            "sources":self.colav_db["sources"].count_documents({})
        }
        return entry

    @endpoint('/app/ourdata', methods=['GET'])
    def app_ourdata(self):
        data = self.request.args.get('data')
        result=self.get_our_data()
        if result:
            response = self.app.response_class(
            response=self.json.dumps(result),
            status=200,
            mimetype='application/json'
            )
        else:
            response = self.app.response_class(
            response=self.json.dumps({}),
            status=204,
            mimetype='application/json'
            )
        
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response