from cpi import inflate
from currency_converter import CurrencyConverter
import datetime
from hunabku_impactu.utils.hindex import hindex

class pies():
    def __init__(self):
        pass

    #Accumulated citations for each faculty department or group
    def citations_by_affiliation(self,data):
        results={}
        for name,works in data.items():
            for work in works:
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
                if name in results.keys():
                    results[name]+=1
                else:
                    results[name]=1
        
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list      


    #Accumulated papers for each faculty department or group
    def products_by_affiliation(self,data):
        result_list=[]
        for idx,value in data.items():
            result_list.append({"name":idx,"value":value})
        return result_list

    #APC cost for each faculty department or group
    def apc_by_affiliation(self,data,base_year):
        c = CurrencyConverter()
        now=datetime.date.today()
        result={}
        for name, costs in data.items():
            for apc in costs:
                value=0
                if apc["currency"]=="USD":
                    raw_value=apc["charges"]
                    value=inflate(raw_value,apc["year_published"],to=base_year)
                else:
                    try:
                        raw_value=c.convert(apc["xcharges"], apc["currency"], 'USD')
                        value=inflate(raw_value,apc["year_published"],to=base_year)
                    except Exception as e:
                        #print("Could not convert currency with error: ",e)
                        value=0
                if value:
                    if name not in result.keys():
                        result[name]=value
                    else:
                        result[name]+=value
        result_list=[]
        for idx,value in result.items():
            result_list.append({"name":idx,"value":int(value)})
        return result_list

    # H index for each faculty department or group
    def hindex_by_affiliation(self,data):
        result_list=[]
        for idx,value in data.items():
            result_list.append({"name":idx,"value":hindex(value)})
        return result_list

    # Ammount of papers per publisher
    def products_by_publisher(self,data):
        results={}
        for work in data:
            if work["publisher"]["name"]:
                if work["publisher"]["name"] in results.keys():
                    results[work["publisher"]["name"]]+=1
                else:
                    results[work["publisher"]["name"]]=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list

    # ammount of papers per openalex subject
    def products_by_subject(self,data):
        results={}
        for subject in data:
            if subject["subject"]["name"] in results.keys():
                results[subject["subject"]["name"]]+=1
            else:
                results[subject["subject"]["name"]]=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list

    # Ammount of papers per database
    def products_by_database(self,data):
        results={}
        for work in data:
            for source in work:
                if source["source"] in results.keys():
                    results[source["source"]]+=1
                else:
                    results[source["source"]]=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list

    # Ammount of papers per open access status
    def products_by_open_access_status(self,data):
        results={}
        for status in data:
            if status in results:
                results[status]+=1
            else:
                results[status]=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list

    # Ammount of papers per author sex
    def products_by_sex(self,data):
        results={}
        for work in data:
            if work["author"][0]["sex"] in results.keys():
                results[work["author"][0]["sex"]]+=1
            else:
                results[work["author"][0]["sex"]]=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list

    # Ammount of papers per author age intervals 14-26 años, 27-59 años 60 años en adelante
    def products_by_age(self,data):
        ranges={"14-26":(14,26),"27-59":(27,59),"60+":(60,999)}
        results={"14-26":0,"27-59":0,"60+":0}
        for work in data:
            birthdate=datetime.datetime.fromtimestamp(work["author"][0]["birthdate"]).year
            date_published=datetime.datetime.fromtimestamp(work["date_published"]).year
            age=(date_published-birthdate)
            for name,(date_low,date_high) in ranges.items():
                if age<date_high and age>date_low:
                    results[name]+=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list
            

    # Ammount of papers per scienti rank
    def products_by_scienti_rank(self,data):
        results={}
        for work in data:
            rank=None
            for ranking in work["ranking"]:
                if ranking["source"]=="scienti":
                    rank=ranking["rank"].split("_")[-1]
                    break
            if rank in ["A","A1","B","C","D"]:
                if rank in results.keys():
                    results[rank]+=1
                else:
                    results[rank]=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list

    # Ammount of papers per journal on scimago
    def products_by_scimago_rank(self,data):
        results={}
        for work in data:
            for ranking in work["source"]["ranking"]:
                if ranking["source"]=="scimago Best Quartile":
                    if ranking["from_date"]<work["date_published"] and ranking["to_date"]>work["date_published"]:
                        if ranking["rank"] in results.keys():
                            results[ranking["rank"]]+=1
                        else:
                            results[ranking["rank"]]=1
                        break
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list

    # Ammmount of papers published on a journal of the same institution
    def products_editorial_same_institution(self,data,institution):
        results={
            "same":0,
            "different":0
        }
        names=list(set([n["name"].lower() for n in institution["names"]]))

        for work in data:
            if work["source"]["publisher"]["name"]:
                if work["source"]["publisher"]["name"].lower() in names:
                    results["same"]+=1
                else:
                    results["different"]+=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"name":idx,"value":value})
        return result_list