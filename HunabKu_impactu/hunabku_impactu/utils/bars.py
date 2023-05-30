from cpi import inflate
from currency_converter import CurrencyConverter
import datetime
from hunabku_impactu.utils.hindex import hindex

class bars():
    def __init__(self):
        pass
    
    #production of affiliations by minciencias product type (within the hierarchy of the viewed entity)
    def products_by_year_by_type(self,data):
        '''
        Returns a list of dicts of the form {x:year, y:count, type:type} sorted by year in ascending order, 
        where year is the year of publication, count is the number of publications of a given type in that year, 
        and type is the type of publication. 

        Parameters
        -----------
        data: list of works

        Returns
        --------
        list of dicts with the format {x:year, y:count, type:typ}
        '''
        if not isinstance(data,list):
            print(type(data))
            return None
        if len(data)==0:
            print(len(data))
            return None
        result={}
        for work in data:
            if "year_published" in work.keys():
                year=work["year_published"]
                if year not in result.keys():
                    result[year]={}
                for typ in work["types"]:
                    if typ["source"]=="scienti":
                        if typ["level"]==2:
                            if typ["type"] not in result[year].keys():
                                result[year][typ["type"]]=1
                            else:
                                result[year][typ["type"]]+=1
        #turn the dict into a list of dicts with the format {x:year, y:count, type:typ} sorted by year in ascending order
        result_list=[]
        for year in result.keys():
            for typ in result[year].keys():
                result_list.append({"x":year,"y":result[year][typ],"type":typ})
        result_list=sorted(result_list,key=lambda x: x["x"])

        return result_list

    def products_by_affiliation_by_type(self,data):
        '''
        Returns a list of dicts of the form {x:affiliation_name, y:count, type:type} sorted by year in ascending order, 
        where affiliation_name is the name of the affiliation that published, count is the number of publications of a given type in that year, 
        and type is the type of publication. 

        Parameters
        -----------
        data: list of works

        Returns
        --------
        list of dicts with the format {x:affiliation_name, y:count, type:typ}
        '''
        if not isinstance(data,dict):
            print(type(data))
            return None
        if len(data)==0:
            print(len(data))
            return None
        result={}
        for name,works in data.items():
            for work in works:
                if name not in result.keys():
                    result[name]={}
                for typ in work["types"]:
                    if typ["source"]=="scienti":
                        if typ["level"]==2:
                            if typ["type"] not in result[name].keys():
                                result[name][typ["type"]]=1
                            else:
                                result[name][typ["type"]]+=1
        #turn the dict into a list of dicts with the format {x:year, y:count, type:typ} sorted by year in ascending order
        result_list=[]
        for name in result.keys():
            for typ in result[name].keys():
                result_list.append({"x":name,"y":result[name][typ],"type":typ})
        result_list=sorted(result_list,key=lambda x: x["y"],reverse=True)

        return result_list

    #anual citations
    def citations_by_year(self,data):
        '''
        Returns a list of dicts of the form {x:year, y:count} sorted by year in ascending order, 
        where year is the year of publication and count is the number of citations of the work in that year.
        
        Parameters
        -----------
        data: list of citations by year

        Returns
        --------
        list of dicts with the format {x:year, y:count}
        '''
        
        result={}
        for work in data:
            for yearly in work["citations_by_year"]:
                if yearly["year"] in result.keys():
                    result[yearly["year"]]+=yearly["cited_by_count"]
                else:
                    result[yearly["year"]]=yearly["cited_by_count"]
        result_list = sorted(result.items(),key=lambda x: x[0])
        result_list = [{"x":x[0],"y":x[1]} for x in result_list]
        
        return result_list
        

    #anual APC costs
    def apc_by_year(self,data,base_year):
        '''
        Returns a list of dicts of the form {x:year, y:cost} sorted by year in ascending order,
        where year is the year of publication and cost is the cost of the APC in that year.

        Parameters
        -----------
        data: list of works with the information about the journal
        base_year: int with the year to which the costs will be inflated

        Returns
        --------
        list of dicts with the format {x:year, y:cost}
        '''
        c = CurrencyConverter()
        now=datetime.date.today()
        result={}
        for reg in data:
            if reg["apc"]["currency"]=="USD":
                raw_value=reg["apc"]["charges"]
                value=inflate(raw_value,reg["year_published"],to=base_year)
            else:
                try:
                    raw_value=c.convert(reg["apc"]["xcharges"], reg["apc"]["currency"], 'USD')
                    value=inflate(raw_value,reg["year_published"],to=base_year)
                except Exception as e:
                    #print("Could not convert currency with error: ",e)
                    value=0
            if value:
                if reg["year_published"] not in result.keys():
                    result[reg["year_published"]]=value
                else:
                    result[reg["year_published"]]+=value
        sorted_result=sorted(result.items(),key=lambda x: x[0])
        result_list=[{"x":x[0],"y":int(x[1])} for x in sorted_result]
        return result_list

    #number of papers in openaccess or closed access
    def oa_by_year(self,data):
        '''
        Returns a list of dicts of the form {x:year, y:count} sorted by year in ascending order, 
        where year is the year of publication and count is the number of works in open access in that year.

        Parameters
        -----------
        data: list of works with bibliographic info

        Returns
        --------
        list of dicts with the format {x:year, y:count}
        '''
        result={}
        for work in data:
            year=work["year_published"]
            if year in result.keys():
                if work["bibliographic_info"]["is_open_acess"]:
                    result[year]["open"]+=1
                else:
                    result[year]["closed"]+=1
            else:
                if work["bibliographic_info"]["is_open_acess"]:
                    result[year]={"open":1,"closed":0}
                else:
                    result[year]={"open":0,"closed":1}
        result_list=[]
        for year in result.keys():
            for typ in result[year].keys():
                result_list.append({"x":year,"y":result[year][typ],"type":typ})
        result_list=sorted(result_list,key=lambda x: x["x"])
        return result_list

    #number of papers by publisher (top 5) in total
    def products_by_year_by_publisher(self,data):
        '''
        Returns a list of dicts of the form {x:year, y:count, type:publisher} sorted by year in ascending order, 
        where year is the year of publication, count is the number of works published in that year by the publisher, 
        and publisher is the name of the publisher.

        Parameters
        -----------
        data: list of works with sources info

        Returns
        --------
        list of dicts with the format {x:year, y:count, type:publisher}
        '''
        result={}
        top5={} #total
        for work in data:
            year=int(work["year_published"])
            if year in result.keys():
                if work["publisher"]["name"] not in result[year].keys():
                    result[year][work["publisher"]["name"]]=1
                else:
                    result[year][work["publisher"]["name"]]+=1
            else:
                result[year]={work["publisher"]["name"]:1}
            if work["publisher"]["name"] not in top5.keys():
                top5[work["publisher"]["name"]]=1
            else:
                top5[work["publisher"]["name"]]+=1

        top5=[top[0] for top in sorted(top5.items(),key=lambda x: x[1],reverse=True)][:5]

        result_list=[]
        for year in result.keys():
            for publisher in top5:
                if publisher in result[year].keys():
                    result_list.append({"x":year,"y":result[year][publisher],"type":publisher})
                else:
                    result_list.append({"x":year,"y":0,"type":publisher})
        
        result_list=sorted(result_list,key=lambda x: x["x"])
        return result_list

    #Anual H index from (temporarily) openalex citations
    def h_index_by_year(self,data):
        '''
        Returns a list of dicts of the form {x:year, y:h_index} sorted by year in ascending order, 
        where year is the year of publication and h_index is the h-index of the works cited up to a selected year.

        Parameters
        -----------
        data: list of citations by year

        Returns
        --------
        list of dicts with the format {x:year, y:h_index}
        '''
        if len(data)<=0:
            return None
        h_by_year={}
        works_total_citations_by_year=[]
        for work in data:
            acc_citations_by_year=[]
            years=[]
            sorted_citations=sorted(work["citations_by_year"],key=lambda x: x["year"])
            for citation in sorted_citations:
                if len(acc_citations_by_year)==0:
                    acc_citations_by_year.append(
                        {"year":citation["year"],
                        "citations":citation["cited_by_count"]}
                    )
                else:
                    acc_citations_by_year.append(
                        {"year":citation["year"],
                        "citations":citation["cited_by_count"]+acc_citations_by_year[-1]["citations"]}
                    )
            works_total_citations_by_year.append(acc_citations_by_year)
        
        for work in works_total_citations_by_year:
            for citations in work:
                year=citations["year"]
                citations=citations["citations"]
                if not year in h_by_year.keys():
                    h_by_year[year] = [citations]
                else:
                    h_by_year[year].append(citations)

        index_by_year=[]
        years=set([x[0] for x in h_by_year.items()])
        for year in years:
            index_by_year.append({"x":year,"y":hindex(h_by_year[year])})
        sorted_index_by_year=sorted(index_by_year,key=lambda x: x["x"])
        return sorted_index_by_year

    #Anual products count by researcher category
    def products_by_year_by_researcher_category(self,data):
        '''
        Returns a list of dicts of the form {x:year, y:count, type:researcher_type} sorted by year in ascending order, 
        where year is the year of publication, count is the number of works published in that year by the researcher type, 
        and researcher_type is the type of the researcher.

        Parameters
        -----------
        data: list of works with author info

        Returns
        --------
        list of dicts with the format {x:year, y:count, type:researcher_type}
        '''
        result={}
        for work in data:
            if "year_published" in work.keys():
                year=work["year_published"]
                if year not in result.keys():
                    result[year]={}
                if work["rank"] not in result[year].keys():
                    result[year][work["rank"]]=1
                else:
                    result[year][work["rank"]]+=1
        result_list=[]
        for year in result.keys():
            for rank in result[year].keys():
                result_list.append({"x":year,"y":result[year][rank],"type":rank})
        result_list=sorted(result_list,key=lambda x: x["x"])
        return result_list

    #Anual products count by group category
    def products_by_year_by_group_category(self,data):
        '''
        Returns a list of dicts of the form {x:year, y:count, type:group_category} sorted by year in ascending order, 
        where year is the year of publication, count is the number of works published in that year by the group category, 
        and group_category is the category of the group.

        Parameters
        -----------
        data: list of works with group info

        Returns
        --------
        list of dicts with the format {x:year, y:count, type:group_category}
        '''
        result={}
        for work in data:
            if "date_published" in work.keys() and "year_published" in work.keys():
                year=work["year_published"]
                year_timestamp=datetime.datetime.strptime(str(year),"%Y").timestamp()
                date=work["date_published"]
                rank_name=""
                #print(year,year_timestamp, date)
                for rank in work["ranking"]:
                    if rank["source"]=="scienti":
                        #print(rank["rank"],rank["from_date"],rank["to_date"])
                        if rank["from_date"]<year_timestamp and rank["to_date"]>year_timestamp:
                            rank_name=rank["rank"]
                            break
                if rank_name!="":
                    #print(rank["rank"],rank["from_date"],rank["to_date"])
                    if year not in result.keys():
                        result[year]={}
                    if rank_name not in result[year].keys():
                        result[year][rank_name]=1
                    else:
                        result[year][rank_name]+=1
        result_list=[]
        for year in result.keys():
            for group_category in result[year].keys():
                result_list.append({"x":year,"y":result[year][group_category],"type":group_category})
        
        result_list=sorted(result_list,key=lambda x: x["x"])
        return result_list