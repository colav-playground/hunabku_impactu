

class bars():
    def __init__(self):
        self.a=1
    
    #production of affiliations by minciencias produyct type (within the hierarchy of the viewed entity)

    def products_by_year_by_type(self,data):
        '''
        Returns a list of dicts of the form {x:year, y:count, type:type} sorted by year in ascending order, 
        where year is the year of publication, count is the number of publications of a given type in that year, 
        and type is the type of publication. 

        Parameters:
        -----------
        data: list of works
        Returns:
        --------
        list of dicts with the format {x:year, y:count, type:typ}
        '''
        #anual production by minciencias product type (must have count, proportion, and total)
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

    #anual citations

    #anual APC costs

    #number of papers in openaccess or closed access

    #number of papers by editorial (top 5)

    #Anual H index from (temoporarily) openalex citations

    #Anual products count by researcher category

    #Anual products count by group category