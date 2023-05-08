from cpi import inflate
from currency_converter import CurrencyConverter
import datetime
from hunabku_impactu.utils.hindex import hindex
import spacy

class pies():
    def __init__(self):
        pass

    #20 most used words in the titles of the works
    def most_used_words(self,data,size=20):
        '''
        Takes a list of dictionaries data as input and returns
        the top 20 most frequently used words in the titles of the works in data.
        
        It uses the en_core_web_sm Spacy model to preprocess the titles and remove
        stopwords and tokens that are numeric or have a length of less than 3. It then
        counts the frequency of each remaining token and returns the top 20 in a list of
        dictionaries with keys "type" and "value" representing the word and its frequency, respectively.

        Parameters
        ----------

        data : list of dictionaries
        A list of dictionaries containing information about works, where each dictionary has a key
        "titles" with a value that is a list of dictionaries with a key "title" and a string value representing the title of the work.

        Returns
        -------

        results : list of dictionaries
        A list of dictionaries containing the top 20 most frequently used words in the titles
        of the works in the data list, where each dictionary has a key "type" with a string
        value representing the word and a key "value" with an integer value representing the frequency of the word.
        '''
        en = spacy.load('en_core_web_sm')
        es = spacy.load('es_core_news_sm')
        stopwords = en.Defaults.stop_words.union(es.Defaults.stop_words)
        results={}
        for work in data:
            title=work["titles"][0]["title"].lower()
            lang=work["titles"][0]["lang"]
            if lang=="es":
                model=es
            else:
                model=en
            title=model(title)
            for token in title:
                if token.lemma_.isnumeric():
                    continue
                if token.lemma_ in stopwords:
                    continue
                if len(token.lemma_)<4:
                    continue
                if token.lemma_ in results.keys():
                    results[token.lemma_]+=1
                else:
                    results[token.lemma_]=1
        topN=sorted(results.items(), key=lambda x: x[1], reverse=True)[:size]
        results=[]
        for top in topN:
            results.append({"type":top[0],"value":top[1]})
        return results     

    #Accumulated citations for each faculty department or group
    def citations_by_affiliation(self,data):
        results={}
        for work in data:
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
            for affiliation in work["affiliations"]:
                if affiliation["name"] in results.keys():
                    results[affiliation["name"]]+=1
                else:
                    results[affiliation["name"]]=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"type":idx,"value":value})
        return result_list      


    #Accumulated papers for each faculty department or group
    def papers_by_affiliation(self,data):
        results={}
        for work in data:
            if work["affiliations"]==[]:
                continue
            for affiliation in work["affiliations"]:
                if affiliation["name"] in results.keys():
                    results[affiliation["name"]]+=1
                else:
                    results[affiliation["name"]]=1
        result_list=[]
        for idx,value in results.items():
            result_list.append({"type":idx,"value":value})
        return result_list

    #APC cost for each faculty department or group
    def apc_by_affiliation(self,data):
        results={}
        for work in data:
            if work["affiliations"]==[]:
                continue
            for affiliation in work["affiliations"]:
                if affiliation["name"] in results.keys():
                    results[affiliation["name"]]+=work["apc"]
                else:
                    results[affiliation["name"]]=work["apc"]
        result_list=[]
        for idx,value in results.items():
            result_list.append({"type":idx,"value":value})
        return result_list

    # H index for each faculty department or group
    def hindex_by_affiliation(self,data):
        results={}
        for work in data:
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
            if work["affiliations"]==[]:
                continue
            for affiliation in work["affiliations"]:
                if affiliation["name"] in results.keys():
                    results[affiliation["name"]].append(citations)
                else:
                    results[affiliation["name"]]=[citations]
        for affiliation in results.keys():
            results[affiliation]=hindex(results[affiliation])
        return results

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
            result_list.append({"type":idx,"value":value})
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
            result_list.append({"type":idx,"value":value})
        return result_list

    # Ammount of papers per database

    # Ammount of papers per open access status

    # Ammount of papers per author sex

    # Ammount of papers per author age intervals

    # Ammount of papers per scienti rank

    # Ammount of papers per journal on scimago

    # Ammmount of papers published on a journal of the same 