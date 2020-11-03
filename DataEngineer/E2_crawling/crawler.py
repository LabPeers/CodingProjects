from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import csv
import pandas as pd
from manufacturerList import mymanufacturers
from manufacturerList import mycategories

class CatalogueCrawler:

    def __init__(self):
        pass

    def retrieve_models(self,info1):
        myurl = "https://www.urparts.com/index.cfm/page/catalogue/{}/{}".format(info1[0],
        info1[1].replace(" ", "%20"))

        req = Request(myurl, headers={'User-Agent': 'Mozilla/5.0'})
        s = urlopen(req).read()       
        soup = BeautifulSoup(s,features="lxml")


        #------Extract models info
        models_div=soup.find('div', class_="c_container allmodels")
        models_li = models_div.find_all('li')
        
        models_list=[]
        for mymodel in models_li:
            models_list.append(mymodel.get_text())
        
        models_list=[var.replace("\n ","").replace(" \n","") for var in models_list]
        print("Successfully retrieved models")
        return models_list


    def retrieve_parts(self,info2):
        #------Retrieve full HTML document
        #info2=[manufacturer, category, model]
        #print(info2)
        parts_df=[]
        myurl="https://www.urparts.com/index.cfm/page/catalogue/{}/{}/{}".format(info2[0],info2[1].replace(" ", "%20"),info2[2].replace(" ", "%20"))
        req = Request(myurl, headers={'User-Agent': 'Mozilla/5.0'})
        s = urlopen(req).read()       

        soup = BeautifulSoup(s,features="lxml")

        #soup_string = str(soup)
        # with open('Ammann.txt', 'w') as f:
        #     f.write(soup_string)

        #------Extract parts info
        parts_div=soup.find('div', class_="c_container allparts")
        if not parts_div:
            print("There are no parts!")
            return [],soup
        
        else:
            parts_li = parts_div.find_all('li')
            
            parts_list=[]
            for mypart in parts_li:
                parts_list.append((mypart.get_text()).replace("\n ","").replace(" \n","").replace("\n",""))

            return parts_list,soup


    def write_csv(self,parts_list,soup,info2):

        parts_df = pd.DataFrame(parts_list, columns = ['parts2'])
                
        parts_df[['part','part_category']] = parts_df.parts2.str.split((" - "),1,expand=True) 

        #------Extract manufacturer, category and model info
        mytitle=soup.find('title').get_text()
        mytitle_list=mytitle[10:].split(" - ")

        parts_df.insert(0, 'manufacturer', mytitle_list[0])
        parts_df.insert(1, 'category', mytitle_list[1])
        parts_df.insert(2, 'model', mytitle_list[2])
        parts_df.drop('parts2', axis = 1,inplace = True) 
        parts_df.to_csv('csv_files_parts/parts_{}.csv'.format(info2[0]), mode='a',index=False)
        print("Successfully inserted data into csv file")




def main():
    
    catalogue = CatalogueCrawler()
    for i in range(0,len(mymanufacturers)):
        for j in range(0,len(mycategories[i])):
            info1 = [mymanufacturers[i],mycategories[i][j]]
            mymodelslist = catalogue.retrieve_models(info1)
            for k in range(0,len(mymodelslist)):
                info2=info1+[mymodelslist[k]]
                print(info2)
                parts_list,soup = catalogue.retrieve_parts(info2)
                if not parts_list:
                    print("Parts list is empty!")    
                else:
                    catalogue.write_csv(parts_list,soup,info2)



if __name__ == '__main__':
    main()

