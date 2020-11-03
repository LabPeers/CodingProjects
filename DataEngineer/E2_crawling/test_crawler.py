import unittest
from crawler import CatalogueCrawler


class CrawlerTest(unittest.TestCase):

    def setUp(self):
        self.manuf = 'Ammann'
        self.category = 'Roller Parts'
        self.model='ASC100'
        self.catalogue = CatalogueCrawler()

    def test_retrieve_models(self):
        print("Test - 1 called ...")
        #Act
        model_list = self.catalogue.retrieve_models([self.manuf,self.category]) #Should print model list
        result1=model_list[0]
        #Assert
        self.assertEqual(result1, "ASC100")

    def test_retrieve_parts(self):
        #Act
        parts_list,soup = self.catalogue.retrieve_parts([self.manuf,self.category,self.model])
        print(type(parts_list))
        result2=parts_list[0]
        #Assert
        self.assertEqual(result2,"ND011710 - LEFT COVER")



if __name__ == "__main__":
    unittest.main()

