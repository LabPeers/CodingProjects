# UrPartsCrawler

UrPartsCrawler is a Python programm for crawling the UrParts catalogue at https://www.urparts.com/index.cfm/page/catalogue.
It crawls the overall at this point in time existing catalogue and creates one CSV file for every manufacturer with the following data (example with 10 lines):

manufacturer,category,model,part,part_category  
Ammann,Roller Parts,ASC100,ND011710,LEFT COVER  
Ammann,Roller Parts,ASC100,ND011758,VIBRATOR  
Ammann,Roller Parts,ASC100,ND011785,RIGHT COVER  
Ammann,Roller Parts,ASC100,ND017673,ECCENTRIC  
Ammann,Roller Parts,ASC100,ND017675,ECCENTRIC  
Ammann,Roller Parts,ASC100,ND018184,HUB  
Ammann,Roller Parts,ASC100,ND018193,BRACKET  
Ammann,Roller Parts,ASC100,ND018214,LEFT SHAFT  
Ammann,Roller Parts,ASC100,ND018216,RIGHT SHAFT  


## Installation


```bash
No installation needed.
```

## Usage

A conda environment and Python 3.7.7 was used. The dependencies are in the requirements.txt file.

```python 
python crawler.py
```

## Contributing
This is a project for a technical interview.

## License
No license.