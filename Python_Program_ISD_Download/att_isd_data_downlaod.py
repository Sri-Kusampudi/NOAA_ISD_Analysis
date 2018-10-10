import requests
import os
from urllib.request import urlopen
from bs4 import BeautifulSoup

print('Beginning file download with requests')

#for i in range(1901, 2019):
#for i in range(1960, 1970):
for i in range(2015, 2016):
    #URL : the site address to download from
    url = 'https://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite/' + str(i) + "/"
    print(url)
    r = requests.get(url)

    # create beautiful-soup object
    soup = BeautifulSoup(r.content, 'html5lib')

    # find all links on web-page
    links = soup.findAll('a')

    # filter the link sending with .gz
    links = [url + link['href'] for link in links if link['href'].endswith('gz')]
    # path = "/Users/sriku/Downloads/att/" + str(i)
    # os.mkdir(path)
    #for every gz link in the folder pertaining to a year
    for link in links:
        print(link)
        searchStr = str(i) + "/"
       # filename = str(i) + "/" + link.split(searchStr,1)[1]
        #split the string at the searchStr and extract the rest of the string as filename
        filename = link.split(searchStr, 1)[1]
        #Open the link, read the data and write to local file syste
        filedata = urlopen(link)
        datatowrite = filedata.read()
        with open('/Users/sriku/Downloads/att/'+ filename, 'wb') as f:
            f.write(datatowrite)