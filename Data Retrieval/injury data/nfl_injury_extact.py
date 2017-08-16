import requests
from bs4 import BeautifulSoup
url = "http://www.footballdb.com/transactions/injuries.html"
file = open('out.txt','w')
r = requests.get(url)
html_content = r.text
soup = BeautifulSoup(html_content,"html.parser")
for tr in soup.find_all('tr'):
 col = tr.find_all('td')
 col = [ele.text.strip() for ele in col]
 str1 = " ".join(str(item1) for item1 in col)
 file.write(str1+ '\n')


