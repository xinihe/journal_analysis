# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 17:45:50 2018

@author: Ni He

数据库表格的名字需要修改 ejos 一共 三处
中断处理办法：读出中断时候的i和pg； 将第96行附近的循环中的i从中断i开始，运行一次；
修改i回到原始状态( for i in range(0,len(paper_suburls)))；将65行附近的循环中的pg从中断pg+1运行到结束；
"""

import requests
import re
import mysql.connector
import time
from bs4 import BeautifulSoup
import numpy as np
import datetime
import socket
import content_analysis as ca
from fake_useragent import UserAgent


socket.setdefaulttimeout(80) 
#
from nltk.tokenize import WordPunctTokenizer  
 
#def wordtokenizer(sentence):
#    words = WordPunctTokenizer().tokenize(sentence)
#    return words

def parse(url):
#    user_agent = ["Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
#                  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
#                  ]
#    headers = {"user-agent": user_agent[np.random.randint(0,2,1)[0]]}
    ua = UserAgent()
    headers = {'User-Agent':ua.random}
    try:
        res = requests.get(url, timeout=60, headers=headers)
        if res is not None:
#        if res.status_code == 200:
            return res
        else:
            print('Unsuccessful parse, retry again')
            time.sleep(np.random.randint(60,600,1)) 
            print('Retry.....')
            return parse(url)
    except:
        print('Error encounted, retry after a few minutes')
        time.sleep(np.random.randint(60,200,1)) 
        print('Retry...')
        return parse(url)
#    else:
#        return res
            

def is_relevant(sentence):
    res = 0
    words = WordPunctTokenizer().tokenize(sentence)
    relevant_words = ['stock', 'credit', 'market', 'volatility', 'arbitrage',\
                      'asset','bid','business','cash','collateralized','corporate',\
                      'earnings','economic', 'invesment', 'invest', 'liability', 'mortgage',\
                      'premium', 'price', 'bond', 'options', 'future', 'purchase',\
                      'recession','finance', 'financial','reinvestment','securities',\
                      'shareholder', 'speculative', 'speculation', 'swaps', 'value', 'turnover',\
                      'vix', 'volatility', 'yield', 'bitcoin', 'transaction', 'banking', 'bank',\
                      'fund', 'debt', 'monetary', 'expenditure', 'fund', 'funds', 'trade',\
                      'deposit','buyout', 'equity', 'cds', 'cdo', 'ipo']
    if len([w.lower() for w in words if w.lower() in relevant_words]) > 0:
        res = 1        
    return res

def con_mysql(sql):    
#连接MySQL数据库
    conn = mysql.connector.connect(host="10.23.0.2",port=3306,user="root",\
                       password="11031103",database="fintech",charset="utf8")
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close
    conn.close
    
    
def parse_list_paper(pagenum, start_url, pageurlprefix, start_page = 1):
    for pg in range(int(start_page),pagenum + 1):
        # Next page URL, being used before the end of this loop
        nexturl = start_url + '&page=' + str(pg)
        # Proceeds to next page by using nexturl
        res = parse(nexturl)
        soup = BeautifulSoup(res.text, 'lxml')
        
        tmp = soup.find_all(class_ = 'search-results-content')
        # 采集论文具体网址，论文题目，作者姓名
        paper_urls = [url.find('a').get('href') for url in tmp]
        paper_authors = []
        paper_titles = []
        for t in tmp:
            paper_titles.append([x for x in t.text.split('\n') if x != ''][0].strip().replace('\'',''))
            paper_authors.append([x for x in t.text.split('\n') if x != ''][1].split('By:')[-1].strip().replace('; et al.', ''))
        res.close()
        time_start = time.time()
        parse_paper(paper_urls, pageurlprefix, paper_titles, paper_authors)
        # Show the progress... 
        print('Page %s out of %s has been stored.. it takes %s secs.' % (str(pg), str(pagenum), time.time()-time_start))
        # End of loop 

def parse_paper(paper_urls, pageurlprefix, paper_titles, paper_authors, start_paper = 0):  
    
    for i in range(int(start_paper),len(paper_urls)):
        paper_suburl = pageurlprefix+paper_urls[i] + '&cacheurlFromRightClick=no'
        res = parse(paper_suburl)
        soup = BeautifulSoup(res.text, 'lxml') 
        try:
            if soup.find('span', text=re.compile('Volume')):
                vol = soup.find('span', text=re.compile('Volume')).find_next_sibling().text
            else:
                vol = 'Null'
            
            if soup.find('span', text=re.compile('Pages')):
                ppages = soup.find('span', text=re.compile('Pages')).find_next_sibling().text
            else:
                ppages = 'Null'
                
            if soup.find('span', text=re.compile('Issue')):
                iss = soup.find('span', text=re.compile('Issue')).find_next_sibling().text
            else:
                iss = 'Null'
            
            if soup.find('span', text=re.compile('Published')):
                pdates = soup.find('span', text=re.compile('Published')).find_parent().text.split(':')[-1]
            else:
                pdates = 'Null'
                
            if soup.find('span', text=re.compile('Document')):
                ptype = soup.find('span', text=re.compile('Document')).find_parent().text.split(':')[-1]
            else:
                ptype = 'Null'
            
            try:
                pabstracts = soup.find('div', text=re.compile('Abstract')).parent.find('p').text.replace('\n','').replace('\'','')
            except:
                pabstracts = 'Null'
            try:
                pkword = soup.find('div', text=re.compile('Keywords')).parent.find('p').text.replace('\n','').replace('\'','').replace('Author Keywords:','')
            except:
                pkword = 'Null'
            try:
                pfunding = soup.find('div', text=re.compile('Funding')).parent.find('p').text.replace('\n','').replace('\'','')
            except:
                pfunding = 'Null'
            
            try:
                pemail = ','.join([x.text for x in soup.find_all('a', class_=re.compile('email'))])
            except:
                pemail = 'Null'
            
            try:
                paddress = soup.find('td', class_='fr_address_row2').text.split('\n')[0]
            except:
                paddress = 'Null'
            try:    
                pcites = soup.find('span', class_='large-number').text
            except:
                pcites = 'Null'
        
            
            prelevant = is_relevant(paper_titles[i])
            
            
            sql_ins = "insert into %s \
            (title, authors, au_email, citation, volume, iss, ppage, pdate, purl, pkeywords, pabstract,\
             funding, paddress, ptypes, prelevant, ctime) \
            values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" %\
            (table_name, paper_titles[i].replace("'","").replace('"',''), paper_authors[i].replace("'","").replace('"',''),\
             pemail, pcites, vol, iss, ppages, pdates, paper_urls[i], pkword, pabstracts,\
             pfunding, paddress, ptype, prelevant, datetime.datetime.now().strftime('%b-%d-%y %H:%M:%S'))
    
            con_mysql(sql_ins)
    #        cur.execute(sql_ins)
    #        conn.commit() 
            time.sleep(np.random.randint(5,50,1)) #睡眠2秒  
            res.close()
        except:
            print('Skip one paper which can be accessed by %s'%paper_urls[i])
            continue
    

    
def main_crawler(table_name, start_url):    
        #build a new table named by the journal title 
    sql = "create table if not exists %s (id int not null unique auto_increment, \
                 title varchar(800), authors varchar(500), au_email varchar(600),\
                 citation varchar(100), volume varchar(20), iss varchar(20), ppage varchar(100), pdate varchar(100), purl varchar(500), pkeywords varchar(1500), pabstract varchar(5000),\
                 funding varchar(4000), paddress varchar(2000), ptypes varchar(500), prelevant varchar(500), ctime varchar(200), primary key(id))" % table_name
    con_mysql(sql)
    
    
    pageurlprefix = 'http://apps.webofknowledge.com/'
    
    
    # Get the total number of pages of this journal
    res = parse(start_url)
    soup = BeautifulSoup(res.text, 'lxml')
    #pagenum = int(selector.xpath('//*[@id="pageCount.top"]/text()')[0])
    pagenum = int(soup.find( id = 'pageCount.top').text.replace(',',''))
    res.close()
    
    isfrombreak = input('There is %s pages for this Journal. Do you want full list? [y or n]' %pagenum)
    if isfrombreak == 'y':
        parse_list_paper(pagenum, start_url, pageurlprefix)
    else:
        start_page = input('Please enter the No. of the page from which to start crawling: ')
        start_paper = input('Please enter the No. of the paper in the starting page from which to start crawling: ')
        if int(start_paper) == 1:
            parse_list_paper(pagenum, start_url, pageurlprefix, int(start_page))
        else:

            start_page = int(start_page) 
                
            # finish the target page
            nexturl = start_url + '&page=' + str(start_page)
            # Proceeds to next page by using nexturl
            res = parse(nexturl)
            soup = BeautifulSoup(res.text, 'lxml')
            
            tmp = soup.find_all(class_ = 'search-results-content')
            # 采集论文具体网址，论文题目，作者姓名
            paper_urls = [url.find('a').get('href') for url in tmp]
            paper_authors = []
            paper_titles = []
            for t in tmp:
                paper_titles.append([x for x in t.text.split('\n') if x != ''][0].strip().replace('\'',''))
                paper_authors.append([x for x in t.text.split('\n') if x != ''][1].split('By:')[-1].strip().replace('; et al.', ''))
            res.close()
            time_start = time.time()
            
            parse_paper(paper_urls, pageurlprefix, paper_titles, paper_authors, start_paper)
            # Show the progress... 
            print('Page %s out of %s has been stored.. it takes %s secs.' % (str(start_page), str(pagenum), time.time()-time_start))
            # End of loop 
            
            # re start the loop
            parse_list_paper(pagenum, start_url, pageurlprefix, int(start_page)+1)

        
if __name__ == "__main__":    

    journal_name = input('Please enter the name of Journal (i.e. Expert Systems with Applications) : ')
    table_name = '_'.join(journal_name.lower().strip().split(' '))
    start_url = input('Please paste the starting URL of that Journal: ')
    main_crawler(table_name, start_url)
    print('Start to analyse the journal...')
    ca.main_analysis_func(journal_name)
    
        


    
  


















