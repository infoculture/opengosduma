# coding: utf-8
import urllib2, urllib, cookielib
import socket
import lxml.html           
import simplejson as json
import datetime
import csv
from BeautifulSoup import BeautifulStoneSoup, BeautifulSoup
from pymongo import Connection

socket.setdefaulttimeout(15)

BASE_URL = 'http://www.duma.gov.ru'
LIST_URL = 'http://www.duma.gov.ru/structure/deputies/?letter=%D0%90'            



class DepInfoParser:
	def __init__(self):
		self.conn = Connection()
		self.db = self.conn['duma']
		self.coll = self.db['deputies']
		self.lcoll = self.db['laws']
		self.pcoll = self.db['perf']
		pass


	def _process_perf_page(self, root, person):
		blocks = root.findAll("div",  {'class' : 'stenogram-result-item '})
		for b in blocks:
			item = {}
			a = b.find('a')
			item['name'] = a.string
			item['url'] = a['href'].split('?')[0]
			o = self.pcoll.find_one({'url' : item['url']})
			if o is None:
				o = item
				o['raw'] = unicode(b)
				o['persons'] = [person, ]
				self.pcoll.save(o)
			else:
				allp = o['persons']
				if person not in allp:
					o['persons'].append(person)					
				self.pcoll.save(o)
			print '- perf', o['url'], 'processed'

	def parse_items(self):
		start = False
		for o in self.coll.find():
			if not start: 
				if o['url'] == 'http://www.duma.gov.ru/structure/deputies/23702/': 
					start = True
				else: 
					continue
			print 'Person', o['url']
			PERF_URL = o['perf_url'].encode('utf8')
			print PERF_URL
			cj = cookielib.CookieJar()
			opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
			u = opener.open(PERF_URL)
			real_url = u.geturl()
			html = u.read()
			root = BeautifulSoup(html)
			allcount = root.find('div', {'class' : 'request-param-page'})
			if not allcount: continue
			scount = allcount.findPreviousSibling().find('span').string
			icount = int(scount.split()[-1])
			pages = icount / 10
			if icount % 10 > 0: pages += 1
			self._process_perf_page(root, o['url'])						
			print 'Person', o['url'], 'pages', pages
			if pages > 1:
				for i in range(2, pages + 1, 1):
					print '--', i
					url = real_url.rsplit('&', 1)[0] + '&Page=%d' %(i)
					print url
					u = opener.open(url)
					html = u.read()
					root = BeautifulSoup(html)
					self._process_perf_page(root, o['url'])

	def process_items(self):
#		all = []
#		wp = self.coll.find({'perf_num' : {'$lt' : 5000, '$gt' : 999}})
#		for o in wp:
#			all.append(o['url'])
		print self.pcoll.find({'fulltext' : {'$exists' : False}}).count()
		for o in self.pcoll.find({'fulltext' : {'$exists' : False}}): #'persons' : {'$in' : all}, 
			url = 'http://www.cir.ru' + o['url']
			print url
			u = urllib2.urlopen(url)
			html = u.read()
			u.close()
			root = BeautifulSoup(html)
			t = root.find('div', {'class' : 'doc-sections'}).findNextSiblings('p')
			fulltext = ""
			fullraw = ""
			for s in t:
				td = s.find('table', {'width': '98%'})
				if not td: continue
				oi = td.find('i')
				if not oi or not oi.string: continue
				name = oi.string.strip()
				print name.encode('cp866', 'replace')
				texts = td.findAll('p')
				for p in texts:
					if not p or not p.string: continue
#					print p.string.encode('cp866', 'replace')
					fulltext += p.string
				fullraw += unicode(s)
			o['fulltext'] = fulltext
			o['rawtext'] =  fullraw
			self.pcoll.save(o)
		pass

	def dump_by_person(self):#, person='http://www.duma.gov.ru/structure/deputies/23928/'):
		for p in self.coll.find({'perf_parsed' : True}):
			f = open('persons/%s.txt' %(p['slug']), 'w')
			for o in self.pcoll.find({'persons' : p['url'], 'fulltext' : {'$exists' : True}}):
				f.write(o['fulltext'].strip().encode('utf8'))
			f.close()
			print p['slug']

	def map_perf(self):
		for o in self.coll.find({'perf_parsed' : {'$exists' : False}}):
			c = self.pcoll.find({'persons' : o['url'], 'fulltext' : {'$exists' : False}}).count()
			if c == 0:
				o['perf_parsed'] = True
			self.coll.save(o)
		
			

if __name__ == "__main__":
	p = DepInfoParser()
#	p.parse_items()
#	p.find_fractions()
#	p.process_items()
	p.map_perf()
	p.dump_by_person()
