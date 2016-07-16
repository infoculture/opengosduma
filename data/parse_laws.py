# coding: utf-8
import urllib2, urllib
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

MONTHS = {u'января' : 1, 
u'февраля' : 2, u'марта': 3, u'апреля' :4,
u'мая': 5, u'июня' : 6, u'июля' :7, u'августа': 8, u'сентября' : 9, u'октября' : 10, u'ноября' : 11, u'декабря' : 12}


class DepInfoParser:
	def __init__(self):
		self.conn = Connection()
		self.db = self.conn['duma']
		self.coll = self.db['deputies']
		self.lcoll = self.db['laws']
		pass


	def _process_law_page(self, root, person):
		blocks = root.findAll("div",  {'class' : 'search-block-result'})
		for b in blocks:
			item = {}
			a = b.find('h3').find('a')
			item['name'] = a.string
			item['url'] = a['href']
			o = self.lcoll.find_one({'url' : item['url']})
			if o is None:
				o = item
				bs = b.findNextSibling().findAll('b')
				o['attrs'] = []
				for b in bs:
					o['attrs'].append({'name' : b.string, 'value' : unicode(b.findNextSibling(text=True)) })
				o['persons'] = [person, ]
				self.lcoll.save(o)
			else:
				allp = o['persons']
				if person not in allp:
					o['persons'].append(person)					
				self.lcoll.save(o)
			print '- law', o['url'], 'processed'

	def parse_items(self):
		for o in self.coll.find():
			print 'Person', o['url']
			LAW_URL = BASE_URL + o['law_url'] 
			u = urllib2.urlopen(LAW_URL)
			html = u.read()
			root = BeautifulSoup(html)
			allcount = root.find('div', {'class' : 'page-nave-count'})
			if not allcount: continue
			parts = allcount.string.rsplit(' ', 1)
			allitems = int(parts[1])
			pages = allitems / 20
			if allitems % 20 > 0: pages += 1
			self._process_law_page(root, o['url'])						
			print 'Person', o['url'], 'pages', pages
			if pages > 1:
				for i in range(2, pages + 1, 1):
					print '--', i
					url = LAW_URL + '&PAGEN_1=%d' %(i)
					print url
					u = urllib2.urlopen(url)
					html = u.read()
					root = BeautifulSoup(html)
					self._process_law_page(root, o['url'])

	def find_fractions(self):
		one_p = 0
		dt = datetime.datetime(2007, 9, 1)
		for l in self.lcoll.find({'state' : 'published', 'initdate': {'$gt': dt}}):
			fr = {}
			if len(l['persons']) > 0:	
				for p in l['persons']:
					p = self.coll.find_one({'url' : p})
					frac = p['faction_name']
					v = fr.get(frac, 0)
					fr[frac] = v + 1
				if len(fr.keys()) == 1 and fr.keys()[0] != u'Фракция «ЕДИНАЯ РОССИЯ»':
					print l['url'], l['initdate']
					print '-', fr.keys()[0].encode('cp866', 'ignore'), 100.0
					one_p += 1
				elif u'Фракция «ЕДИНАЯ РОССИЯ»' not in fr.keys():
					total = len(l['persons'])
					for k, v in fr.items():
						print '-', k.encode('cp866', 'ignore'), v * 100.0 / total					
					one_p += 1
		print one_p

	def find_relations(self):
		one_p = 0
		one_n = 0
		mul_n = 0
		for l in self.lcoll.find():
			fr = {}
			if len(l['persons']) == 1:
				one_p +=1
			if len(l['persons']) > 1:	
				for p in l['persons']:
					p = self.coll.find_one({'url' : p})
					frac = p['faction_name']
					v = fr.get(frac, 0)
					fr[frac] = v + 1
				print l['url']
				if len(fr.keys()) == 1:
					print '-', fr.keys()[0].encode('cp866', 'ignore'), 100.0
					one_n += 1
				else:
					total = len(l['persons'])
					for k, v in fr.items():
						print '-', k.encode('cp866', 'ignore'), v * 100.0 / total
					mul_n +=1
		print one_n, mul_n, one_p

	def process_laws(self):
		states = {}
		i = 0
		for l in self.lcoll.find():
			i += 1
			if i % 1000 == 0: print i
			state = None
			if l['initdate'].year < 2007: continue
			for a in l['attrs']:
				if a['name'] == u'Стадия:':
					state = a['value'].strip()
				if a['name'] == u'Дата события:':
					thed = a['value'].strip()
					parts = thed.split()
					dt = datetime.datetime(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
					l['edate'] = dt
					d = dt - l['initdate']
					print d.days
					l['days'] = d.days
			if not state: continue
			v = states.get(state, 0)
			states[state] = v + 1
			if state == u'Опубликование закона (опубликование закона в "Российской газете")':
				l['state'] = 'published'
			elif state == u'Предварительное рассмотрение законопроекта, внесенного в Государственную Думу (рассмотрение Советом Государственной Думы законопроекта, внесенного в Государственную Думу)':
				l['state'] = 'preview'
			elif state == u'Рассмотрение законопроекта в первом чтении (рассмотрение законопроекта Государственной Думой)':
				l['state'] = 'fread'
			elif state == u'Рассмотрение законопроекта в первом чтении (рассмотрение Советом Государственной Думы законопроекта, представленного ответственным комитетом)':
				l['state'] = 'fread'
			self.lcoll.save(l)		
			
		for k, v in states.items():
			print k.encode('cp866', 'ignore'), v
			

if __name__ == "__main__":
	p = DepInfoParser()
#	p.parse_items()
	p.find_fractions()
#	p.process_laws()
