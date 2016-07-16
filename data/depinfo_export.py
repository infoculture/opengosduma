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

def writeline(d, keys):
	arr = []
	for k in keys:
		arr.append(unicode(d[k]))
	return ('\t'.join(arr)).encode('utf8', 'replace')

keys=['name', 'url', 'birthdate', 'partyname', 'group', 'num', 'status_app', 'status_reg']

MONTHS = {u'января' : 1, 
u'февраля' : 2, u'марта': 3, u'апреля' :4,
u'мая': 5, u'июня' : 6, u'июля' :7, u'августа': 8, u'сентября' : 9, u'октября' : 10, u'ноября' : 11, u'декабря' : 12}


class DepInfoParser:
	def __init__(self):
		self.conn = Connection()
		self.db = self.conn['duma']
		self.coll = self.db['deputies']
		self.lcoll = self.db['laws']
		self.pcoll = self.db['perf']
		pass

	def get_list(self):
		list_url = LIST_URL
		f = urllib2.urlopen(list_url)
		html = f.read()
		root = BeautifulStoneSoup(html)
		ddat = root.find("div",  {'class' : 'page-nave-1'})
		hrefs = ddat.findAll('a')	
		print LIST_URL
		for h in hrefs:
			print BASE_URL + h['href']
		pass

	def parse_lists(self):
		f = open('list.txt', 'r')
		for l in f:
			l = l.strip()
			u = urllib.urlopen(l)
			html = u.read()
			root = BeautifulStoneSoup(html)
			ddat = root.find("table",  {'id' : 'lists_list_elements_35'})
			trs = ddat.findAll('tr')	
			record = {}
			keys = ['img', 'fullname', 'url', 'faction_name', 'faction_url', 'working', 'regions']
			for tr in trs[1:]:
				tds = tr.findAll('td')
				record['img'] = BASE_URL + tds[0].find('a').find('img')['src']
				record['fullname'] = tds[1].find('a').string
				record['url'] = BASE_URL + tds[1].find('a')['href']
				record['faction_name'] = BASE_URL + tds[2].find('a').string
				record['faction_url'] = BASE_URL + tds[2].find('a')['href']
				coms = []
				hrefs = tds[3].findAll('a')
				for h in hrefs:
					coms.append(u"%s|%s" %(BASE_URL + h['href'], h.string))
				record['working'] = u';'.join(coms)
				record['regions'] = tds[4].string
				s = []
				for k in keys:
					s.append(record[k])
				print (u'\t'.join(s)).encode('utf8')
		pass

	def update_factions(self):
		f = open('deputies.csv', 'r')
		for l in f:
			l = l.strip().decode('utf8')
			parts = l.split('\t')
			url = parts[2]
			o = self.coll.find_one({'url' : url})
			if o is not None:
				print parts[3].encode('cp866', 'ignore')
				o['faction_name'] = parts[3].rsplit('u', 1)[1]
			self.coll.save(o)
		

	def process_items(self):
		f = open('deputies.csv', 'r')
		for l in f:
			l = l.strip()
			parts = l.split('\t')
			url = parts[2]
			o = self.coll.find_one({'url' : url})
			if not o:
				u = urllib.urlopen(parts[2])
				html = u.read()
				root = BeautifulSoup(html)
				ddat = root.find("div",  {'class' : 'deputat-info'})
				name= root.find('div', {'class' : 'hc-r'}).find('h1').string
				o = {'url' : url, 'raw': unicode(ddat), 'name' : name}
				self.coll.save(o)
			print url

	def parse_items(self):
		for o in self.coll.find():
			root = BeautifulSoup(o['raw'])
			ddat = root.find("div",  {'class' : 'deputat-info'})
			# Find birth date
			p = ddat.find('p', {'class' : 'deputat-info-date'})
			if p:
				o['birthdate_s'] = p.string
			# Find intro and workdate and working comitteee
			p = ddat.find('p', {'class' : 'deputat-info-intro'})
			if p:
				o['intro'] = p.string
				p2 = p.findNextSibling()
				o['workdate_s'] = p2.string
				alla = p2.findNextSibling().findAll('a')
				items = []
				for a in alla:
					items.append({'name': a.string, 'url' : BASE_URL + a['href']})
				o['works'] = items
			# Find edu and awards blocks
			h3s = ddat.find('div', {'class' : 'deputat-info-right'})
			for h in h3s:
				s = h.string
				items = []								
				lis = h.findNextSibling()
				if not lis: continue
				lis = lis.findAll('li')
				has_urls = False
				if s == u'Образование': 
					key = 'edu'
				elif s == u'Ученые степени':
					key = 'awards'
				elif s == u'Связь с избирателями в регионах РФ':
					key = 'regions'
				elif s == u'Персональные страницы, блоги':			
					key = 'websites'
					has_urls = True
				else: 
					continue
				if key == 'websites':
					for l in lis:
						items.append(l.find('a')['href'])
				elif key in ['edu', 'awards', 'regions']:
					for l in lis:
						items.append(l.string)
				o[key] = items
			# Find performing and laws
			li = root.find('li', {'class' : 'di-perfom'})
			a = li.find('a')
			o['perf_url'] = a['href']		
			s = a.find('span')
			if s is not None:
				s = s.string.strip(')').strip('(')
				o['perf_num'] = int(s) if len(s) > 0 else 0
			else:
				o['perf_num'] = 0

			# Find laws
			li = root.find('li', {'class' : 'di-law'})
			a = li.find('a')
			o['law_url'] = a['href']
			s = a.find('span')
			if s:
				s = s.string.strip(')').strip('(')
				o['law_num'] = int(s) if len(s) > 0 else 0
			else:
				o['law_num'] = 0
			o['avg_num'] = o['perf_num'] + o['law_num']
			self.coll.save(o)
						
	def find_dep(self, query=u'КГБ'):
		for o in self.coll.find():
			if not o.has_key('edu'): continue
			for k in o['edu']:
				if k.find(query) > -1 or k.find(u'ФСБ') > -1:
					print o['name'].encode('cp866'), o['url']
					break
	def map_past(self):
		f = open('persons.csv', 'r')
		i = 0
		for l in f:
			i += 1
			if i == 1: continue
			l = l.strip().decode('utf8')
			parts = l.split(u'\t')
			if parts[-1] != u'избр.': continue
			o = self.coll.find_one({'name' : parts[0]})
			if o is not None:
				o['is_gd6'] = True			
				print o['name'].encode('cp866')
			self.coll.save(o)
			
	def faction_stats(self):
		factions = {}
		ft = {}
		f2 = {}
		for o in self.coll.find():
			f = o['faction_name']
			v = factions.get(f, [])
			v.append(o['law_real_num'])
			factions[f] = v
			if o['law_real_num'] == 0:
				v = f2.get(f, 0)
				f2[f] = v + 1
			v = ft.get(f, 0)
			ft[f] = v + 1
		for k, v in factions.items():
			total = 0
			for n in v:
				total += n
			avg = total * 1.0 / len(v)
			if not f2.has_key(k): f2[k] = 0
			print k.encode('cp866', 'ignore'), len(v), total, avg, f2[k], f2[k] * 100.0 / ft[k]


	

	def parse_workdates(self):
		for o in self.pcoll.find({'date' : {'$exists' : False}}):
			parts = o['name'].strip().split()[0].split('.')
			dt = datetime.datetime(int(parts[2]), int(parts[1]), int(parts[0]))
			o['date'] = dt
			self.pcoll.save(o)
			print dt
		return
		for o in self.coll.find():
			s = o['workdate_s'].strip()
			s = s.split(u'Дата начала полномочий:')[1].split('\t')[0].strip()
			parts = s.split()
			dt = datetime.datetime(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
			o['workdate'] = dt
			self.coll.save(o)
			print dt
		for o in self.lcoll.find():
			for a in o['attrs']:
				if a['name'] == u'Дата внесения в ГД:':
					parts = a['value'].strip().split()
					dt = datetime.datetime(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
					o['initdate'] = dt
					self.lcoll.save(o)
					print dt



	def calc_realnum(self):
		for o in self.coll.find():
			n = 0
			for l in self.lcoll.find({'persons': o['url']}):
				print l['initdate']
				if l['initdate'] >= o['workdate']:
					n += 1
			o['law_real_num'] = n
			n = 0
			for l in self.pcoll.find({'persons': o['url']}):
				if not l.has_key('date'): continue
				print l['date']
				if l['date'] >= o['workdate']:
					n += 1
			o['perf_real_num'] = n
			self.coll.save(o)

	def calc_published(self):
		for o in self.coll.find():
			o['law_stats'] = {'pub' : 0, 'preview' : 0, 'fread' : 0}
			n = 0
			for l in self.lcoll.find({'persons': o['url'], 'state' : 'published'}):
				print l['initdate']
				if l['initdate'] >= o['workdate']:
					n += 1
			o['law_stats']['pub'] = n
			for l in self.lcoll.find({'persons': o['url'], 'state' : 'preview'}):
				print l['initdate']
				if l['initdate'] >= o['workdate']:
					n += 1
			o['law_stats']['preview'] = n
			for l in self.lcoll.find({'persons': o['url'], 'state' : 'fread'}):
				print l['initdate']
				if l['initdate'] >= o['workdate']:
					n += 1
			o['law_stats']['fread'] = n
			self.coll.save(o)


	def calc_avgnum(self):
		for o in self.coll.find():
			o['avg_num'] = o['perf_real_num'] + o['law_real_num']
			self.coll.save(o)

	def depu_stats(self):
		groups = {'0' : 0, '1-20' : 0, '21-100' : 0, '101-200' : 0, '201-' : 0}
		groups_k = {'0' : 0, '1-20' : 0, '21-100' : 0, '101-200' : 0, '201-' : 0}
		for o in self.coll.find():
			n = o['avg_num'] 
			if n == 0: groups['0']  += 1
			elif n > 0 and n < 21: groups['1-20'] += 1
			elif n > 20 and n < 101: groups['21-100'] += 1
			elif n > 100 and n < 201: groups['101-200'] += 1
			elif n > 200:  groups['201-'] += 1
			if o.has_key('is_gd6') and o['is_gd6']:
				if n == 0: groups_k['0']  += 1
				elif n > 0 and n < 21: groups_k['1-20'] += 1
				elif n > 20 and n < 101: groups_k['21-100'] += 1
				elif n > 100 and n < 201: groups_k['101-200'] += 1
				elif n > 200:  groups_k['201-'] += 1
		for k, v in groups.items():
			print k, v, groups_k[k], groups_k[k] * 100.0 / v
		

	def edu_export(self):
		keys = {}
		for o in self.coll.find():
			if o.has_key('edu'):
				for k in o['edu']:
					if k[-1] == ')':
						s = k.rsplit(' ', 1)[0]
						v = keys.get(s, 0)
						keys[s] = v+ 1
		for k, v in keys.items():
			print '%s\t%d' %(k.encode('utf8'), v)

	def awards_export(self):
		keys = {}
		for o in self.coll.find():
			if o.has_key('awards'):
				for k in o['awards']:
					s = k
					v = keys.get(s, 0)
					keys[s] = v+ 1
		for k, v in keys.items():
			print '%s\t%d' %(k.encode('utf8'), v)
			
	def update_slugs(self):
		import pytils
		for o in self.coll.find():
			o['slug'] = pytils.translit.slugify(o['name'])
			print o['slug']
			self.coll.save(o)

	def update_images(self):
		f = open('deputies.csv', 'r')
		for l in f:
			l = l.strip().decode('utf8')
			parts = l.split('\t')
			url = parts[2]
			o = self.coll.find_one({'url' : url})
			if o is not None:
				print parts[0].encode('cp866', 'ignore')
				o['img'] = parts[0]
			self.coll.save(o)


if __name__ == "__main__":
	p = DepInfoParser()
#	p.get_list()
#	p.parse_lists()
#	p.process_items()
#	p.update_factions()
#	p.parse_items()
#	p.edu_export()
#	p.awards_export()
	p.faction_stats()
#	p.map_past()
#	p.find_dep()
#	p.parse_workdates()
#	p.calc_realnum()
#	p.calc_published()
#	p.calc_avgnum()
#	p.depu_stats()
#	p.update_slugs()
#	p.update_images()