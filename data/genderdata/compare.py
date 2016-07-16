# coding: utf-8
import urllib2, urllib, sys
import socket
import lxml.html           
import simplejson as json
import datetime
import csv
from BeautifulSoup import BeautifulStoneSoup, BeautifulSoup
from pymongo import Connection

socket.setdefaulttimeout(15)


class DepInfoParser:
	def __init__(self):
		pass

	def load_list(self, fname, otype=1):
		f = open(fname, 'r')
		names = []
		for l in f:
			l = l.strip().decode('utf8')			
			parts = l.split('\t')
			if otype == 1:
				names.append(parts[0])
			else:
				names.append(parts[1])
		return names

	def load_gender(self, fname='gender.txt'):
		f = open(fname, 'r')
		names = {}
		for l in f:
			l = l.strip().decode('utf8')			
			parts = l.split('\t')
			names[parts[0]] = parts[1]
		return names


	def load_fulllist(self, fname):
		f = open(fname, 'r')
		names = {}
		for l in f:
			l = l.strip().decode('utf8')			
			parts = l.split('\t')
			names[parts[1]]= {'faction_url' : parts[4]}
		return names



	def compare_lists(self, fname1, fname2, otype1, otype2):
		l1 = self.load_list(fname1, otype=otype1)
		l2 = self.load_list(fname2, otype=otype2)
		res = set(l1).intersection(l2)
		share = len(res) * 100.0 / len(l2)
		print len(l1), len(l2), len(res), share
		pass

	def get_gender(self, name):
		params = urllib.urlencode({'text' : name.encode('utf8')})
		url = "http://apibeta.skyur.ru/names/parse/?%s" % params
		f = urllib2.urlopen(url.encode('utf8'))
		data = f.read()
		f.close()
		return json.loads(data)


	def generate_gender(self):
		l1 = self.load_list('deputies_cv1.txt', otype=1)
		l2 = self.load_list('deputies_cv2.txt', otype=1)
		l3 = self.load_list('deputies_cv3.txt', otype=1)
		l4 = self.load_list('deputies_cv4.txt', otype=1)
		l5 = self.load_list('deputies_cv5.txt', otype=2)
		l6 = self.load_list('deputies_cv6.txt', otype=2)
		full = set(l1)
		f = open('gender.txt', 'w')
		for l in [l2,l3,l4,l5,l6]:
			full = full.union(l)
		for name in full:
			gender = self.get_gender(name)
			if gender['parsed'] == True:
				g = gender['gender']
			else:
				g = 'n'
			f.write((u'%s\t%s\n' %(name, g)).encode('utf8'))
			print (u'%s\t%s' %(name, g)).encode('cp866')
		f.close()
		



	def compare_fractions(self):
		FRACTIONS = ['http://www.duma.gov.ru/structure/factions/kprf/', 'http://www.duma.gov.ru/structure/factions/er/', 'http://www.duma.gov.ru/structure/factions/sr/', 'http://www.duma.gov.ru/structure/factions/ldpr/']
		fulllist = self.load_fulllist('deputies_cv6.txt')
		l1 = self.load_list('deputies_cv1.txt', otype=1)
		l2 = self.load_list('deputies_cv2.txt', otype=1)
		l3 = self.load_list('deputies_cv3.txt', otype=1)
		l4 = self.load_list('deputies_cv4.txt', otype=1)
		l5 = self.load_list('deputies_cv5.txt', otype=2)
		l6 = self.load_list('deputies_cv6.txt', otype=2)
		alllists = [l1, l2, l3, l4, l5, l6]
		total = len(l6)
		total_factions = {}
		for n in FRACTIONS:
			total_factions[n] = 0
		for n in l6:
			faction = fulllist[n]['faction_url']
			total_factions[faction] += 1
		for k, v in total_factions.items():
			print '-', k, v
		for i in range(0, len(alllists)-1, 1):
			factions = {}
			for n in FRACTIONS:
				factions[n] = 0
			l = alllists[i]
			res = set(l)
			for n in range(i+1,len(alllists), 1):
				res = res.intersection(alllists[n])							
			print i+1, len(res), len(res) * 100.0 / total
			for n in res:
				faction = fulllist[n]['faction_url']
				factions[faction] += 1
			for k, v in factions.items():
				print '-', k, v, total_factions[k], v*100.0 / total_factions[k]
				

		allnot6 = set(l1)
		for n in [l2, l3, l4, l5]:
			allnot6 = allnot6.union(n)
		res = set(l6).intersection(allnot6)
		print len(res), len(res) * 100.0 / total



	def compare_alllists(self):
		l1 = self.load_list('deputies_cv1.txt', otype=1)
		l2 = self.load_list('deputies_cv2.txt', otype=1)
		l3 = self.load_list('deputies_cv3.txt', otype=1)
		l4 = self.load_list('deputies_cv4.txt', otype=1)
		l5 = self.load_list('deputies_cv5.txt', otype=2)
		l6 = self.load_list('deputies_cv6.txt', otype=2)
		alllists = [l1, l2, l3, l4, l5, l6]
		total = len(l6)
		for i in range(0, len(alllists)-1, 1):
			l = alllists[i]
			res = set(l)
			for n in range(i+1,len(alllists), 1):
				res = res.intersection(alllists[n])
			print i+1, len(res), len(res) * 100.0 / total

		allnot6 = set(l1)
		for n in [l2, l3, l4, l5]:
			allnot6 = allnot6.union(n)
		res = set(l6).intersection(allnot6)
		print len(res), len(res) * 100.0 / total
#			if i == 0:
#				for name in res:
#					print name.encode('utf8')
#		res = set(l1).intersection(l2)
#		share = len(res) * 100.0 / len(l2)
#		print len(l1), len(l2), len(res), share
		pass

	def calc_gender(self):
		genderlist = self.load_gender('gender.txt')
		l1 = self.load_list('deputies_cv1.txt', otype=1)
		l2 = self.load_list('deputies_cv2.txt', otype=1)
		l3 = self.load_list('deputies_cv3.txt', otype=1)
		l4 = self.load_list('deputies_cv4.txt', otype=1)
		l5 = self.load_list('deputies_cv5.txt', otype=2)
		l6 = self.load_list('deputies_cv6.txt', otype=2)
		alllists = [l1, l2, l3, l4, l5, l6]
		i = 0
		for l in alllists:
			i += 1
			total = len(l)
			genders = {'m' : 0, 'f' : 0}
			for n in l:
				genders[genderlist[n]] += 1			
			for k, v in genders.items():
				print i, k, v, v * 100.0 / total



if __name__ == "__main__":
	p = DepInfoParser()
#	p.compare_lists(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
#	p.compare_alllists()
#	p.compare_fractions()
#	p.generate_gender()
	p.calc_gender()
