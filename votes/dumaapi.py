#!/usr/bin/env python
# coding: utf8
__author__ = 'ibegtin'

from urllib2 import urlopen
import json

DUMA_API_URL = "http://api.duma.gov.ru/api/"

class DumaAPI:
    def __init__(self, token, app_token):
        self.token = token
        self.app_token = app_token
        pass

    def listRegbody(self):
        url = DUMA_API_URL + self.token + '/regional-organs.json?app_token=%s' %(self.app_token)
        print url
        u = urlopen(url)
        arr = json.load(u)
        return arr

    def search(self, regbody=None):
        url = DUMA_API_URL + self.token + '/search.json?app_token=%s' %(self.app_token) + "&regional_subject=%s" %(regbody)
        print url
        u = urlopen(url)
        arr = json.load(u)
        return arr

def test():
    token = "1c6f64160b35398ee741e6e44dfb037f2dd0341a"
    app_token = "app730a3811ed5cced5f4774e382d9d3b195cf08b22"

    api = DumaAPI(token, app_token)
    for o in api.listRegbody():
        print o['name'], o['id']

    arr = api.search(regbody="6214700")
    print arr['count']
    for o in arr['laws']:
        print o['name']
        print o['lastEvent']['phase']['name']

if __name__ == "__main__":
    test()
