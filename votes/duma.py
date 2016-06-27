#!/usr/bin/env python
# -*- coding: utf8 -*-
import json
import mechanize
from BeautifulSoup import BeautifulSoup
from pymongo import Connection
from sets import Set

VOTE_URL = 'http://vote.duma.gov.ru'
VOTING_YES = -1
VOTING_NO = 1
VOTING_ABSENT = 2
VOTING_ABSTAIN = 0


def map_results(key):
    if key == VOTING_YES:
        return "yes"
    if key == VOTING_NO:
        return "no"
    if key == VOTING_ABSENT:
        return "absent"
    if key == VOTING_ABSTAIN:
        return "abstain"
    return "unknown (%d)" % (key)


class Bot:
    """Duma bot"""

    def __init__(self):
        self.conn = Connection()
        self.db = self.conn['duma']
        self.vcoll = self.db['raw']
        self.pvotecoll = self.db['personvotes']
        self.fcoll = self.db['factions']
        self.votingscoll = self.db['votings']
        self.dcoll = self.db['deputies']
        self.dcoll.ensure_index('name', 1)
        self.dcoll.ensure_index('faction', 1)
        self.dcoll.ensure_index('url', 1)
        self.vcoll.ensure_index('href', 1)

    def get_object_fields(self, prefix, object):
        fields = Set()
        for k, v in object.items():
            if type(v) == type({}):
                fields.update(self.get_object_fields(prefix=prefix + k + '.', object=v))
            else:
                fields.add(prefix + k)
        return fields

    def get_fields(self, coll, limit=1000):
        allfields = Set([])
        if limit is not None or limit != 0:
            objs = coll.find().limit(limit)
        else:
            objs = coll.find()
        for o in objs:
            fields = self.get_object_fields(prefix="", object=o)
            allfields.union_update(fields)

        allfields = list(allfields)
        allfields.sort()
        return allfields

    def get_field_value(self, object, fieldname):
        parts = fieldname.split('.')
        if len(parts) == 1:
            v = object[parts[0]]
        else:
            curr = object
            for k in parts:
                if curr.has_key(k):
                    curr = curr[k]
                else:
                    curr = ""
                    break
            v = curr
        return unicode(v)

    def db_dump(self, collname):
        print 'Dumping', collname
        f = open(collname + '.csv', 'w')
        fields = self.get_fields(self.db[collname], limit=1000)
        f.write(('\t'.join(fields)).encode('utf8') + "\n")
        for o in self.db[collname].find():
            values = []
            for fn in fields:
                values.append(self.get_field_value(o, fn))
            f.write(('\t'.join(values)).encode('utf8') + "\n")
        f.close()

    def dump_all(self):
        self.db_dump('deputies')
        self.db_dump('factions')
        self.db_dump('votings')
        self.db_dump('personvotes')

    def generate_db(self):
        """Generates subcollections"""
#        self.votingscoll.remove()
#        self.pvotecoll.remove()
        self.fcoll.remove()
        n = 0
        for o in self.vcoll.find():
            break
            n += 1
            print 'Processing %d' % (n)
            print '- saving voting'
            voting = {'title': o['title'], 'url': o['url'], 'result': 'success' if o[
                'color'] == 'green' else 'failed', 'datet': o['datet']}
            voting['voting_id'] = o['href'].rsplit('/', 1)[-1]
            voting['asozd_url'] = o['asozd_url'] if o.has_key('asozd_url') else None
            voting['number'] = o['n_url'].rsplit('=', 1)[-1] if o.has_key('n_url') else None
            voting['text_votes'] = o['vote_result']
            voting['text_num'] = o['vote_num']
            voting['convocation'] = 'AAAAAAA6'
            for k in ['votes', 'votes_share', 'factions', 'factions_share', 'text']:
                voting[k] = o[k]
                self.votingscoll.save(voting)
            print '- saving vote info'
            for p in o['raw']:
                perv = {}
                perv['voting_id'] = voting['voting_id']
                perv['name'] = p['sortName']
                perv['dep_id'] = p['url'].rsplit('=', 1)[-1]
                perv['result'] = map_results(p['result'])
                perv['faction'] = p['faction']
                perv['letter'] = p['letter']
                self.pvotecoll.save(perv)
        print "Processing factions data"
        factions = [u'ЕР', u'СР', u'КПРФ', u'ЛДПР']
        f_data = {}
        for k in factions:
            f_data[k] = {'name': k, 'vote_stats': {'abstain': 0,
                                                   'absent': 0, 'no': 0, 'yes': 0}, 'deputies': 0}
        for d in self.dcoll.find():
            v = f_data.get(d['faction'])
            v['deputies'] += 1
            for k in ['abstain', 'absent', 'no', 'yes']:
                if d['vote_stats'].has_key(k):
                    v['vote_stats'][k] += d['vote_stats'][k]
            f_data[d['faction']] = v
        print 'Calc share'
        for k, v in f_data.items():
            v['votes_share'] = {}
            for key, val in v['vote_stats'].items():
                v_sh = float(val) / v['deputies']
                v['votes_share'][key] = v_sh
            f_data[k] = v
        print 'Saving factions'
        for k, v in f_data.items():
            print v
            self.fcoll.save(v)

    def process_listpage(self, br, num):
        url = VOTE_URL + '?page=%d' % num
        resp = br.open(url)
        data = resp.read()
        soup = BeautifulSoup(data)
        tags = soup.findAll('div', attrs={'class': 'item'})
        items = []
        for tag in tags:
            left = tag.find('div', attrs={'class': 'item-left'})
            atag = left.find('a')
            item = []
            item.append(atag['href'])
            item.append(atag.text)
            item.append(VOTE_URL + atag['href'])
            right = tag.find('div', attrs={'class': 'item-right'})
            divs = right.findAll('div')
            color = divs[0]['class']
            item.append(color)
            item.append(divs[0].text)
            item.append(divs[1].text)
            items.append(item)
        return br, items

    def process(self):
        keys = ['href', 'name', 'url', 'color', 'vote_result', 'votes']
        print '\t'.join(keys)
        br = mechanize.Browser()
        for n in range(1, 159):
            br, items = self.process_listpage(br, n)
            for item in items:
                r = self.vcoll.find_one({'href': item[0]})
                if r is not None:
                    continue
                s = ('\t'.join(item)).encode('utf8')
                val = {}
                val['href'] = item[0]
                val['title'] = item[1]
                val['url'] = item[2]
                val['color'] = item[3]
                val['vote_result'] = item[4]
                val['vote_num'] = item[5]
                self.vcoll.save(val)
                print s

    def process_page(self, br, url):
        """Process page with data about representatives"""
        resp = br.open(url)
        data = resp.read()
        soup = BeautifulSoup(data)
        text = soup.find('h1').text
        head = soup.find('div', attrs={'class': 'date-p'})
        datet = head.find('span').text
        record = {'text': text, 'datet': datet}
        hrefs = head.findAll('a')
        if len(hrefs) > 0:
            nurl = hrefs[0]['href']
            record['n_url'] = nurl
        if len(hrefs) > 1:
            record['asozd_url'] = hrefs[1]['href']
        dep_t = data.split('deputiesData = ', 1)[1]
        dep_t = dep_t.split(';', 1)[0]
        jsd = json.loads(dep_t)
        record['raw'] = jsd
        return record

    def process_deep_data(self):
        """Extract data from vote pages"""
        br = mechanize.Browser()
        # {'processed' : {'$exists' : False}}):#{'processed' : False}):
        for o in self.vcoll.find({'raw': {'$exists': False}}):
            rec = self.process_page(br, o['url'])
            o.update(rec)
            o['processed'] = True
            print o['href'], 'processed'
            self.vcoll.save(o)

    def enrich(self):
        for o in self.vcoll.find():
            factions = {}
            for p in o['raw']:
                v = factions.get(p['faction'], {})
                rname = map_results(p['result'])
                r = v.get(rname, 0)
                v[rname] = r + 1
                factions[p['faction']] = v
            er = factions[u'ЕР']
            o['factions'] = factions
            print factions
            self.vcoll.save(o)

    def importReps(self):
        deps = []
        for o in self.vcoll.find():
            for p in o['raw']:
                name = p['sortName']
                if name in deps:
                    continue
                deps.append(name)
                dep = {'name': p['sortName'], 'faction': p['faction'],
                       'letter': p['letter'], 'url': p['url']}
                o = self.dcoll.find_one({'name': dep['name']})
                print o
                if o is None:
                    self.dcoll.save(dep)

    def calcFactionShare(self):
        f_data = {u"ЕР": [], u'КПРФ': [], u'ЛДПР': [], u'СР': []}
        n = 0
        for o in self.vcoll.find():
            s_data = {'yes': 0, 'no': 0, 'abstain': 0, 'absent': 0}
            n += 1
            total = 0
            faction_share = {}
            for k, v in o['factions'].items():
                rec = {}
                tot_f = 0
                for vk, vv in v.items():
                    tot_f += vv
                for vk, vv in v.items():
                    rec[vk] = (100.0 * vv) / tot_f
                faction_share[k] = rec
#                print k, rec
                total += tot_f
            o['factions_share'] = faction_share
            o['total_votes'] = total
            for p in o['raw']:
                vote_key = map_results(p['result'])
                s_data[vote_key] += 1
            o['votes'] = s_data
            share_data = {}
            for k, v in s_data.items():
                share_data[k] = (100.0 * v) / total
            o['votes_share'] = share_data
            print share_data
            self.vcoll.save(o)

    def factionStats(self):
        keys = ['abstain', 'no', 'yes', 'absent']
        f_data = {u"ЕР": [], u'КПРФ': [], u'ЛДПР': [], u'СР': []}
        n = 0
        for o in self.vcoll.find():
            n += 1
            if n % 100 == 0:
                print n
            if n > 200:
                break
            for k, v in o['factions'].items():
                f_data[k].append(v)
        for k, v in f_data.items():
            for rec in v:
                total = 0
                rec_share = {}
                for vkey in rec.keys():
                    total += rec[vkey]
                for vkey in rec.keys():
                    rec_share[vkey] = (100.0 * rec[vkey]) / total
                print k, rec_share

    def calcSpecial(self):
        n = 0
        total = 0
        nt = 0
        for o in self.vcoll.find():
            vs = o['votes_share']
            total += 1
            if vs['absent'] > 90:
                nt += 1
                print o['title']
                print '-', o['url']
                print '-', vs['absent']
            if vs['absent'] > 50:
                n += 1
        print (100.0 * n) / total
        print (100.0 * nt) / total

    def calcIncoherent(self):
        n = 0
        total = 0
        nt = 0
        deputies = {}
        for o in self.vcoll.find():
            n += 1
            if n % 100 == 0:
                print 'Processing', n
            er = o['factions_share'][u'ЕР']
            if er.get('yes') == 100:
                #                print '- skip all yes'
                continue
            if er.get('absent') == 100:
                #                print '- skip all absent'
                continue
            if er.has_key('no') and er['no'] == 100:
                #                print '- skip all no'
                continue
            for k in ['yes', 'no', 'absent', 'abstain']:
                er[k] = er[k] if er.has_key(k) else 0
            if er['yes'] > 50:
                look_result = VOTING_YES
            elif er['absent'] + er['no'] > 50:
                look_result = VOTING_NO
            for p in o['raw']:
                if p['faction'] != u'ЕР':
                    continue
                if look_result == VOTING_YES:
                    if p['result'] in [VOTING_YES, VOTING_ABSENT]:
                        continue
                if look_result == VOTING_NO:
                    if p['result'] in [VOTING_NO, VOTING_ABSENT]:
                        continue
                v = deputies.get(p['sortName'], 0)
                v += 1
                deputies[p['sortName']] = v
                print p['sortName']
                print '-', o['title'], o['url']
        for k, v in deputies.items():
            print k, v

    def buildRepProfiles(self):
        deps = {}
        n = 0
        for o in self.vcoll.find():
            n += 1
            if n % 100 == 0:
                print n
            for p in o['raw']:
                v = deps.get(p['sortName'], {})
                rname = map_results(p['result'])
                r = v.get(rname, 0)
                v[rname] = r + 1
                deps[p['sortName']] = v
        for k, v in deps.items():
            o = self.dcoll.find_one({'name': k})
            print k, o
            o['vote_stats'] = v
            self.dcoll.save(o)
        pass

if __name__ == "__main__":
    bot = Bot()
#    bot.generate_db()
    bot.dump_all()
#    bot.calcIncoherent()
#    bot.calcSpecial()
#    bot.calcFactionShare()
#    bot.factionStats()
#    bot.importReps()
#    bot.buildRepProfiles()
#    bot.process()
#    bot.enrich()
#    bot.process_deep_data()
