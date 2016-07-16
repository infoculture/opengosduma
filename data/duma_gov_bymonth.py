# coding: utf-8
# �����祭�� ������ � ����⨪� ������⢮��᪮�� ����� � ᠩ� ���㤠��⢥���� ��� ���ᨩ᪮� �����樨
import urllib2
import socket
import lxml.html           
import csv

def writeline(d, keys):
    arr = []
    for k in keys:
        arr.append(unicode(d[k]))
    return ('\t'.join(arr)).encode('utf8')

# �㭪�� ��� �����祭�� ������ �� ��࠭��� ����⨪�
def parse_page(url, data={}):
    f = urllib2.urlopen(url)
    h = f.read()
    root = lxml.html.fromstring(h)
    n = 0
# ����� �ࠢ��⥫쭮 ���� ����������� ⠪ ��� ��室���� � ����� ⠡���
    for tr in root.cssselect("div[class='table-data td-filter'] table tr"):
        tds = tr.cssselect("td")
        if len(tds) < 4: continue
        n += 1
# ������ ᫮����
        d = {'name' : tds[0].text_content().strip(), 'num_total' : int(tds[1].text_content()), 'num_fedconst_laws' : int(tds[2].text_content()), 'num_ratif' : int(tds[3].text_content()), 'order' : n}   
# �� �����⢥��� ���� 㭨���쭮�� �� ��뫪� ��࠭��� ����⨪� � ����� ��ப� � ���祭�ﬨ �������஢
        d['uniq'] = str(d['order']) + '_' + data['url']
        data.update(d)        
        print writeline(data, keys=['year', 'month', 'url', 'name', 'num_total', 'num_fedconst_laws', 'num_ratif', 'order', 'uniq'])
# ���࠭塞 १����
    
socket.setdefaulttimeout(10)

def parse_all():
    keys=['year', 'month', 'url', 'name', 'num_total', 'num_fedconst_laws', 'num_ratif', 'order', 'uniq']
    print '\t'.join(keys)
    # �� ���� ���� ��࠭��� � ��������� �� ��� ᯨ᮪ ��� ��⠫��� �� ������ ��� ���� ������� ���祭�� �������஢
    f = urllib2.urlopen("http://www.duma.gov.ru/legislative/statistics/?type=month&v=01.2010")
    html = f.read()
    # �����ࠥ� ��࠭��� � ������� lxml
    root = lxml.html.fromstring(html)
    # ��뫪� �� ����⨪� �� ����栬 ����� ������� ��� ⠪�� ����ᮬ
    for td in root.cssselect("td[class='month']"):
        hr = td.cssselect("a")

    # �� �⭮�⥫쭮� ��뫪� �� ����砥�: �����, ��� � ��᮫���� ��뫪�
        s = hr[0].attrib['href'].split('v=')[1]
        month = int(s.split('.')[0])
        year = int(s.split('.')[1])
        url = 'http://www.duma.gov.ru' + hr[0].attrib['href']
    # �� ����� ���ᨬ � ᫮����
        data = {
          'url' : url,
          'month' : month,
          'year' : year
        }
    # ����᪠�� �����祭�� ������ �� ��࠭���, �஬� 1996 ����, ⠬ ��������� �����-� �訡��       
        data = parse_page(url, data)


if __name__ == "__main__":
    parse_all()

