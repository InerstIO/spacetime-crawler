import logging
from datamodel.search.Fengy12YunluyEnzhengYuanzhl4_datamodel import Fengy12YunluyEnzhengYuanzhl4Link, OneFengy12YunluyEnzhengYuanzhl4UnProcessedLink, add_server_copy, get_downloaded_content
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter, ServerTriggers
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs, urljoin
from uuid import uuid4

from collections import Counter
import tldextract
import json

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(Fengy12YunluyEnzhengYuanzhl4Link)
@GetterSetter(OneFengy12YunluyEnzhengYuanzhl4UnProcessedLink)
@ServerTriggers(add_server_copy, get_downloaded_content)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        self.app_id = "Fengy12YunluyEnzhengYuanzhl4"
        self.frame = frame
        self.succ_ctr, self.invalid_ctr = 0, 0
        self.subdom = Counter()
        self.max_out = ['',0]


    def initialize(self):
        self.count = 0
        l = Fengy12YunluyEnzhengYuanzhl4Link("http://www.ics.uci.edu/")
        print l.full_url
        self.frame.add(l)

    def UrlResponse2dict(self, UrlResponse):
        return {'url':UrlResponse.url,
                'content':UrlResponse.content,
                'error_message':UrlResponse.error_message,
                'headers':UrlResponse.headers,
                'http_code':UrlResponse.http_code,
                'is_redirected':UrlResponse.is_redirected,
                'final_url':UrlResponse.final_url}

    def update(self):
        unprocessed_links = self.frame.get(OneFengy12YunluyEnzhengYuanzhl4UnProcessedLink)
        if unprocessed_links:
            link = unprocessed_links[0]
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            with open('downloaded/'+re.sub(r'[\W_]+', ' ',link.full_url),'w') as f:
                f.write(json.dumps(downloaded, default=self.UrlResponse2dict))
            try:
                links = extract_next_links(downloaded)
            except Exception as e:
                #self.invalid_ctr += 1 # count invalid links
                print('error:',e)
                with open('err_url', 'a') as f:
                    f.write(str(e)+' | '+str(downloaded.url)+' | '+str(downloaded.error_message)+'\n')
            #links = extract_next_links(downloaded)
            else:
                self.succ_ctr += 1 # count number of successfully downloaded links
                ext = tldextract.extract(link.full_url) 
                subdomain = '.'.join(ext[:2])
                self.subdom[subdomain] += 0 # count number of URLs from subdomains ==> move into if is_valid
                if self.max_out[1] < len(links): # keep track of the page with most out links
                    self.max_out[1] = len(links)
                    self.max_out[0] = link.full_url
                for l in links:
                    if is_valid(l):
                        self.frame.add(Fengy12YunluyEnzhengYuanzhl4Link(l))
                        self.subdom[subdomain] += 1 # count number of URLs from subdomains
                    else:
                        #print l+str('!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                        self.invalid_ctr += 1 # count invalid links
                if not self.succ_ctr%10: # record analytics data periodically
                    with open('analytics', 'w') as f:
                        f.write('# crawled: ' + str(self.succ_ctr)+'\n')
                        f.write('subdomains:\n')
                        for item in self.subdom.items():
                            f.write('   '+item[0] + ', ' + str(item[1]) + '\n')
                        f.write('\ninvalid links: '+str(self.invalid_ctr)+'\n\n')
                        f.write('page with the most out links: '+str(self.max_out[0])+' ('+str(self.max_out[1])+')')

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    html = etree.HTML(rawDataObj.content)
    links = html.xpath('.//*/@href')

    outputLinks = [urljoin(rawDataObj.final_url if rawDataObj.is_redirected else rawDataObj.url,l) for l in links] # get absolute form

    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        # deal with traps including Repeating Directories, Extra Directories, Calendars and sessionID
        if re.match(r"^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$", url) or \
            re.match(r"^.*(/misc|/sites|/all|/themes|/modules|/profiles|/css|/field|/node|/theme){3}.*$", url) or \
            re.match(r"^.*calendar.*$", url) or \
            re.match(r"^.*(jsessionid|sid|affid)",parsed.path.lower()):
            return False
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower() + parsed.query.lower()) # made some modification here

    except TypeError:
        print ("TypeError for ", parsed)
        return False
