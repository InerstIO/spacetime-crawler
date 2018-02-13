import logging
from datamodel.search.Fengy12_datamodel import Fengy12Link, OneFengy12UnProcessedLink, add_server_copy, get_downloaded_content
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter, ServerTriggers
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs
from uuid import uuid4

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(Fengy12Link)
@GetterSetter(OneFengy12UnProcessedLink)
@ServerTriggers(add_server_copy, get_downloaded_content)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        self.app_id = "Fengy12"
        self.frame = frame


    def initialize(self):
        self.count = 0
        l = Fengy12Link("http://www.ics.uci.edu/")
        print l.full_url
        self.frame.add(l)
        #print self.frame.get(Fengy12Link)
        #print 'fuxk'

    def update(self):
        unprocessed_links = self.frame.get(OneFengy12UnProcessedLink)
        if unprocessed_links:
            link = unprocessed_links[0]
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            try:
                links = extract_next_links(downloaded)
            except Exception as e:
                print('error:',e)
                with open('err_url', 'a') as f:
                    f.write(str(e)+' | '+str(downloaded.url)+' | '+str(downloaded.error_message)+'\n')
            #links = extract_next_links(downloaded)
            else:
                for l in links:
                    if is_valid(l):
                        self.frame.add(Fengy12Link(l))

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
    outputLinks = html.xpath('.//*/@href')
    '''
    outputLinks.append("http://www.cs.uci.edu/")
    outputLinks.append("http://www.ics.uci.edu/")
    outputLinks.append("http://www.ics.uci.edu/about/")
    outputLinks.append("www.ics.uci.edu/community/news/view_news?id=1271")
    print outputLinks
    '''
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
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False