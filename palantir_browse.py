# -*- coding: utf-8 -*-

import SeleniumControl
import time
import random
import codecs


class Palantir:
    def __init__(self):
        self.s = SeleniumControl.SeleniumControl()
    
        
    # Main function. Browses the channel and checks for
    # social media urls and whether there is an email or not.
    
    def browseChannel(self,channel):
        self.s.openUrl(channel + "/videos/")
        time.sleep(1)

        # I removed score calculation, maybe someone can add 
        # this, it needs some work. Like counting keywords from
        # all videos on a channel or something.

        #score = self.calculateScore()
        subs = self.getSubs()
        self.s.openUrl(channel + "/about")
        time.sleep(1)
                
        links = self.getLinks()
        
        # If there are no social media links:
        
        twitterlink = "no twitter"
        facebooklink = "no facebook"

        linkstring = ""
        for x in links:
            if x.find("twitter") != -1:
                twitterlink = self.makeLink(x)
            elif x.find("facebook") != -1:
                facebooklink = self.makeLink(x)
            
            else:
                linkstring += ";" + self.makeLink(x)
        linkstring = linkstring[1:]
        
        hasemail = self.hasEmail()

	country = self.getCountry()
        
        savestring = u"" + self.makeLink(channel) + u"," + country + u"," + unicode(subs) + u"," + unicode(hasemail) + u"," + unicode(facebooklink) + u"," + unicode(twitterlink) + u"," + unicode(linkstring)
        
        self.saveLink(savestring)

    def makeLink(self, x):
        return u"=HIPERŁĄCZE(\"%s\")" % x
    
    # This is really important. You have to input
    # the string on the mail label here.
    
    def hasEmail(self):
        if self.s.driver.page_source.find(u"Wyświetl adres e-mail") != -1 or \
	   self.s.driver.page_source.find(u"View email address") != -1:
            return "has email"
        return "no email"
   
    def getCountry(self):
        country = self.s.driver.find_elements_by_class_name("country-inline")
	if len(country) > 0:
	    country = u", ".join(c.text for c in country)
	else:
	    country = u"no country"
	return country
   
    # Gets all the links from about section.
    def getLinks(self):
        links = []
        el = self.s.driver.find_elements_by_class_name("about-channel-link")
        for x in el:
            links.append(x.get_attribute("href"))
        return links

  
    # Fetches the subscriber count.
    def getSubs(self):
        e = self.s.driver.find_elements_by_class_name("yt-uix-tooltip")
        for x in e:
            if x.text != "":
                return x.text.replace(" ","")
        
    # Turns the page.  
    def nextPage(self):
        el = self.s.driver.find_elements_by_class_name("yt-uix-button-content")
        for x in el:
            if x.text.find("Nast") != -1:
                x.click()
            else:
                pass

    # File I/O
    def saveLink(self,link):
        f = codecs.open("data/YoutubeChannelsParsed.Oink",'a', "utf-8")
        f.write(link+"\n")
        f.close()
        

p = Palantir()
f = open("data/YoutubeChannels.Oink",'r')
p.s.noProxy()

channels = f.read().split("\n")
f.close()

i = 0    
for x in range(0, len(channels)):
    print i
    p.browseChannel(channels[x])
    i = i + 1

