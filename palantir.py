# -*- coding: utf-8 -*-

import signal
import re
import unicodecsv as csv
import time
import random
import urllib
import urlparse
import logging
from argparse import ArgumentParser
from collections import deque
from selenium import webdriver
from frozendict import frozendict


class WorkItem(object):

    def __init__(self, queue):
        self.queue = queue

    def __repr__(self):
        return "WorkItem(queue=%s)" % repr(self.queue)

    def __unicode__(self):
        return self.__repr__()

    def is_link(self):
        return False

    def __eq__(self, other):
        return type(self) == type(other) and self.queue == other.queue

    def __ne__(self, other):
        return type(self) != type(other) or self.queue != other.queue

    def __hash__(self):
        return hash(self.queue)


class LinkItem(WorkItem):

    def __init__(self, queue, link):
        super(LinkItem, self).__init__(queue)
        self.link = link

    def is_link(self):
        return True

    def __repr__(self):
        return "LinkItem(queue=%s, link=%s)" % (
            repr(self.queue),
            repr(self.link),
        )

    def __eq__(self, other):
        return type(self) == type(other) and self.queue == other.queue and self.link == other.link

    def __ne__(self, other):
        return type(self) != type(other) or self.queue != other.queue or self.link != other.link

    def __hash__(self):
        return hash(self.queue) * 13 + hash(self.link) * 7


class OutputItem(WorkItem):

    def __init__(self, queue, output):
        super(OutputItem, self).__init__(queue)
        self.output = frozendict(output)

    def __repr__(self):
        return "OutputItem(queue=%s, output=%s)" % (
            repr(self.queue),
            repr(self.output),
        )

    def __eq__(self, other):
        return type(self) == type(other) and self.queue == other.queue and self.output == other.output

    def __ne__(self, other):
        return type(self) != type(other) or self.queue != other.queue or self.output != other.output

    def __hash__(self):
        return hash(self.queue) * 13 + hash(self.output)


class Crawler(object):

    def __init__(self, driver):
        self.driver = driver
        self.queue = deque()
        self.already_processed = set()

    def crawl(self):
        logging.info(u"Started crawling")
        for item in self._get_start_items():
            logging.info(u"Start item: " + repr(item))
            if self._is_result_item(item):
                yield item
            else:
                self._push_item(item)
        while len(self.queue) > 0:
            item = self._pop_item()
            logging.info(u"Processing item: " + repr(item))
            if item.is_link():
                self.driver.get(item.link)
            for new_item in self._process_item(item):
                logging.info(u"New item: " + repr(new_item))
                if self._is_result_item(new_item):
                    yield new_item
                else:
                    self._push_item(new_item)

    def _pop_item(self):
        return self.queue.popleft()

    def _push_item(self, item):
        if item in self.already_processed:
            return
        self.already_processed.add(item)
        self.queue.append(item)

    def _is_result_item(self, item):
        return item.queue == 'result'

    def _get_start_items(self):
        raise NotImplementedError()

    def _process_item(self, item):
        if True:
            raise NotImplementedError()
        yield


class YoutubeChannelCrawler(Crawler):

    def __init__(self, driver, term, page_count=100):
        super(YoutubeChannelCrawler, self).__init__(driver)
        self.term = term
        self.page_count = page_count

    def _get_start_items(self):
        params = urllib.urlencode({'q': self.term})
        link = urlparse.urlunparse(('https', 'www.youtube.com', '/results', '', params, ""))
        yield LinkItem('result_list', link)

    def _process_item(self, item):
        if item.queue == 'result_list':
            for item in self._process_result_list(item):
                yield item
        elif item.queue == 'channel_page':
            for item in self._process_channel_page(item):
                yield item
        else:
            logging.warn("Unrecognized work item queue: %s" % item.queue)

    def _process_result_list(self, item):
        for i in xrange(self.page_count):
            self._next_page()
            self._random_wait()

        channel_elems = self._get_channel_elems()
        channel_urls = self._get_elems_urls(channel_elems)

        for url in channel_urls:
            yield LinkItem('channel_page', url)

    def _wait_for(self, func, timeout=20):
        start_time = time.time()
        while True:
            if func():
                return True
            time.sleep(0.5)
            if time.time() - start_time > timeout:
                return False

    CHANNEL_TITLE_COLUMN = 'channel_title'
    CHANNEL_LINK_COLUMN = 'channel_link'
    SUB_COUNT_COLUMN = 'sub_count'
    COUNTRY_COLUMN = 'country'
    EMAIL_COLUMN = 'email'
    TWITTER_COLUMN = 'twitter'
    FACEBOOK_COLUMN = 'facebook'
    TWITCH_COLUMN = 'twitch'

    OUTPUT_COLUMNS = (
        CHANNEL_TITLE_COLUMN,
        CHANNEL_LINK_COLUMN,
        SUB_COUNT_COLUMN,
        COUNTRY_COLUMN,
        EMAIL_COLUMN,
        TWITTER_COLUMN,
        FACEBOOK_COLUMN,
        TWITCH_COLUMN,
    )

    def _process_channel_page(self, item):
        info_tab_elems = self.driver.find_elements_by_xpath('//*[contains(@class, "paper-tab") and text()[contains(.,"Inform")]]')
        if len(info_tab_elems) == 0:
            return

        info_tab = info_tab_elems[0]
        info_tab.click()

        def has_info_desc():
            elems = self.driver.find_elements_by_css_selector('.subheadline.style-scope.ytd-channel-about-metadata-renderer')
            return len(elems) > 0

        self._wait_for(has_info_desc)

        time.sleep(1)

        info = {}

        info[self.CHANNEL_TITLE_COLUMN] = self._get_channel_title()
        info[self.CHANNEL_LINK_COLUMN] = item.link
        info[self.SUB_COUNT_COLUMN] = self._get_sub_count()
        info[self.COUNTRY_COLUMN] = self._get_country()
        info[self.EMAIL_COLUMN] = self._get_email()
        info[self.TWITTER_COLUMN] = self._get_twitter_url()
        info[self.FACEBOOK_COLUMN] = self._get_facebook_url()
        info[self.TWITCH_COLUMN] = self._get_twitch_url()

        yield OutputItem('result', info)

    def _get_channel_title(self):
        return self.driver.find_element_by_id('channel-title').text

    def _get_sub_count(self):
        sub_count_elem = self.driver.find_element_by_id('subscriber-count')
        sub_count = sub_count_elem.text
        sub_count = re.sub(ur'[^0-9]', u'', sub_count)
        try:
            return long(sub_count)
        except ValueError:
            return None

    def _get_email(self):
        email_elems = self.driver.find_elements_by_xpath('//a[@id="email"]')
        if len(email_elems) > 0:
            return email_elems[0].get_attribute('innerHTML')
        else:
            return None

    def _get_country(self):
        country_elems = self.driver.find_elements_by_xpath('(//td[contains(@class,"ytd-channel-about-metadata-renderer")]/yt-formatted-string)[3]')
        if len(country_elems) > 0:
            return country_elems[0].text.strip()
        else:
            return None

    def _get_social_url(self, domain):
        link_elems = self.driver.find_elements_by_xpath('//a[contains(@href,"%s")]' % domain)
        link_url = None
        if len(link_elems) > 0:
            link_url = link_elems[0].get_attribute("href")
        return link_url

    def _get_twitter_url(self):
        return self._get_social_url('twitter.com')

    def _get_facebook_url(self):
        return self._get_social_url('facebook.com')

    def _get_twitch_url(self):
        return self._get_social_url('twitch.com')

    def _get_elems_urls(self, elems):
        result = []
        for el in elems:
            url = el.get_attribute("href")
            result.append(url)
        return result

    def _get_video_elems(self):
        return self.driver.find_elements_by_xpath("//a[starts-with(@href,'/watch?')]")

    def _get_channel_elems(self):
        return self.driver.find_elements_by_xpath("//a[starts-with(@href,'/user/')]")

    def _next_page(self):
        self.driver.execute_script("window.scrollBy(0,1000)", "")

    def _random_wait(self):
        time.sleep(random.uniform(0.5, 1))


class ResultDatabase(object):

    def __init__(self, columns, results=()):
        self.columns = columns
        self.column_set = set(columns)
        self.results = list()
        self.result_set = set()
        for r in results:
            self.add_result(r)

    def has_result(self, result):
        return result in self.result_set

    def add_result(self, result):
        if self.has_result(result):
            return
        for c in result:
            if c not in self.column_set:
                self.column_set.add(c)
                self.columns.append(c)
        if isinstance(result, dict):
            result = frozendict(result)
        elif isinstance(result, set):
            result = frozenset(result)
        self.result_set.add(result)
        self.results.append(result)

    def sort_by(self, name, reverse=False):
        assert name in self.column_set
        self.results.sort(key=lambda r: r.get(name), reverse=reverse)

    def as_csv(self):
        for r in self.results:
            row = []
            for c in self.columns:
                v = r.get(c)
                if v is None:
                    v = ""
                row.append(v)
            yield row


def create_database():
    return ResultDatabase(YoutubeChannelCrawler.OUTPUT_COLUMNS)


def crawl(search_term, page_count, database):
    driver = webdriver.Chrome()
    crawler = YoutubeChannelCrawler(driver, search_term, page_count=page_count)
    for item in crawler.crawl():
        database.add_result(item.output)
    database.sort_by(YoutubeChannelCrawler.SUB_COUNT_COLUMN, reverse=True)


def save_as_csv(output_file, database):
    with open(output_file, 'wb') as f:
        writer = csv.writer(f)
        for row in database.as_csv():
            writer.writerow(row)


def main():
    logging.basicConfig(level=logging.INFO)

    parser = ArgumentParser()
    parser.add_argument("-o", "--output", dest="output_file",
                        help="write output to FILE", metavar="FILE")
    parser.add_argument("-s", "--search", dest="search_term",
                        help="search term to look for on YouTube", metavar="TERM")
    parser.add_argument("-p", "--page-count", dest="page_count", type=int, default=10,
                        help="number of scroll-downs to perform on infinite result list", metavar="COUNT")

    args = parser.parse_args()
    database = create_database()

    d = {}

    def save_results():
        if d.get('already_saved'):
            return
        d['already_saved'] = True
        logging.info(u"Saving to CSV")
        save_as_csv(args.output_file, database)

    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, save_results)
    signal.signal(signal.SIGTERM, save_results)

    try:
        crawl(args.search_term, args.page_count, database)
    finally:
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)

        save_results()


if __name__ == "__main__":
    main()
