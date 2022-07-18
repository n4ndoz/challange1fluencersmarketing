from requests_html import HTMLSession
import requests_html
import bs4
from lxml.html.soupparser import fromstring
from lxml.etree import tostring
import lxml
from datetime import datetime
import re

class Scraper():

    def setup(self):
        return None
    def crawl(self):
        return None
    def scraper(self):
        return None

class TweetCrawler(Scraper):
    def __init__(self, user: str):
        self.user = user
        self.search_url = f"https://twitter.com/search?q=(from%3A{user})&src=typed_query&f=live"
        self.tweets_data = None
    
    ## Utils
    def parse_tweet_engagement(self, tweet) -> dict:
        # tweet is an lxml element
        # grabs engagement data 
        tweet = tweet.xpath(".//*[contains(concat(' ', normalize-space(@aria-label), ' '), 'replies')]")[0]
        data = dict()
        data['replies'],data['retweets'],data['likes'] = [int(s) for s in tweet.attrib['aria-label'].split() if s.isdigit()]    
        return data

    def parse_tweet_timestamp(self, tweet) -> dict:
        return {'timestamp': tweet.xpath("string(.//div[@data-testid='User-Names']//time/@datetime)")}

    def fetch_tweet_id(self, tweet):
        # tweet is an lxml HtmlElement object
        # id is the status id, from the tweet url
        pattern = f".//a[contains(concat(' ', normalize-space(@href), ' '), '/status/') and @dir='auto']/@href"
        try:
            tweet = re.sub("/.*/status/",'',tweet.xpath(pattern)[0])
        except IndexError as e:
            tweet = None
        return tweet

    def parse_tweet(self, tweet: lxml.html.HtmlElement):
        # tweet is an lxml HtmlElement
        tweet_data = dict()
        try:
            data = self.parse_tweet_engagement(tweet)
        except Exception as e:
            print(e)
            data = {k:0 for k in ['replies', 'retweets', 'likes']}
        tweet_data.update(data)

        try:
            data = self.parse_tweet_timestamp(tweet)
        except Exception as e:
            print(e)
            data = {'timestamp': ''}
        tweet_data.update(data)

        return tweet_data

    #####

    def scrape(self, response: requests_html.HTMLResponse, maxposts: int=5, maxattempts: int=10):
        # in this case response is from a user search
        # so we MUST iterate over all possible tweets fetched from the initial search
        data_tweets = dict()
        attempts = 0
        while True:
            response.html.render(keep_page=True,scrolldown=2000)
            tweets = fromstring(response.html.html).xpath(".//article[@data-testid='tweet']")
            for tweet in tweets:
                tweet_id = self.fetch_tweet_id(tweet)
                if tweet_id:
                    if not tweet_id in data_tweets.keys():
                        data_tweets[tweet_id] = self.parse_tweet(tweet)

            attempts+=1
            
            if attempts >= maxattempts or len(data_tweets) >= maxposts: break
        response.html.render(keep_page=False) # this will enable page closing?
        return data_tweets

    def crawl(self):
        with HTMLSession() as session:
            print('## Fetching Tweets')
            r = session.get(self.search_url)
            r.html.render(keep_page=True, scrolldown=2000)
            return self.scrape(r, maxposts=10, maxattempts=20)

class UsersCrawler(Scraper):
    def __init__(self, maxusers: int=10, minfollowers: int=1000, **kwargs):
        self.maxusers = maxusers
        self.minfollowers = minfollowers
        self.search_users_url = kwargs.pop('search_users_url', 'https://twitter.com/search?q=a&src=typed_query&f=user')

    @staticmethod
    def value_to_int(x):
        if type(x) == float:
            return int(x) # no flooring
        if 'K' in x:
            if len(x) > 1:
                return int(float(x.replace('K', '')) * 1000)
            return 1000
        if 'M' in x:
            if len(x) > 1:
                return int(float(x.replace('M', '')) * 1000000)
            return 1000000
        if 'B' in x:
            return int(float(x.replace('B', '')) * 1000000000)
        return 0

    def parse_date_field(self, field: str):

        if 'joined' in field.lower():
            date_pattern = ['%B %Y', '%Y/%m']
        elif 'born' in field.lower():
            date_pattern = ['%B %d', '%m/%d']

        field = field.partition(' ')[2]

        return datetime.strptime(field, date_pattern[0]).strftime(date_pattern[1])


    def parse_users(self, response):
        response = fromstring(response.html.html)
        users = response.findall(".//div[@data-testid='cellInnerDiv']//div[@data-testid='UserCell']/div/div[2]/div/div/div/div[2]/div/a/div/div/span")
        names = response.findall(".//div[@data-testid='cellInnerDiv']//div[@data-testid='UserCell']/div/div[2]/div/div/div/div/a/div/div/span/span")
        return {u.replace('@',''):n for u,n in zip([u.text_content() for u in users], [n.text_content() for n in names])}

    def fetch_top_random_users(self, response: requests_html.HTMLResponse, maxattempts=10) -> set:
        users = dict()
        attempts = 0
        print('## Fetching users')
        while True:
            response.html.render(keep_page=True,scrolldown=2000)
            users.update(self.parse_users(response))
            attempts+=1    
            if attempts >= maxattempts or len(users) >= self.maxusers: break
        response.html.render(keep_page=False) # this will enable page closing?
        return users
            
    def scrape_user_info(self, username: str, fields: list=[]):
        user_data = {}
        with HTMLSession() as session:
            r = session.get(f'https://www.twitter.com/{username}')
            r.html.render(keep_page=True,sleep=2)
            #b = get_top_random_users(r)
            r = fromstring(r.html.html)
            try:
                data = ''.join(r.xpath('.//div[@data-testid="UserDescription"]//text()'))
            except Exception as e:
                print(e)
                data = ''
            user_data['desc'] = data

            try:
                data = ''.join(r.xpath(".//a[@data-testid='UserUrl']//text()"))
            except Exception as e:
                print(e)
                data = ''
            user_data['website'] = data

            try:
                # needs parsing
                data = ''.join(r.xpath(".//span[@data-testid='UserJoinDate']//text()"))
                data = self.parse_date_field(data) if data != '' else data
            except Exception as e:
                print(e)
                data = ''
            
            user_data['joined_at'] = data
            
            try:
                data = self.value_to_int(''.join(r.xpath(f".//a[@href='/{username}/followers']/span[1]//text()")))
            except Exception as e:
                print(e)
                data = 0
            
            user_data['followers'] = data

            try:
                data = self.value_to_int(''.join(r.xpath(f".//a[@href='/{username}/following']/span[1]//text()")))
            except Exception as e:
                print(e)
                data = 0

            user_data['following'] = data
                
            try:
                data = ''.join(r.xpath(".//span[@data-testid='UserBirthdate']//text()"))
                data = self.parse_date_field(data) if data != '' else data
            except Exception as e:
                print(e)
                data = ''
            user_data['birthdate'] = data

        return user_data

    def crawl(self) -> list:
        # this function should be async
        # so we can crawl each users tweets aswell
        with HTMLSession() as session:
            r = session.get(self.search_users_url)
            users = self.fetch_top_random_users(r)

        users_info = {}
        tweets = {}
        print("## Crawling for user info")
        for user in users.keys():
            users_info[user] = {'name': users[user]}
            users_info[user].update(self.scrape_user_info(user))
        return users_info

class Pipeline():
    def __init__(self, **kwargs):
        self.users_crawler = UsersCrawler(**kwargs)
        self.users = None

    def set_users(self):
        try:
            self.users = self.users_crawler.crawl()
        except Exception as e:
            print(e)
            self.users = None
    
    def set_tweets(self):
        if self.users:
            for user in self.users.keys():
                tc = TweetCrawler(user=user)
                tweets = tc.crawl()
                self.users[user].update({'tweets': tweets})
        

pl = Pipeline()
pl.set_users()
pl.set_tweets()