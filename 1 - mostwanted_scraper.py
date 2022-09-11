
import scrapy
from scrapy.item import Item, Field
from scrapy import signals
import logging
from scrapy.utils.log import configure_logging


# Base url to start
###############################################################################
BASE_URL = 'https://eumostwanted.eu/'
###############################################################################


# Define item class
class MostwantedItem(scrapy.Item):
	person = scrapy.Field()


# Define pipeline
import json
import codecs
class ItemsPipeline(object):

	def __init__(self):
		self.file = codecs.open('mostwanted.json', 'w', encoding='utf-8')

	def process_item(self, item, spider):
		line = json.dumps(dict(item), ensure_ascii=False) + "\n"
		self.file.write(line)
		return item

	def close_spider(self, spider):
		self.file.close()


# Middleware
class MostwantedDownloaderMiddleware:
	# Not all methods need to be defined. If a method is not defined,
	# scrapy acts as if the downloader middleware does not modify the
	# passed objects.

	@classmethod
	def from_crawler(cls, crawler):
		# This method is used by Scrapy to create your spiders.
		s = cls()
		crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
		return s

	def process_request(self, request, spider):
		# Called for each request that goes through the downloader
		# middleware.

		# Must either:
		# - return None: continue processing this request
		# - or return a Response object
		# - or return a Request object
		# - or raise IgnoreRequest: process_exception() methods of
		#   installed downloader middleware will be called
		return None

	def process_response(self, request, response, spider):
		# Called with the response returned from the downloader.

		# Must either;
		# - return a Response object
		# - return a Request object
		# - or raise IgnoreRequest
		return response

	def process_exception(self, request, exception, spider):
		# Called when a download handler or a process_request()
		# (from other downloader middleware) raises an exception.

		# Must either:
		# - return None: continue processing this exception
		# - return a Response object: stops process_exception() chain
		# - return a Request object: stops process_exception() chain
		pass

	def spider_opened(self, spider):
		spider.logger.info('Spider opened: %s' % spider.name)



# Define spider
class MostwantedSpider(scrapy.Spider):
	name = "mostwanted"
	custom_settings = {
		'ITEM_PIPELINES': {'__main__.ItemsPipeline': 400},
		'DOWNLOADER_MIDDLEWARES' : {'__main__.MostwantedDownloaderMiddleware': 543,}
		}

	# Configure error log
	logging.basicConfig(
		filename='log.txt',
		format='%(levelname)s: %(message)s',
		level=logging.INFO
	)

	def start_requests(self):
		yield scrapy.Request(url=BASE_URL, callback=self.parse)

	def parse(self, response):
		if response.status==200:
			raw_persons = response.xpath("//div[contains(@class,'views-row')]")
			# for item in raw_persons:
			for idx, item in enumerate(raw_persons):
				person = MostwantedItem()
				person = {
					"name": "",
					"crime": "",
					"gender": "",
					"dob": "",
					"nationality": "",
					"state_of_case": "",
					"url": ""
				}

				# url
				raw_url = item.xpath(".//a/@href").extract()[0]
				if raw_url:
					print(idx+1, raw_url)
					person['url'] = raw_url

				request = scrapy.Request(
					raw_url,
					callback=self.parse_person,
					dont_filter=True)

				request.meta['item'] = person

				yield request

	def parse_person(self, response):
		person = response.meta['item']
		if response.status == 200:

			# name
			try:
				name = response.xpath("//div[contains(@class,'title-field')]/h2/text()").extract()[0]
			except:
				name = ""
			person['name'] = name


			# crime
			try:
				crime = response.xpath("//div[contains(@class,'field-crime')]/ul/li/text()").extract()
			except:
				crime = ""
			person['crime'] = crime


			# gender
			try:
				gender = response.xpath("//div[contains(@class,'field-gender')]/ul/li/text()").extract()[0]
			except:
				gender = ""
			person['gender'] = gender


			# dob
			try:
				dob = response.xpath("//div[contains(@class,'field-date-of-birth')]/span/text()").extract()[0]
			except:
				dob = ""
			person['dob'] = dob

			# nationality
			try:
				nationality = response.xpath("//div[contains(@class,'field-nationality')]/ul/li/text()").extract()[0]
			except:
				nationality = ""
			person['nationality'] = nationality

			# state_of_case
			try:
				state_of_case = response.xpath("//div[contains(@class,'field-state-of-case')]/div/div/text()").extract()[0]
			except:
				state_of_case = ""
			person['state_of_case'] = state_of_case

			yield person


####################################################################################################################
# Run Spider #######################################################################################################
####################################################################################################################
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner

if __name__ == '__main__':
	runner = CrawlerRunner()
	runner.crawl(MostwantedSpider)

	d = runner.join()
	d.addBoth(lambda _: reactor.stop())

	reactor.run()
