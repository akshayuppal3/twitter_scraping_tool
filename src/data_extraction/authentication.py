#############################
##Class for authentication###
##containing secrets key info
#############################
#############################
##TODO put all in doc string
import tweepy
from tweepy import OAuthHandler
import os
import pandas as pd
import git
class Authenticate:

	def __init__(self):
		self.api = self.get_api()

	def get_git_root(self,path):
		git_repo = git.Repo(path, search_parent_directories=True)
		git_root = git_repo.git.rev_parse("--show-toplevel")
		return git_root

	def get_api(self):
		api_list = list()
		keys_path = os.path.join(self.get_git_root(os.getcwd()),"keys.csv")
		df_keys = pd.read_csv(keys_path,encoding="utf-8")
		for index,row in df_keys.iterrows():
			api = self.authorization(row)
			api_list.append(api)
		return api_list

	def authorization(self,ob):
		consumer_key = ob.consumer_key
		consumer_secret = ob.consumer_secret
		access_token = ob.access_token
		access_secret = ob.access_secret
		author = OAuthHandler(consumer_key, consumer_secret)
		author.set_access_token(access_token, access_secret)
		# change to accomodate rate limit errors
		api = tweepy.API(author, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=5, retry_delay=5)
		return (api)


