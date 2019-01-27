#########################
###Alternative way to   #
# scrape data using    ##
# Twint moduel##########
import twint
import pandas as pd
import os
import pandas.io.common
import util
import logging
from logging.handlers import RotatingFileHandler
import argparse
import numpy as np
from tqdm import tqdm

searchQ = ["#doit4juul", "#juul", "#juulvapor", "#juulnation"]
inceptionDate = "2018-09-09"
logging.basicConfig(level="INFO", format=util.format, filename=(util.logdir + "/twintScrapingLogs.log"))
logger = logging.getLogger("logger")
handler = RotatingFileHandler(util.logdir + "/twintScrapingLogs.log", maxBytes=10000000, backupCount=10)
logger.addHandler(handler)


class ScrapeTwitter:
	def __init__(self):
		self.outputPath = os.path.abspath("../output/twintData/")
		self.userLimit = 1
		self.followingLimit = 100

	def getTweetData(self):
		param = self.twintParam(op="Search")
		df = pandas.DataFrame()
		for idx, element in enumerate(searchQ):
			param.Search = element
			param.Output = str(util.logdir + "\juulMainfile" + str(idx) + ".csv")
			twint.run.Search(param)
			if (os.path.isfile(param.Output)):
				df = self.readCSV(param.Output)
				df.append(df, ignore_index=True)
		if (not df.empty):
			logging.info('[INFO] main tweet files for hashtags created')
		return (df)

	def getUserData(self, df):
		if (not df.empty):
			df.set_index("id")
			## get the csv data
			df_final = df.drop_duplicates(subset='id', keep="first")
			self.toCSV(df_final, "/juulUserTwint.csv")
			logging.info('[INFO] user data created')
			userList = list(df_final.username)
			logging.info('[INFO] extracted user list')
			return (userList)

	def followingData(self, userList):
		param = self.twintParam(op="Following")
		dfFoll = pd.DataFrame([])
		for idx, friend in (enumerate(tqdm(userList))):
			param.Username = friend
			param.Output = str(util.twintDir + "/juulFollowing" + str(friend) + ".csv")
			logging.info('[INFO] subfile %s of following created' % param.Output)
			twint.run.Following(param)
			if (os.path.exists(param.Output)):
				dfFoll = pd.read_csv(param.Output)
				dfFoll.assign(Parent=param.Username)  # @TODO add the parent field to the table
				dfFoll = dfFoll.append(dfFoll, ignore_index=True)
			else:
				logging.info(" no following found for %s" % friend)
		if (not dfFoll.empty):
			return dfFoll
		else:
			return pd.DataFrame([])

	def getData(self):
		logging.info("[INFO] new extraction process started")
		df = self.getTweetData()
		if (not df.empty):
			userList = self.getUserData(df)
			if (userList):
				self.followingData(userList)

	def twintParam(self, op="Search"):
		config = twint.Config()
		config.Store_csv = True
		if (op == "Search"):
			config.Since = inceptionDate
			config.Store_csv = True
			config.Limit = self.userLimit
		elif (op == "Following"):
			config.User_full = True
			config.Limit = self.followingLimit    # in a levels of 20
			# config.Count = self.followingLimit
		return (config)

	def execData(self, config, op="Search"):
		if (op == "Search"):
			twint.run.Search(config)
		elif (op == "Following"):
			twint.run.Following(config)

	def toCSV(self, df, filename):
		path = os.path.abspath(util.logdir)
		df.to_csv(path + filename)

	def readCSV(self, path):
		try:
			df = pd.read_csv(path)
			return df
		except FileNotFoundError:
			logging.error("[ERROR] file not found")
		except pd.io.common.EmptyDataError:
			logging.error("[ERROR] empty file")

	def getFriendsData(self,df,testMode =False):
		users = util.getUsers(df,type= 'name')
		if users is not None:
			if testMode == True:
				users= users[:util.twintLimit]
			if users is not None:
				users = ob.followingData(users)
				df_friends = ob.followingData(users)
				if not df_friends.empty:
					return df_friends
				else:
					return pd.DataFrame([])
			else:
				return pd.DataFrame([])
		else:
			return pd.DataFrame([])

	def output(self, df, filename):
		os.chdir(util.inputdir)
		util.output_to_csv(df, filename=filename)
		logging.info("[INFO] CSV file created")

# if __name__ == '__main__':
# 	ob = ScrapeTwitter()
# 	df = pd.read_csv("C:\Users\akshay\Desktop\thesis\twitter_juul\input\hexagonFull.csv", lineterminator='\n', index_col=0)
# 	if "userName\r" in df:  # windows problem
# 		print("removing carriage")
# 		df["userName\r"] = df["userName\r"].str.replace(r'\r', '')
# 		df.rename(columns={'userName\r': "userName"}, inplace=True)
# 	friendsData = ob.getFriendsData(df, True)
# 	ob.output(friendsData, 'friendsDatanNewTwint')

if __name__ == '__main__':
	ob = ScrapeTwitter()
	parser = argparse.ArgumentParser(description='Extracting data from twintAPI')
	parser.add_argument('-i', '--inputFile', help='Specify the input file path for extracting friends',required=True)
	parser.add_argument('-o','--outputFile', help = 'Specify the output filename',default='friendsDataTw.csv')
	parser.add_argument('-t','--testMode', help = "Specify the test Mode", default=False,type=util.str2bool)
	args = vars(parser.parse_args())
	testMode = args['testMode']
	if (args['inputFile']):
		logging.info('[NEW] ---------------------------------------------')
		logging.info('[INFO] new extraction process started')
		filename_input = args['inputFile']
		filename_output = args['outputFile']
		if (np.DataSource().exists(filename_input)):
			df = pd.read_csv(filename_input, lineterminator='\n', index_col=0)
			if "userName\r" in df:              # windows problem
				print("removing carriage")
				df["userName\r"] = df["userName\r"].str.replace(r'\r', '')
				df.rename(columns={'userName\r': "userName"}, inplace=True)
			friendsData = ob.getFriendsData(df,testMode)
			ob.output(friendsData,filename_output)
		else:
			print("please specify correct path to input file")
	logging.info('[INFO] The job completed succesfully')


