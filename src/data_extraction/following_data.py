#############################
##Class for scraping ########
##following data twitter#####
##########version 2 #########
#############################

from authentication import Authenticate
from tweepy import OAuthHandler
import numpy as np
import pandas as pd
import util
import argparse
import logging
import tweepy
import os
from tqdm import tqdm
import ast
import time
import pickle
import math
from pandas import ExcelWriter
from openpyxl import load_workbook
from collections import deque

logging.basicConfig(level=logging.INFO, format= util.format, filename= os.path.join(util.logdir,"followingData.log"))

## just passing the file and it would extract the following data

class twitter_following():

	def __init__(self):
		self.api_list = self.get_api()

	def get_api(self):
		ob = Authenticate()
		api_list = ob.api
		return (api_list)

	def getUserTimelineData(self, user,api):
		userData = pd.DataFrame([])
		if user is not None:
			try:
				for status in tweepy.Cursor(api.user_timeline, user_id=user,
				                            tweet_mode="extended").items():
					data = util.getTweetObject(status)
					userData = userData.append(data, ignore_index=True)
				return userData

			except tweepy.TweepError as e:
				logging.error("[Error] " + e.reason)

	def getuserTimeline(self, users,filename_output):
		apis = deque(self.api_list)
		finalData = pd.DataFrame([])
		unique_users = set(users)
		count= 1
		for idx,user in enumerate(tqdm(unique_users)):
			apis.rotate(-1)
			api = apis[0]
			userData = self.getUserTimelineData(user,api)
			if ((idx > 0) & (idx % 500 == 0)):  # dump after every 500
				filename = filename_output+ str(count) + '.csv' + '.pkl'
				with open(filename,"wb") as f:
					pickle.dump(finalData,f)
				count += 1
				finalData = pd.DataFrame([])
			else:
				finalData = finalData.append(userData,ignore_index=True)
		filename = filename_output + str(count) + '.csv' + '.pkl'
		with open(filename, "wb") as f:   # last batch
			pickle.dump(finalData, f)

	# @param df, filename , testMode(bool)
	# @return None
	# writes to an excel file
	def getFriendsData(self,df,output_path,index=None):
		users = util.getUsers(df,type= 'ID')
		if isinstance(index,int):
			users = users[index:]
			logging.info("Starting with index %d" % index)
		try:
			if users:
				df = pd.DataFrame()
				apis = deque(self.api_list)
				for index,user in enumerate(tqdm(users)):
					try:
						apis.rotate(-1)
						api = apis[0]
						friendList = api.friends_ids(user)  # @API returns list of friends
						temp = pd.DataFrame(
							{'userID':user,
							 'following':[friendList]})
						df = df.append(temp)
						util.df_write_csv(temp,output_path)
					except:
						logging.error("Some error in connection")
						continue
				return df

		except tweepy.TweepError as e:          # except for handling tweepy api call
			print("[Error] " + e.reason)

	# @return DataFrame @ param friends ID and parent ID
	# takes batch of friend ids and returns detailed information of user
	def getFriendBatch(self, friendIds, parent_id):
		apis = deque(self.api_list)
		data = pd.DataFrame([])
		try:
			apis.rotate(-1)
			api = apis[0]
			user = api.lookup_users(friendIds, include_entities=True)  # api to look for user (in batch of 100)
			if user:
				for idx, statusObj in enumerate(user):
					userData = util.getTweetObject(tweetObj=statusObj)
					data = data.append(userData, ignore_index=True)
				return data
			else:
				print("no user found for the batch")

		except tweepy.TweepError as e:
			print(e.reason)
			logging.error("[Error] " + e.reason)
			# self.getFriendBatch(friendIds, parent_id)       # check when connection is getting lost

		except:
			print("connection timeout")
			logging.error("[lookup users] Some error in api or connection")
			# self.getFriendBatch(friendIds, parent_id)       # check when connection is getting lost

	# return None
	# get the detailed following data for the users and write to excel
	def get_detail_friends_data(self,df):
		data = pd.DataFrame([])
		if df is not None:
			with tqdm(total=(len(list(df.iterrows())))) as pbar:
				for index,row in (df.iterrows()):
					pbar.update(1)                                                # handling tqdm for pandas
					parent_id = row['userID']
					friends_data = ast.literal_eval(row['following'])             # as data as interpreted as string instead of list
					if len(friends_data) > 100:                                    # as api.lookup users take data in batch of 100
						batch_size = int(math.ceil(len(friends_data) / 100))
						for i in tqdm(range(batch_size)):
							dfBat = friends_data[(100 * i): (100 * (i + 1))]
							friends_detailed = self.getFriendBatch(dfBat, parent_id)
							if friends_detailed is not None:
								data = data.append(friends_detailed,ignore_index=False)
								# util.df_write_excel(friends_detailed,output_path)   # write the data to excel file
					else:
						friends_detailed = self.getFriendBatch(friends_data, parent_id)
						if friends_detailed is not None:
							data = data.append(friends_detailed, ignore_index=False)
							# util.df_write_excel(friends_detailed,output_path)
				return data

	## @param usersIDs @return adjacency matrix (2d array)
	def get_friendships(self,userIDs):
		adjacency_matrix = np.empty((0, len(userIDs)))

		for friend in tqdm(userIDs):
			a = list()
			for neighbor in tqdm(userIDs):
				if (friend == neighbor):
					a.append(0)
					continue
				try:
					status = self.api.show_friendship(source_id=friend, target_id=neighbor)
				except tweepy.TweepError as e:
					print(e.reason)
					a.append(0)
					continue
				if (status[0].following is True):
					a.append(1)
				elif (status[0].following is False):
					a.append(0)
			adjacency_matrix = np.vstack((adjacency_matrix, a))
		return adjacency_matrix

if __name__ == '__main__':
	ob = twitter_following()
	parser = argparse.ArgumentParser(description='Extracting data from userDataFile')
	parser.add_argument('-i', '--inputFile', help='Specify the input file path for extracting friends', required=False)
	parser.add_argument('-i2', '--inputFile2', help='Specify the input file path with user and friends id', required=False)
	parser.add_argument('-i3', '--inputFile3', help='Specify the input file path with user and friends id',required=False)
	parser.add_argument('-i4', '--inputFile4', help= 'Specify the input file for users for their timeline data',required=False)
	parser.add_argument('-o',  '--outputFile', help='Specify the output file name with following data',default='followingList')
	parser.add_argument('-p', '--path', help='Specify the existing path for the file <include extension>')
	parser.add_argument('-x', '--index', help='Specify the idx for users for following list', default=None)
	args = vars(parser.parse_args())
	if (args['inputFile']):
		logging.info('[NEW] ---------------------------------------------')
		logging.info('new extraction process started')
		filename_input = args['inputFile']
		filename_path = args['outputFile']
		filename_output = os.path.join(util.inputdir, filename_path + '_trailing'  + '.csv')
		if (args['index'] is not None):
			index = int(args['index'])
		else:
			index = None
		df = util.readCSV(filename_input)
		df = ob.getFriendsData(df,filename_output,index)
		filename_output = os.path.join(util.inputdir, filename_path + '.csv')
		util.output_to_csv(df,filename_output)
		logging.info("File creation of basic user and following completed")
	if (args['inputFile2']):
		logging.info('[NEW] ---------------------------------------------')
		logging.info('getting detailed following list for users')
		filename_input = args['inputFile2']
		filename_output = args['outputFile']
		filename_output = os.path.join(util.inputdir,filename_output+'.csv')
		df = util.read_excel(filename_input)
		if (df is not None):
			data = ob.get_detail_friends_data(df)
			util.output_to_csv(data,filename_output)
			logging.info("File creation of detailed user completed")
	# for reading the friendship IDS
	if (args['inputFile3']):
		logging.info('[NEW] ---------------------------------------------')
		filename_input = args['inputFile3']
		filename_output = args['outputFile']
		filename_output = os.path.join(util.inputdir, filename_output + '.csv')
		logging.info('getting pairwise matrix for the users')
		if filename_input.endswith('.xlsx'):
			df = util.read_excel(filename_input)
		elif filename_input.endswith('.csv'):
			df = util.readCSV(filename_input)
		if not df.empty:
			## getting the list of userIDs
			if 'userID' in df:
				userIDs = list(df.userID.astype(int))        #treat as int intead of floats
				pairwise_adjacency_matrix = ob.get_friendships(userIDs)
				columns = userIDs
				df = pd.DataFrame(pairwise_adjacency_matrix, columns=columns, index=columns)
				df.to_csv(filename_output)
				print(filename_output,"created successfully")
				logging.info("File creation of detailed user completed")
	if (args['inputFile4']):
		logging.info('[NEW] ---------------------------------------------')
		filename_input = args['inputFile4']
		filename_output = args['outputFile']
		filename_output = os.path.join(util.inputdir, filename_output)
		logging.info('getting the timeline data for users')
		if filename_input.endswith('.xlsx'):
			df = util.read_excel(filename_input)
		elif filename_input.endswith('.csv'):
			df = util.readCSV(filename_input)
		if not df.empty:
			## getting the list of userIDs
			if 'userID' in df:
				userIDs = list(df.userID.unique().astype(int))
				if (args['index'] is not None):
					index = int(args['index'])
					logging.info("Starting with index %d" % index)
					print("Starting with index %d" % index)
					userIDs = userIDs[index:]
				ob.getuserTimeline(userIDs,filename_output)
				print(filename_output, "created successfully")
				logging.info("File creation of detailed user completed")


