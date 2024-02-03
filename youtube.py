import streamlit as st
import os
import googleapiclient.discovery
import googleapiclient.errors
import pymongo
import mysql.connector
import pandas as pd

#api service
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey='AIzaSyC34eXTqN7ieiHYojAQpkoP0PcCGXieUXw')

client=pymongo.MongoClient('mongodb://localhost:27017')

tab1,tab2=st.tabs(['Home','Query'])

with tab1:

    channelId=st.text_input('Enter a Channel Id')
    #channelId='UC5cY198GU1MQMIPJgMkCJ_Q'

    #channel data
    if st.button('Search'):
        def channelData(channelId):

            response=youtube.channels().list(part='statistics,contentDetails,snippet',id=channelId).execute()
            
            channel_data=dict(channelName=response['items'][0]['snippet']['title'],
                                channelId=response['items'][0]['id'],
                                viewCount=response['items'][0]['statistics']['viewCount'],
                                subscriberCount=response['items'][0]['statistics']['subscriberCount'],
                                videoCount=response['items'][0]['statistics']['videoCount'],
                                description=response['items'][0]['snippet']['description'],
                                playlistId=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
            return channel_data

        #playlist details
        def getplaylistDetails(channelId):
            playlistDetails=[]
            nextPageToken=None
            while True:
                response=youtube.playlists().list(part='contentDetails,snippet',channelId=channelId,maxResults=50,pageToken=nextPageToken).execute()
                for i in response['items']:
                    data=dict(playlistId=i['id'],
                            channelName=i['snippet']['channelTitle'],
                            channelId=i['snippet']['channelId'],
                            title=i['snippet']['title'],
                            description=i['snippet']['description'],
                            videocount=i['contentDetails']['itemCount'])
                    playlistDetails.append(data)
                nextPageToken=response.get('nextPageToken')
                if nextPageToken is None:
                    break
            return playlistDetails

        #videoIds
        def videoIds(channelId):
            response=youtube.channels().list(part='statistics,contentDetails,snippet',id=channelId).execute()
            playlistId=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

            page_token=None
            video_Id=[]
            
            while(True):
                response=youtube.playlistItems().list(part='contentDetails,snippet',playlistId=playlistId,maxResults=50,pageToken=page_token).execute()
                page_token=response.get('nextPageToken')
                for i in range(len(response['items'])):
                    video_Id.append(response['items'][i]['snippet']['resourceId']['videoId'])
                if page_token is None:
                    break
            return video_Id

        #video data
        def videoData(video_Id):
            videoDetails=[]
            for i in video_Id:
                response=youtube.videos().list(part='contentDetails,snippet,statistics',id=i).execute()
                data=dict(channelId=response['items'][0]['snippet']['channelId'],
                          channelName=response['items'][0]['snippet']['channelTitle'],
                        videoId=response['items'][0]['id'],
                        title=response['items'][0]['snippet']['title'],
                        duration=response['items'][0]['contentDetails']['duration'],
                        viewCount=response['items'][0]['statistics'].get('viewCount'),
                        likeCount=response['items'][0]['statistics'].get('likeCount'),
                        commentCount=response['items'][0]['statistics'].get('commentCount'),
                        favoriteCount=response['items'][0]['statistics'].get('favoriteCount'),
                        publishedAt=response['items'][0]['snippet']['publishedAt'])
                videoDetails.append(data)
            return videoDetails

        #comments
        def getComments(video_Ids):
            commentDetails=[]
            try:
                for i in video_Ids:
                    response=youtube.commentThreads().list(part='replies,snippet',videoId=i,maxResults=100).execute()
                    for i in response['items']:
                        data=dict(videoId=i['snippet']['videoId'],
                                commentId=i['snippet']['topLevelComment'].get('id'),
                                textDisplay=i['snippet']['topLevelComment']['snippet'].get('textDisplay'),
                                authorDisplayName=i['snippet']['topLevelComment']['snippet'].get('authorDisplayName'),
                                publishedAt=i['snippet']['topLevelComment']['snippet'].get('publishedAt'))
                        commentDetails.append(data)
            except:
                pass
            return commentDetails


        def insertinto_mongo(channelId):
            channel_data=channelData(channelId)
            video_Id=videoIds(channelId)
            videoDetails=videoData(video_Id)
            commentDetails=getComments(video_Id)
            playlistDetails=getplaylistDetails(channelId)

            db=client['DB1']
            channel_collection=db['youtube_channels']
            channel_collection.insert_one({'channelDetails':channel_data,'videoDetails':videoDetails,'playlistDeatils':playlistDetails,'commentDetails':commentDetails})
            #return channel_collection


        #data to mongodb
        channel_collection=insertinto_mongo(channelId)

    #data to mysql

    connection=mysql.connector.connect(host='localhost',user='root',passwd='Mysql@123',database='DB1')
    cur=connection.cursor()
    db=client['DB1']
    channel_collection=db['youtube_channels']
        
    if st.button('Export to Sql'):

        #channel
        def insert_channelDetails(channel_collection):

            channelDetails_lst=[]
            for i in (channel_collection.find({},{'_id':0,'channelDetails':1})):
                channelDetails_lst.append(i['channelDetails'])
            df_channelDetails=pd.DataFrame(channelDetails_lst)

            for i,j in df_channelDetails.iterrows():

                query='INSERT INTO channelDetails VALUES(%s,%s,%s,%s,%s,%s,%s)'
                values=(j['channelName'],
                        j['channelId'],
                        j['viewCount'],
                        j['subscriberCount'],
                        j['videoCount'],
                        j['description'],
                        j['playlistId'])
                
                cur.execute(query,values)
                connection.commit()

        #playlist

        def insert_playlistDeatils(channel_collection):   
            for i in (channel_collection.find({},{'_id':0,'playlistDeatils':1})):
                df_playlistDeatils=pd.DataFrame(i['playlistDeatils'])

            for i,j in df_playlistDeatils.iterrows():

                query='INSERT INTO playlistDeatils VALUES(%s,%s,%s,%s,%s,%s)'
                values=(j['playlistId'],
                        j['channelName'],
                        j['channelId'],
                        j['title'],
                        j['description'],
                        j['videocount'])
                
                cur.execute(query,values)
                connection.commit()

        #video
        def insert_videoDetails(channel_collection):
            for i in (channel_collection.find({},{'_id':0,'videoDetails':1})):
                df_videoDetails=pd.DataFrame(i['videoDetails'])    
            for i,j in df_videoDetails.iterrows():

                query='INSERT INTO videoDetails VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                values=(j['channelId'],
                        j['channelName'],
                        j['videoId'],
                        j['title'],
                        j['duration'],
                        j['viewCount'],
                        j['likeCount'],
                        j['commentCount'],
                        j['favoriteCount'],
                        j['publishedAt'])
                
                cur.execute(query,values)
                connection.commit()

        #comment
        def insert_commentDetails(channel_collection):
            for i in (channel_collection.find({},{'_id':0,'commentDetails':1})):
                df_commentDetails=pd.DataFrame(i['commentDetails'])
            for i,j in df_commentDetails.iterrows():

                query='INSERT INTO commentDetails VALUES(%s,%s,%s,%s,%s)'
                values=(j['videoId'],
                        j['commentId'],
                        j['textDisplay'],
                        j['authorDisplayName'],
                        j['publishedAt'])
                try:
                    cur.execute(query,values)
                    connection.commit()
                except:
                    pass

        def insertinto_mysql(channel_collection):
            insert_channelDetails(channel_collection)
            insert_playlistDeatils(channel_collection)
            insert_videoDetails(channel_collection)
            insert_commentDetails(channel_collection)

        
        insertinto_mysql(channel_collection)
        channel_collection.drop()

with tab2:
    option=st.selectbox('Select you query',
                  ('Names of all the videos and their corresponding channels',
                    'Channel with most number of videos',
                    'Top 10 most viewed videos',
                    'Number of comments made on each video',
                    'Highest Number of Likes',
                    'Total Number of Likes for each video',
                    'Total Number of view for each Channel',
                    'Channel that published video in 2022',
                    'Videos with highest number of comments')
                   ,index=None,placeholder='Select an option')
    st.write(option)

    connection=mysql.connector.connect(host='localhost',user='root',passwd='Mysql@123',database='DB1')
    cur=connection.cursor()

    if option =='Names of all the videos and their corresponding channels':
        query='select vd.title,cd.channelName from videoDetails vd join channelDetails cd where vd.channelId=cd.channelId;'
        cur.execute(query)
        row=cur.fetchall()
        lst=[]
        for i in row:
        #    j=[i[0],i[1]]
            lst.append(i)
        df=pd.DataFrame(lst,columns=['Video','Channel'])
        st.write(df)
    elif option =='Channel with most number of videos':
        query='select channelName,videoCount from channelDetails order by 2 desc limit 1;'
        cur.execute(query)
        row=cur.fetchall()
        df=pd.DataFrame(row,columns=['Channel','No.of Videos'])
        st.write(df)

    elif option=='Top 10 most viewed videos':
        query='select vd.title,cd.channelName,vd.viewCount from videoDetails vd join channelDetails cd where vd.channelId=cd.channelId order by 3 desc limit 10;'
        cur.execute(query)
        row=cur.fetchall()
        lst=[]
        for i in row:
        #    j=[i[0],i[1],i[2]]
            lst.append(i)
        df=pd.DataFrame(lst,columns=['Title','Channel','Views'])
        st.write(df)
    elif option=='Number of comments made on each video':
        query='select vd.title,cd.channelName,vd.commentCount from videoDetails vd join channelDetails cd where vd.channelId=cd.channelId;'
        cur.execute(query)
        row=cur.fetchall()
        lst=[]
        for i in row:
        #    j=[i[0],i[1],i[2]]
            lst.append(i)
        df=pd.DataFrame(lst,columns=['Title','Channel','No.of Comments'])
        st.write(df)
    elif option=='Highest Number of Likes':
        query='select cd.channelName,vd.title,vd.likeCount from videodetails vd join channelDetails cd where vd.channelId=cd.channelId order by vd.likeCount desc limit 1;'
        cur.execute(query)
        row=cur.fetchall()
        df=pd.DataFrame(row,columns=['Channel','Title','No.of Likkes'])
        st.write(df)
    elif option=='Total Number of Likes for each video':
        query='select title,likeCount from videodetails;'
        cur.execute(query)
        row=cur.fetchall()
        lst=[]
        for i in row:
        #    j=[i[0],i[1]]
            lst.append(i)
        df=pd.DataFrame(lst,columns=['Title','No.of Likes'])
        st.write(df)
    elif option=='Total Number of view for each Channel':
        query='select channelName,viewCount from channelDetails;'
        cur.execute(query)
        row=cur.fetchall()
        lst=[]
        for i in row:
        #    j=[i[0],i[1]]
            lst.append(i)
        df=pd.DataFrame(lst,columns=['Channel','No.of Views'])
        st.write(df)
    elif option=='Channel that published video in 2022':
        query='select channelName from channelDetails where channelId in (select distinct channelId from videoDetails where year(publishedAt)=''2022'');'
        cur.execute(query)
        row=cur.fetchall()
        lst=[]
        for i in row:
            lst.append(i)
        df=pd.DataFrame(lst,columns=['Channel'])
        st.write(df)
    elif option=='Videos with highest number of comments':
        query='select channelName,title,commentCount from videodetails order by commentCount desc limit 1;'
        cur.execute(query)
        row=cur.fetchall()
        df=pd.DataFrame(row,columns=['Channel','Title','No.of Comments'])
        st.write(df)
