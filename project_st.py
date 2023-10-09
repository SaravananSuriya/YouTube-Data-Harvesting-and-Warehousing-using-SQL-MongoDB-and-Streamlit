import streamlit as st
import time
import pandas as pd
import numpy as np
from streamlit_option_menu import option_menu
import pymongo
from googleapiclient.discovery import build
import json
import os
from dateutil import parser
from datetime import datetime
import mysql.connector
import plotly.express as px

myclient = pymongo.MongoClient('mongodb://localhost:27017/')
try:
    db = myclient['project'] 
    coll = db['collection'] 
except:
    pass

conn = mysql.connector.connect(host = 'localhost', password = 'Saravanan@246', user = 'root', port = 3307, database = 'project')
cur = conn.cursor()
try:
  cur.execute('create table Channel_Details(Channel_Name varchar(255) not null, Channel_Id varchar(255) not null primary key, Channel_Description text not null, Channel_Subscriber_Count int not null, Channel_Video_Count int not null, Channel_View_Count bigint(20) UNSIGNED not null, Channel_Playlists_Id varchar(255) not null)')
  cur.execute('create table playlist(Playlist_Id varchar(255) not null primary key, Channel_id varchar(255) not null, foreign key(Channel_id) references Channel_Details(Channel_Id) on delete cascade, Playlist_Name text not null, Playlist_Video_Count int not null, Playlist_PublishedAt datetime not null)')
  cur.execute('create table videos(Playlist_id varchar(255) not null, foreign key(Playlist_id) references playlist(Playlist_Id) on delete cascade, Video_Id varchar(250) not null primary key, Video_Name varchar(255) not null, Video_Description text not null, Video_PublishedAt datetime not null, Video_Duration time not null, Video_View_Count int not null, Video_Like_Count int not null, Video_Favorite_Count int not null, Video_Comment_Count int not null, Video_Thumbails varchar(255), Video_Caption_Status varchar(50))')
  cur.execute('create table Comments(Video_Id varchar(255) not null, foreign key(Video_Id) references videos(Video_Id) on delete cascade, Comment_Id varchar(255) not null primary key, Comment_Text text not null , Comment_Author varchar(200) not null, Comment_PublishedAt datetime not null, Comment_Like_Count int not null, Comment_TotalReply_Count int not null)')
except:
  pass


# horizontal menu
st.set_page_config(layout = 'wide')
selected = option_menu(
  menu_title = 'YouTube Data Harvesting and Warehousing',
  options = ['Home','Data Collections','Store in NoSQL','Store in SQL','Data Removing','Data Analysis'],
  icons = ['house','collection-fill','filetype-json','database-fill-add','trash','bar-chart-line-fill'],
  menu_icon = 'youtube',
  default_index = 0,
  orientation = 'horizontal', # here we use 'vertical' also
  styles = {
    "container" : {'padding':'0!important','background-color':None},
    'icons':{'color':'orange','fontsize':'40px'},
    'nav-link':{
      'font-size':'18px',
      'text-align':'center',
      'margin':'0px',
    }
  }
)

if selected == 'Home':
  st.markdown(f'## Project Title')
  st.markdown('#### YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit')
  st.markdown(f'## Domain')
  st.markdown('#### Social Media')
  st.markdown('## Skills take away From This Project')
  st.markdown('#### Python scripting, Data Collection, Streamlit, API integration, Data Managment using MongoDB and SQL')
  
elif selected == 'Data Collections':

  st.header('Here is the data collected from the YouTube using API integration')
  st.info('Copy a Channel Id for Data Migration.')
  cha_id = ['UCCezIgC97PvUuR4_gbFUs5g','UCnz-ZXXER4jOvuED5trXfEA','UC63URkuUvnugRBeTNqmToKg','UCJ5v_MCY6GNUBTO8-D3XoAg','UCk3JZr7eS3pg5AGEvBdEvFg','UCRijo3ddMTht_IHyNSNXpNQ','UCY6KjrDBN_tIRFT_QNqQbRQ','UCueYcgdqos0_PzNOq81zAFg','UCnjU1FHmao9YNfPzE039YTw','UC8_aMwsn53tncdH-5iLu8-w','UCk081mmVz4hzff-3YVBAxow','UCn4rEMqKtwBQ6-oEwbd4PcA','UC_oCw5PLyGQEJvRtlkjwS6A','UCq8DICunczvLuJJq414110A','UCLbdVvreihwZRL6kwuEUYsA','UCkAGrHCLFmlK3H2kd6isipg']
  cha_name = ['Corey Schafer','techTFQ','Sundeep Saradhi Kanthety','WWE','Village Cooking Channel','Dude Perfect','Madan Gowri','Parithabangal',"Irfan's view",'Empty Hand','Saravanan Decodes','Sony Music South','Gaming Tamizhan','Zach King','Think Music India','Mr Bean']
  List = []
  for i,j in zip(cha_id,cha_name):
    result = ((i),(j))
    List.append(result) 
  data = pd.DataFrame(List,columns=['Channel Ids','Channel Names'])
  st.dataframe(data,600,500)

elif selected == 'Store in NoSQL':
  st.header('Get a Channel Id from Data Collections Page or Get from YouTube')
  st.markdown(
    """
    <style>
    div[class*="stTextInput"] label {
          color: purple;
        }
    </style>
    """,
    unsafe_allow_html=True,
  )
  id = st.text_input("🔗 Enter a Channel Id")
  names = []
  name = ''
  st.markdown('👇 Click below button to store in MongoDB')
  if st.button('Click to Submit'):
    if len(id) != 0:
      for i in db.coll.find({},{'Channel_Name':1,'_id':0}):
        names.append(i['Channel_Name'])
      for i in db.coll.find({'Channel_Id':id},{'Channel_Name':1,'_id':0}):
        name += i['Channel_Name']
      if name in names:
        with st.spinner('please wait'):
          time.sleep(1)
        st.warning('⚠️ Already Exists in MongoDB')
      else:
        def channel(channel_id):
          api_key = 'AIzaSyCD0M_AAq6edBOhb3NSMsfds0L3b0NoUEo'
          api_service_name = 'youtube'
          api_version = 'v3'
          youtube = build(api_service_name, api_version, developerKey=api_key)
          response = youtube.channels().list(
              id=channel_id,
              part='snippet,statistics,contentDetails'
          )
          channel_data = response.execute()
                      
          channel_name = channel_data['items'][0]['snippet']['title']
          global chan_name
          chan_name = ''
          chan_name+=(channel_data['items'][0]['snippet']['title'])
          db.coll.insert_one({'Channel_Name':channel_data['items'][0]['snippet']['title'],'Channel_Id':channel_data['items'][0]['id'],'Channel_Description':channel_data['items'][0]['snippet']['description'],'Channel_Subscriber_Count':channel_data['items'][0]['statistics']['subscriberCount'],'Channel_Video_Count':channel_data['items'][0]['statistics']['videoCount'],'Channel_View_Count':channel_data['items'][0]['statistics']['viewCount'],'Channel_Playlists_Id':channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']})
          
          def playlists():

              request = youtube.playlists().list(
                  part='snippet,contentDetails',
                  channelId=channel_id,
                  maxResults=10
              )
              response = request.execute()
              playlistid = []
              for i in response['items']:
                  data = (i['id'])
                  playlistid.append(data)

              video_id = []
              for i in playlistid:
                  if len(video_id) < 10:
                      request = youtube.playlistItems().list(
                          part='contentDetails',
                          playlistId=i,
                          maxResults=10
                      )
                      response = request.execute()
                      for i in response['items']:
                          video_id.append(i['contentDetails']['videoId'])
                  else:
                      break

              def videos():

                  vid_request = youtube.videos().list(
                      part='snippet,statistics,contentDetails',
                      maxResults=5,
                      id=','.join(video_id)
                  )
                  vid_response = vid_request.execute()
                  video_dt = []
                  comment_dt = []
                  for i in vid_response['items']:
                      try:
                          vid_data = ((i['id']),(i['snippet']['title']),(i['snippet']['description']),(i['snippet']['publishedAt']),(i['contentDetails']['duration']),(i['statistics']['viewCount']),(i['statistics']['likeCount']),(i['statistics']['favoriteCount']),(i['statistics']['commentCount']),(i['snippet']['thumbnails']['default']['url']),(i['contentDetails']['caption']))
                          video_dt.append(vid_data)

                          com_request = youtube.commentThreads().list(
                              part="snippet",
                              videoId = i['id'],
                              maxResults=5,
                          )
                          com_response = com_request.execute()
                          for j in com_response['items']:
                              comm_data = ((j['snippet']['videoId']),(j['id']),(j['snippet']['topLevelComment']['snippet']['textOriginal']),(j['snippet']['topLevelComment']['snippet']['authorDisplayName']),(j['snippet']['topLevelComment']['snippet']['publishedAt']),(j['snippet']['topLevelComment']['snippet']['likeCount']),(j['snippet']['totalReplyCount']))
                              comment_dt.append(comm_data)
                      except:
                         pass

                  df = pd.DataFrame(video_dt,columns = ['Video_Id','Video_Name','Video_Description','Video_PublishedAt','Video_Duration','Video_View_Count','Video_Like_Count','Video_Favorite_Count','Video_Comment_Count','Video_Thumbails','Video_Caption_Status'])
                  df1 = pd.DataFrame(comment_dt,columns = ['Video_Id','Comment_Id','Comment_Text','Comment_Author','Comment_PublishedAt','Comment_Like_Count','Comment_TotalReply_Count'])
      
                  df.reset_index(inplace=True)
                  df1.reset_index(inplace=True)
                  data_dict = df.to_dict('records')
                  data_dict1 = df1.to_dict('records')
                  db.coll.update_one({'Channel_Name':chan_name},{'$set':{'Vidoes':data_dict}})
                  db.coll.update_one({'Channel_Name':chan_name},{'$set':{'Comments':data_dict1}})
                  with st.spinner('please wait...'):
                    time.sleep(3)
                  st.success('👍 Successfully Uploaded in MongoDB')
                  st.subheader('Sample data of MongoDB')
                  try:
                    datas = db.coll.find_one({'Channel_Name':chan_name},{'_id':0})
                    data = {'Channel_Name':datas['Channel_Name'],'Channel_Id':datas['Channel_Id'],'Channel_Description':datas['Channel_Description'],'Channel_Subscriber_Count':datas['Channel_Subscriber_Count'],'Channel_Video_Count':datas['Channel_Video_Count'],'Channel_View_Count':datas['Channel_View_Count'],'Channel_Playlists_Id':datas['Channel_Playlists_Id'],'Videos':datas['Vidoes'][0],'Comments':datas['Comments'][0]}
                    result = json.dumps(data, indent=3)
                    st.code(result)
                    st.balloons()
                  except:
                    datas = db.coll.find_one({'Channel_Name':chan_name},{'_id':0})
                    data = {'Channel_Name':datas['Channel_Name'],'Channel_Id':datas['Channel_Id'],'Channel_Description':datas['Channel_Description'],'Channel_Subscriber_Count':datas['Channel_Subscriber_Count'],'Channel_Video_Count':datas['Channel_Video_Count'],'Channel_View_Count':datas['Channel_View_Count'],'Channel_Playlists_Id':datas['Channel_Playlists_Id']}
                    result = json.dumps(data, indent=3)
                    st.code(result)
                    st.balloons()
              videos()
          playlists()
        channel(id)
    else:
      st.error(body = 'Empty Value',icon = '🚨')
elif selected == 'Store in SQL':
  st.header('Here is the Channel List already stored in MongoDB')
  names = []
  for i in db.coll.find({},{'Channel_Name':1,'_id':0}):
    names.append(i['Channel_Name'])
  
  choice = st.selectbox('Pick a Channel Name',options = names,index = 0)
  st.markdown('👇 Click below button to store in SQL')
  if st.button('Click to submit'):
    ch_name = []
    cur.execute('select Channel_Name from channel_details')
    for i in cur:
        res = list(i)
        ch_name.extend(res)
    if choice not in ch_name:
      ch_id = db.coll.find_one({'Channel_Name':choice},{'Channel_Id':1,'_id':0})

      def channel(channel_id):
        api_key = 'AIzaSyBE97Q-EkBFdJdypkxornS8W3xGqmQmYL8'
        api_service_name = 'youtube'
        api_version = 'v3'
        youtube = build(api_service_name, api_version, developerKey=api_key)
        response = youtube.channels().list(
          id=channel_id,
          part='snippet,statistics,contentDetails'
        )
        channel_data = response.execute()
        

        for i in channel_data['items']:
          channel_Id = i['id']                   
          global chan_id
          chan_id = channel_Id
          
          insert_query = 'insert into Channel_Details(Channel_Name, Channel_Id, Channel_Description, Channel_Subscriber_Count, Channel_Video_Count, Channel_View_Count, Channel_Playlists_Id) values(%s,%s,%s,%s,%s,%s,%s)'
          value = (i['snippet']['title'], channel_Id, i['snippet']['description'], i['statistics']['subscriberCount'], i['statistics']['videoCount'], i['statistics']['viewCount'], i['contentDetails']['relatedPlaylists']['uploads'])
          cur.execute(insert_query,value)
          conn.commit()
        
        def playlists():
              request = youtube.playlists().list(
                  part='snippet,contentDetails',
                  channelId=chan_id,
                  maxResults=10
              )
              response = request.execute()
              playlistid = []
              for i in response['items']:
                  publishedAt =  i['snippet']['publishedAt']
                  data = (i['id'])
                  playlistid.append(data)
                  dt = parser.parse(publishedAt)
                  ins_query = 'insert into playlist(Playlist_Id, Channel_id, Playlist_Name, Playlist_Video_Count, Playlist_PublishedAt) values(%s,%s,%s,%s,%s)'
                  values = (i['id'],i['snippet']['channelId'],i['snippet']['title'],i['contentDetails']['itemCount'],dt)
                  cur.execute(ins_query,values)
                  conn.commit()
              video_id = []
              playlist = []
              for i in playlistid:
                  playlist.append(i)
                  if len(video_id) < 10:
                        request = youtube.playlistItems().list(
                            part='contentDetails',
                            playlistId=i,
                            maxResults=10
                        )
                        response = request.execute()
                        for i in response['items']:
                            video_id.append(i['contentDetails']['videoId'])
                  else:
                        break
              
              def videos():
                  global vid_id
                  vid_id = []
                  vid_request = youtube.videos().list(
                        part='snippet,statistics,contentDetails',
                        maxResults=5,
                        id=','.join(video_id)
                  )
                  vid_response = vid_request.execute()
                  for i in vid_response['items']:
                        published = i['snippet']['publishedAt']
                        dt = parser.parse(published)
                        t = i['contentDetails']['duration']
                        if ('S' in t) and ('P' in t) and ('T' in t) and ('M' not in t) and ('H' not in t):
                          res = datetime.strptime(t,'PT%SS')
                          time = res.time()
                        elif ('S' in t) and ('P' in t) and ('T' in t) and ('H' in t) and ('M' not in t):
                          res = datetime.strptime(t,'PT%HH%SS')
                          time = res.time()
                        elif len(t) <= 5:
                          res = datetime.strptime(t,'PT%MM')
                          time = res.time()
                        elif len(t) <= 8:
                          res = datetime.strptime(t,'PT%MM%SS')
                          time = res.time()
                        else:
                          res = datetime.strptime(t,'PT%HH%MM%SS')
                          time = res.time()

                        try:
                            insert_que = 'insert into videos(Playlist_id, Video_Id, Video_Name, Video_Description, Video_PublishedAt, Video_Duration, Video_View_Count, Video_Like_Count, Video_Favorite_Count, Video_Comment_Count, Video_Thumbails, Video_Caption_Status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                            val = (playlist[0],i['id'],i['snippet']['title'],i['snippet']['description'],dt,time,i['statistics']['viewCount'],i['statistics']['likeCount'],i['statistics']['favoriteCount'],i['statistics']['commentCount'],i['snippet']['thumbnails']['default']['url'],i['contentDetails']['caption'])
                            cur.execute(insert_que,val)
                            conn.commit()
                            vid_id.append(i['id'])
                        except:
                            conn.rollback() 

                  def comments():
                    for i in vid_id:
                        com_request = youtube.commentThreads().list(
                              part="snippet",
                              videoId = i,
                              maxResults=5,
                        )
                        com_response = com_request.execute()
                        for j in com_response['items']:
                              publish = j['snippet']['topLevelComment']['snippet']['publishedAt']
                              Datetime = parser.parse(publish)
                              try:
                                  query = 'insert into comments(Video_Id, Comment_Id, Comment_Text, Comment_Author, Comment_PublishedAt, Comment_Like_Count, Comment_TotalReply_Count) values(%s,%s,%s,%s,%s,%s,%s)'
                                  value = (j['snippet']['videoId'],j['id'],j['snippet']['topLevelComment']['snippet']['textOriginal'],j['snippet']['topLevelComment']['snippet']['authorDisplayName'],Datetime,j['snippet']['topLevelComment']['snippet']['likeCount'],j['snippet']['totalReplyCount'])
                                  cur.execute(query,value)
                                  conn.commit()
                              except:
                                  conn.rollback()
                
                  comments()
              videos()
        playlists()
      channel(ch_id['Channel_Id'])
      with st.spinner('loading...'):
        time.sleep(3)
      st.success('👍 Successfully Uploaded in SQL')
      st.markdown('## Sample data of SQL')
      st.markdown('#### Channel Table')
      cur = conn.cursor()
      cur.execute('select * from Channel_Details')
      Channel = []
      for i in cur:
        Channel.append(i)
      df = pd.DataFrame(Channel,columns = ['Channel_Name','Channel_Id','Channel_Description','Channel_Subscriber_Count','Channel_Video_Count','Channel_View_Count','Channel_Playlists_Id'])
      st.dataframe(df,2050,400)
      st.markdown('#### Playlist Table')
      cur.execute('select * from playlist')
      playlst = []
      for i in cur:
          playlst.append(i)
      df1 = pd.DataFrame(playlst,columns = ['Playlist_Id','Channel_Id','Playlist_Name','Playlist_Video_count','Playlist_PublishedAt'])
      st.dataframe(df1,2000,700)
      conn.close()
      st.balloons()
    else:
      with st.spinner('please wait'):
        time.sleep(1)
      st.warning('⚠️ Already Exists in SQL')

elif selected == 'Data Removing':
  choice1 = st.radio(
    label='Choose an option', 
    options=['Delete From NoSQL','Delete from SQL']
  )
  st.markdown(
      """<style>
  div[class*="stRadio"] > label > div[data-testid="stMarkdownContainer"] > p {
      font-size: 30px;
  }
      </style>
      """, unsafe_allow_html=True)
  if choice1 == 'Delete From NoSQL':
    names = []
    for i in db.coll.find({},{'Channel_Name':1,'_id':0}):
      names.append(i['Channel_Name'])
    choice = st.selectbox('Pick a Channel Name',options = names,index = 0)
    check = st.button('Delete in MongoDB')
    if check:
      with st.spinner('please wait'):
        time.sleep(1)
      db.coll.delete_one({'Channel_Name':choice})
      st.success('👍 Successfully Deleted in MongoDB')
  else:
    ch_name = []
    cur.execute('select Channel_Name from Channel_Details')
    for i in cur:
      res = list(i)
      ch_name.extend(res)
    cho = st.selectbox('Pick a Channel Name',options = ch_name,index = 0)
    check = st.button('Delete in SQL')
    if check:
      cur = conn.cursor()
      cur.execute(f"DELETE FROM Channel_Details WHERE Channel_Name = '{cho}'")
      conn.commit()
      conn.close()
      with st.spinner('please wait..'):
        time.sleep(1)
      st.success('👍 Successfully Deleted in SQL')
    else:
      pass
else:
  st.header('Data Analyzing with stored data')
  choice = st.selectbox('Select any question to Analyze the datas',options = ['1. What are the names of all the videos and their corresponding channels?','2. Which channels have the most number of videos, and how many videos do they have?','3. What are the top 10 most viewed videos and their respective channels?','4. How many comments were made on each video, and what are their corresponding video names?','5. Which videos have the highest number of likes, and what are their corresponding channel names?','6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?','7. What is the total number of views for each channel, and what are their corresponding channel names?','8. What are the names of all the channels that have published videos in the year  2022?','9. What is the average duration of all videos in each channel, and what are their corresponding channel names?','10. Which videos have the highest number of comments, and what are their corresponding channel names?'],index = 0)
  if choice == '1. What are the names of all the videos and their corresponding channels?':
    cur = conn.cursor()
    cur.execute('select c.Channel_Name,v.Video_Name from channel_details c left join playlist p on c.Channel_Id = p.Channel_Id join Videos v on v.Playlist_id = p.Playlist_id')
    a = []
    for i in cur:
        a.append(i)
    df = pd.DataFrame(a,columns = ['Channel_Name','Video_Name'])
    # st.dataframe(df)
    st.table(df)

  elif choice == '2. Which channels have the most number of videos, and how many videos do they have?':
    conn = mysql.connector.connect(host = 'localhost', password = 'Saravanan@246', user = 'root', port = 3307, database = 'project')
    cur = conn.cursor()
    cur.execute('select Channel_Name,format(Channel_Video_Count,"N") from channel_details  order by Channel_Video_Count desc')
    a = []
    
    for i in cur:
        a.append(i)
    df = pd.DataFrame(a,columns = ['Channel_Name','Channel_Video_Count'])
    st.dataframe(df)
    figure = px.bar(df,x='Channel_Name',y='Channel_Video_Count',color='Channel_Name',title='Channels contain most number of Videos',orientation='v',height=700)
    st.plotly_chart(figure,use_container_width=True)
    
  elif choice == '3. What are the top 10 most viewed videos and their respective channels?':
    cur = conn.cursor()
    cur.execute('select c.Channel_Name,v.Video_Name,format(v.Video_View_Count, "no")as Video_View_Count from channel_details c left join playlist p on c.Channel_Id = p.Channel_Id join Videos v on v.Playlist_id = p.Playlist_id order by v.Video_View_Count desc limit 10')
    a = []
    for i in cur:
        a.append(i)
    df = pd.DataFrame(a,columns = ['Channel_Name','Video_Name','Video_View_Count'])
    st.dataframe(df)
    # st.table(df)
    figure = px.bar(df,x='Channel_Name',y='Video_View_Count',color='Channel_Name',title='Top 10 most Viewed Videos',orientation='v',height=600)
    st.plotly_chart(figure,use_container_width=True)
    
  elif choice == '4. How many comments were made on each video, and what are their corresponding video names?':
    cur = conn.cursor()
    cur.execute('select Video_Name, format(Video_Comment_Count, "N")as Video_Comment_Count from Videos')
    a = []
    for i in cur:
        a.append(i)
    df = pd.DataFrame(a,columns = ['Video_Name','Video_Comment_Count'])
    #st.dataframe(df)
    st.table(df)

  elif choice == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    cur = conn.cursor()
    cur.execute('select c.Channel_Name,v.Video_Name,format(v.Video_Like_Count, "no")as Video_Like_Count from channel_details c left join playlist p on c.Channel_Id = p.Channel_Id join Videos v on v.Playlist_id = p.Playlist_id order by v.Video_Like_Count desc limit 10')
    a = []
    c = 0
    for i in cur:
        a.append(i)
    df = pd.DataFrame(a,columns = ['Channel_Name','Video_Name','Video_Like_Count'])
    st.dataframe(df)
    figure = px.bar(df,x='Channel_Name',y='Video_Like_Count',color='Channel_Name',title='Top 10 most Liked  Videos',orientation='v',height=600)
    st.plotly_chart(figure,use_container_width=True)

  elif choice == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    st.info('Officially, YouTube removed **👎 dislikes** to stop discrimination against smaller Channels in 2021')
    cur = conn.cursor()
    cur.execute('select Video_Name, format(Video_Like_Count,"no") from Videos order by Video_Like_Count desc')
    a = []
    for i in cur:
        a.append(i)
    df = pd.DataFrame(a,columns = ['Video_Name','Video_Like_Count'])
    #st.dataframe(df)
    st.table(df)

  elif choice == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    cur = conn.cursor()
    cur.execute('select Channel_Name, format(Channel_View_Count, "N") from channel_details order by Channel_View_Count desc')
    a = []
    for i in cur:
        a.append(i)
    df = pd.DataFrame(a,columns = ['Channel_Name','Channel_View_Count'])
    st.dataframe(df)
    figure = px.bar(df,x='Channel_Name',y='Channel_View_Count',color='Channel_Name',title='Top 10 most Channel View Count',orientation='v',height=600)
    st.plotly_chart(figure,use_container_width=True)

  elif choice == '8. What are the names of all the channels that have published videos in the year  2022?':
    cur = conn.cursor()
    cur.execute('select c.Channel_Name,v.Video_Name,v.Video_PublishedAt from channel_details c left join playlist p on c.Channel_Id = p.Channel_Id join Videos v on v.Playlist_id = p.Playlist_id where year(v.Video_PublishedAt) = 2022')
    a = []
    c = 0
    for i in cur:
        a.append(i)
    df = pd.DataFrame(a,columns = ['Channel_Name','Video_Name','Video_PublishedAt'])
    st.dataframe(df)
    figure = px.bar(df,x='Channel_Name',y='Video_PublishedAt',color='Channel_Name',title='Videos Published in year 2022',orientation='v',height=600)
    st.plotly_chart(figure,use_container_width=True)

  elif choice == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    cur = conn.cursor()
    cur.execute('select c.Channel_Name, AVG(v.Video_Duration) from channel_details c left join playlist p on c.Channel_Id = p.Channel_Id join Videos v on v.Playlist_id = p.Playlist_id group by c.Channel_Name')
    a = []
    c = 0
    for i in cur:
        a.append(i)
    df = pd.DataFrame(a,columns = ['Channel_Name','Average_Duration'])
    st.dataframe(df)
    figure = px.bar(df,x='Channel_Name',y='Average_Duration',color='Channel_Name',title='Average Duration of all Videos in each Channel',orientation='v',height=600)
    st.plotly_chart(figure,use_container_width=True)

  elif choice == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    cur = conn.cursor()
    cur.execute('select c.Channel_Name,v.Video_Name, format(v.Video_Comment_count, "NO") as Video_Comment_count from channel_details c left join playlist p on c.Channel_Id = p.Channel_Id join Videos v on v.Playlist_id = p.Playlist_id order by v.Video_Comment_Count desc limit 10')
    a = []
    name = []
    comm = []
    c = 0
    for i in cur:
        a.append(i)
        name.append(i[0])
        comm.append(i[2])
    df = pd.DataFrame(a,columns = ['Channel_Name','Video_Name','Video_Comment_count'])
    st.dataframe(df)
    figure = px.bar(df,x='Channel_Name',y='Video_Comment_count',color='Channel_Name',title='Average Duration of all Videos in each Channel',orientation='v',height=600)
    st.plotly_chart(figure,use_container_width=True)
  
#it is the code to hide the header and footer
hide = """
    <style>
    footer {visibility: hidden;}
    #header {visibility: hidden;}
    </style>
    """
st.markdown(hide,unsafe_allow_html = True)
