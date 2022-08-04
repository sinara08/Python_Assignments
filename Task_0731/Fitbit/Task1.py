import mysql.connector as conn
import pandas as pd
import pymongo
import json

#-------------mangoDB------------
client = pymongo.MongoClient("mongodb+srv://sinara:rootroot@sincluster.0lcik.mongodb.net/?retryWrites=true&w=majority")
mymongo_db = client.test


#--------SQL ------------------

mysql_db = conn.connect(host = "localhost", user="root", passwd="rootroot", allow_local_infile=True)
crs = mysql_db.cursor()

#pip install csvkit
# csvsql --dialect mysql --snifflimit 100000  /Users/nikhilgopalakrishnan/Documents/Sinara/FSDS/"FitBit_data.csv" > /Users/nikhilgopalakrishnan/Documents/Sinara/FSDS/fitbit_table.sql
cre_sql = "CREATE TABLE  if not exists sinara.FitBit_data ( \
	`Id` DECIMAL(38, 0) NOT NULL, \
	`ActivityDate` DATE NOT NULL,\
	`TotalSteps` DECIMAL(38, 0) NOT NULL,\
	`TotalDistance` DECIMAL(38, 17) NOT NULL,\
	`TrackerDistance` DECIMAL(38, 17) NOT NULL,\
	`LoggedActivitiesDistance` DECIMAL(38, 15) NOT NULL,\
	`VeryActiveDistance` DECIMAL(38, 17) NOT NULL,\
	`ModeratelyActiveDistance` DECIMAL(38, 16) NOT NULL,\
	`LightActiveDistance` DECIMAL(38, 17) NOT NULL,\
	`SedentaryActiveDistance` DECIMAL(38, 17) NOT NULL,\
	`VeryActiveMinutes` DECIMAL(38, 0) NOT NULL,\
	`FairlyActiveMinutes` DECIMAL(38, 0) NOT NULL,\
	`LightlyActiveMinutes` DECIMAL(38, 0) NOT NULL,\
	`SedentaryMinutes` DECIMAL(38, 0) NOT NULL,\
	`Calories` DECIMAL(38, 0) NOT NULL\
)"
crs.execute(cre_sql)

#--Since I have used "Load data local infile" in my previous task, I have used below csvkit command to load the data
#csvsql --db mysql+pymysql://root:rootroot@localhost/sinara --no-create  --insert /Users/nikhilgopalakrishnan/Documents/Sinara/FSDS/"FitBit_data.csv"
#Date fields are already in Date format

#4 . Find out in this data that how many unique id's we have
unq_sql = "select count(distinct id) from sinara.FitBit_data"
crs.execute(unq_sql)
print("4. Find out in this data that how many unique id's we have:")
print(crs.fetchall()[0][0])
print("=========================================================\n")


#5 . which id is one of the active id that you have in whole dataset
#active person is considered as highest TotalSteps who is regular in activities
act_id_sql="select id from \
            (select id,sum(TotalSteps) tot_sum , sum(case when TotalSteps = 0 then 1 else 0 end) zero_step,rank() over(order by sum(TotalSteps) desc) rnk \
                from sinara.FitBit_data group by 1 having zero_step = 0 \
            ) a where rnk = 1"
crs.execute(act_id_sql)
print("5.Most active id")
print(crs.fetchall()[0][0])
print("=========================================================\n")

#6 . how many of them have not logged there activity find out in terms of number of ids
No_Log_hr = "select count(id) from (select id, sum(LoggedActivitiesDistance) log_tot from sinara.FitBit_data group by 1 \
                having log_tot = 0.000000000000000) a "

crs.execute(No_Log_hr)
print("6.how many of them have not logged there activity. Find out in terms of number of ids")
print(crs.fetchall()[0][0])
print("=========================================================\n")

#7 . Find out who is the laziest person id that we have in dataset
lazy_id = "select id from \
                (select id, count(distinct ActivityDate) cnt, rank() over(order by count(distinct ActivityDate)  asc) rnk \
                from sinara.FitBit_data group by 1 order by cnt asc) a \
            where rnk = 1"
crs.execute(lazy_id)
print("7 . Find out who is the laziest person id that we have in dataset")
for i in crs.fetchall():
    print(i[0],sep=' ', end=' ')
#print(crs.fetchall()[0][0])
print("=========================================================\n")

#8 . Explore over an internet that how much calories burn is required for a healthy person and find out how many healthy person we have in our dataset
#Considering calories to be burnt is 2200

hlthy_id_cnt = "select count(distinct id) from sinara.FitBit_data where Calories > 2200"
crs.execute(hlthy_id_cnt)
print("8.how many healthy person we have")
print(crs.fetchall()[0][0])
print("=========================================================\n")

#9. how many person are not a regular person with respect to activity try to find out those
irreg_id = "select count(distinct id) from sinara.FitBit_data where TotalSteps=0"
crs.execute(irreg_id)
print("9.how many person are not a regular person with respect to activity try to find out those")
print(crs.fetchall()[0][0])
print("=========================================================\n")

#10 . who is the third most active person in this dataset find out those in pandas and in sql both .
#active person is considered as highest TotalSteps with regular activities
actv_id = "select id from \
            (select id,sum(TotalSteps) tot_sum , sum(case when TotalSteps = 0 then 1 else 0 end) zero_step,rank() over(order by sum(TotalSteps) desc) rnk \
                from sinara.FitBit_data group by 1 having zero_step = 0 \
            ) a where rnk = 3"
crs.execute(actv_id)
print("10.third most active person in this dataset")
print(crs.fetchall()[0][0])
print("=========================================================\n")

#11 . who is the 5th most laziest person avilable in dataset find it out
#Lazy person is considered as Lowest TotalSteps with irregular activities
laz_id = "select id from \
            (select id,sum(TotalSteps) tot_sum , sum(case when TotalSteps = 0 then 1 else 0 end) zero_step,rank() over(order by sum(TotalSteps) asc) rnk \
                from sinara.FitBit_data group by 1 having zero_step <> 0 \
            ) a where rnk = 5"
crs.execute(laz_id)
print("11.Fifth most laziest person in this dataset")
print(crs.fetchall()[0][0])
print("=========================================================\n")

#12 . what is a totla acumulative calories burn for a person find out

cal_tot = "select id, sum(Calories) from sinara.FitBit_data group by 1"

crs.execute(cal_tot)
print("12 . what is a total acumulative calories burn for a person find out")
for i,j in crs.fetchall():
    print("Id - {} and Sum of Calories {}".format(i,j))
print("=========================================================\n")

#-------------mangoDB------------
client = pymongo.MongoClient("mongodb+srv://sinara:rootroot@sincluster.0lcik.mongodb.net/?retryWrites=true&w=majority")
mymongo_db = client.test

#----------Read data set as dataframe---------------
crs.execute('select * from sinara.FitBit_data')
df_fitbit = pd.DataFrame(data=crs.fetchall())


#----------(4)convert fitbit datset to json ------------
attr_data = df_fitbit.to_json('fitbit.json')

#----------(5)Load Attribute data to MongoDB ---------------
with open('fitbit.json','r') as file:
    file_data = json.load(file)


database = client['Fitbit']
collection = database["StepDetails"]
collection.insert_one(file_data)
