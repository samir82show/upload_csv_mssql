#!/opt/rh/rh-python36/root/usr/bin/python3.6
import pymssql
import pandas as pd
import time
#import argparse


#ap = argparse.ArgumentParser()
#ap.add_argument("-c", "--csvreport", required=True, help="DPA csv report")
#args = vars(ap.parse_args())


today = time.strftime("%Y-%m-%d") + "%"
server = "Servername\\InstanceName"
user = "Username"
password = "Password"
conn = pymssql.connect(server, user, password, "DBname", port = "portnumber")

#df = pd.read_csv(args["csvreport"])
df = pd.read_csv("/CSV_file_full_path")

site = df["Object"].str.split(":", n = 2, expand = True)[0]
domains = df["Object"].str.split(":", n = 2, expand = True)[2].str.split(":", n = 1, expand = True)[0]
groups = df["Object"].str.split(":", n = 2, expand = True)[2].str.split(":", n = 1, expand = True)[1]
df["domains"] = domains
df["groups"] = groups
df["site"] = site

if df.columns[5] == 'Size (TB)':
	df["Size (GB)"] = df["Size (TB)"] * 1024
#print (df["Size (GB)"])

#df.drop(["Object", "Success Rate (%)", "Active"], axis = 1, inplace = True)
print(df.head(10))
cursor = conn.cursor()

#Index(['Completed', 'Succeeded', 'Failed', 'Size (GB)', 'domains', 'groups'], dtype='object')

cursor.execute("DELETE FROM dbo.BackupStatus WHERE date like %s", today) 

for index,row in df.iterrows():
	#print("(%s, %s, %s, %d, %d, %d, %d)" % (row["site"], row["domains"], row["groups"], row["Completed"], row["Succeeded"], row["Failed"], row["Size (GB)"]))
	cursor.execute("INSERT INTO dbo.BackupStatus(Server, domains, groups, completed, succeeded, failed, Size_in_GB) VALUES (%s, %s, %s, %d, %d, %d, %d)", \
	 (row["site"], row["domains"], row["groups"], row["Completed"], row["Succeeded"], row["Failed"], row["Size (GB)"]))

cursor.execute("SELECT COUNT(*) FROM table")

for i in cursor: print(i[0])

conn.commit()
conn.close()
