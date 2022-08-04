import mysql.connector as conn
import pandas as pd


#--------SQL ------------------

mysql_db = conn.connect(host = "localhost", user="root", passwd="rootroot", allow_local_infile=True)
crs = mysql_db.cursor()

#pip install csvkit
# csvsql --dialect mysql --snifflimit 100000  /Users/nikhilgopalakrishnan/Documents/Sinara/FSDS/"FitBit_data.csv" > /Users/nikhilgopalakrishnan/Documents/Sinara/FSDS/fitbit_table.sql
cre_ord_sql="CREATE TABLE if not exists sinara.Superstore_USA_Orders ( \
	`Row ID` DECIMAL(38, 0) NOT NULL, \
	`Order Priority` VARCHAR(13) NOT NULL, \
	`Discount` DECIMAL(38, 2) NOT NULL, \
	`Unit Price` DECIMAL(38, 2) NOT NULL, \
	`Shipping Cost` DECIMAL(38, 2) NOT NULL, \
	`Customer ID` DECIMAL(38, 0) NOT NULL, \
	`Customer Name` VARCHAR(28) NOT NULL, \
	`Ship Mode` VARCHAR(14) NOT NULL, \
	`Customer Segment` VARCHAR(14) NOT NULL, \
	`Product Category` VARCHAR(15) NOT NULL, \
	`Product Sub-Category` VARCHAR(30) NOT NULL, \
	`Product Container` VARCHAR(10) NOT NULL, \
	`Product Name` VARCHAR(98) NOT NULL, \
	`Product Base Margin` DECIMAL(38, 2), \
	`Region` VARCHAR(7) NOT NULL, \
	`State or Province` VARCHAR(20) NOT NULL, \
	`City` VARCHAR(19) NOT NULL, \
	`Postal Code` DECIMAL(38, 0) NOT NULL, \
	`Order Date` DATE NOT NULL, \
	`Ship Date` DATE NOT NULL, \
	`Profit` DECIMAL(38, 7) NOT NULL, \
	`Quantity ordered new` DECIMAL(38, 0) NOT NULL, \
	`Sales` DECIMAL(38, 2) NOT NULL, \
	`Order ID` DECIMAL(38, 0) NOT NULL \
)"

crs.execute(cre_ord_sql)
mysql_db.commit()

#Loaded data using below command
# csvsql --db mysql+pymysql://root:rootroot@localhost/sinara --no-create  --insert /Users/nikhilgopalakrishnan/Documents/Sinara/FSDS/"Superstore_USA_Orders.csv"


#---Returns---
cre_rtns_sql = "CREATE TABLE if not exists sinara.Superstore_USA_Returns ( \
	`Order ID` DECIMAL(38, 0) NOT NULL, \
	`Status` VARCHAR(8) NOT NULL    \
)"
crs.execute(cre_rtns_sql)
mysql_db.commit()

#Loaded data using below command
#csvsql --db mysql+pymysql://root:rootroot@localhost/sinara --no-create  --insert /Users/nikhilgopalakrishnan/Documents/Sinara/FSDS/"Superstore_USA_Orders.csv"


join_sql = "select * from sinara.Superstore_USA_Orders ord inner join sinara.Superstore_USA_Returns rtn \
           on ord.`Order ID` = rtn.`Order ID`"

crs.execute(join_sql)
for i in crs.fetchall():
    print("\n",i)
