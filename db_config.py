import datetime
from sqlalchemy import create_engine
import psycopg2

db_days = 0

date2 = (datetime.datetime.today() - datetime.timedelta(db_days)).strftime('%m/%d/%Y')

con = psycopg2.connect(database="PdfExtractor", user="postgres", password="1234", host="localhost", port="5432")

cursor = con.cursor()


def db_data(property_name, pdf_files):
	cursor1 = con.cursor()
	cursor2 = con.cursor()

	arrival_query = f'''select guest_name as "Guest Name",company as "Company",rate as "Rate",rate_plan as "Rate Plan",arrival_date as "Arrival Date",
	COALESCE(depart_date,'') as "Depart Date",room_type as "Room Type",COALESCE(hilton_honor_tier,'') as "Loyalty" from 
	arrival_data where extraction_date='{date2}' and property_id = (select id from mst_property where property_name='{property_name}') ORDER BY company'''

	for e in pdf_files:

		if e.startswith(('guest', 'gstlist')):

			guest_query = f'''select guest_name as "Guest Name",company as "Company",COALESCE(rate,'') as "Rate", room_number as "Room Number", 
			guest_status  as "Guest Status", "group" as "Group" from guest_list where extraction_date='{date2}' 
			and property_id = (select id from mst_property where property_name='{property_name}') ORDER BY guest_name'''

		elif e.startswith('IN HOUSE'):
			guest_query = f'''select guest_name as "Guest Name",company as "Company", room_number as "Room Number", room_type as "Room Type", 
						room_status  as "Room Status", gt as "GT" , mbv_level as "Mbv Level", 
							arrival_date as "Arrival Date", depart_date as "Depart Date", city as "City" from guest_list where extraction_date='{date2}' 
							and property_id = (select id from mst_property where property_name='{property_name}') ORDER BY guest_name'''

	cursor1.execute(arrival_query)
	cursor2.execute(guest_query)

	column_name1 = []
	for c1 in cursor1.description:
		c11 = c1[0]
		column_name1.append(c11)

	column_name2 = []
	for c2 in cursor2.description:
		c22 = c2[0]
		column_name2.append(c22)

	data1 = cursor1.fetchall()
	data2 = cursor2.fetchall()

	arrival_file = f"Today's Arrivals - {len(data1)}"
	guest_file = f"In-House Guests - {len(data2)}"

	tab1Data = ""
	tab1Header = ""

	tab2Data = ""
	tab2Header = ""

	for a in column_name1:
		tab1Header += '<th class="thhead">' + str(
			a) + " </th>"

	for b in data1:
		tab1Data += '''<tr class="trdata">'''
		for c in b:
			tab1Data += '<td class="tddata" style="white-space: nowrap;">' + str(
				c) + "</td>"
		tab1Data += "</tr>"

	for a2 in column_name2:
		tab2Header += '<th class="thhead">' + str(
			a2) + " </th>"

	for b2 in data2:
		tab2Data += '''<tr class="trdata">'''
		for c2 in b2:
			tab2Data += '<td class="tddata" style="white-space: nowrap;">' + str(
				c2) + "</td>"
		tab2Data += "</tr>"

	html = """
					<!DOCTYPE html>
					<html lang="en">

					<head>
					<link href="https://fonts.googleapis.com/css2?family=Inter:wght@100;200;300;400;500;600;700;800;900&display=swap"
							rel="stylesheet">
					<style>

					body{
						margin:0; 
						background:#f9f9f9;
					}
					div.main{
						margin-bottom: 20px; 
						background: #234b81; 
						text-transform: uppercase; 
						padding: 30px 0; 
						text-align: center; 
						font-weight: 600; 
						font-size: 28px; 
						line-height: 34px; 
						color: #FFFFFF; 
						font-family: 'Inter';
					}
					div.main2{
						padding:0 20px;
					}
					div.main3{
						float: left;
						width: 100%;
					}
					div.main4{
						float: left; 
						width: 100%; 
						text-align: center; 
						font-size: 22px; 
						color: #0e418d; 
						font-weight: 700; 
						margin-bottom: 20px; 
						font-family: 'Inter';
					}
					div.main5{
						width:100%; 
						float: left; 
						margin-top: -4px;
					}
					div.tableresp table-responsive{
						display: flex; 
						overflow: auto;
					}
					table.table{
						width: 100%; 
						border-collapse: collapse;
					}
					thead{
						color: #FFFFFF;
					}
					tr.trhead{
						height: 48px;
						font-family: 'Inter'; 
						font-weight: 600; 
						font-size: 16px; 
						line-height: 19px;
					}
					th.thhead{
						background: #24628c; 
						padding: 0 15px; 
						width:100px; 
						text-align: left; 
						white-space: nowrap; 
						border-left:1px solid #27597f;
					}
					tr.trdata{
						height: 45px;  
						font-family: Inter; 
						font-weight: 400; 
						font-size: 14px; 
						line-height: 17px; 
						color: #000000; 
						border-bottom:1px solid #ededee;
					}
					td.tddata{
						background: #f5f5f5;  
						border-right: 0; 
						border-left: 0; 
						padding: 0 15px; 
						text-align: left; 
						border-left:1px solid #ededee;
					}


					</style>
					<meta http-equiv="Content-Type" content="text/html; charset=utf-8"><title>Data</title>
					</head>

					<body>
						<div>
							<div class="main">
								Kriya Hotels
							</div>
							<div calss="main2">
								<div class="main3">
									<div class="main4">
										""" + property_name + """
									</div>
									
									<div class="main5">
										<div class="main4">
											""" + arrival_file.upper() + """
										</div>
										<div  class="tableresp table-responsive">
											<table class="table">
												<thead>
													<tr class="trhead">

														""" + tab1Header + """


													</tr>
												</thead>
												<tbody>
													 """ + tab1Data + """
												</tbody>
											</table>

										<br>
										<br>
										
									   
										<div class="main4">
										""" + guest_file.upper() + """
										</div>
										<div class="tableresp table-responsive">

											<table class="table">
												<thead>
													<tr class="trhead" >
														""" + tab2Header + """
													</tr>
												</thead>
												<tbody>
													 """ + tab2Data + """
												</tbody>
											</table>
										</div>
									</div>
								</div>
							</div>
						</div>

					</body>

					</html>
					"""

	return html


def insert_data(item):
	try:

		field_list = []
		value_list = []
		for field in item:
			field_list.append(str(field))
			value_list.append(str(item[field]).replace("'", "’"))
		fields = ','.join(field_list)
		values = "','".join(value_list)
		insert_db = "insert into " + "arrival_data" + "( " + fields + " ) values ( '" + values + "' )"

		cursor.execute(insert_db)
		con.commit()

	except Exception as e:
		print('problem in data insert ', str(e))


def insert_guest_data(item):
	try:

		field_list = []
		value_list = []
		for field in item:
			field_list.append(str(field))
			value_list.append(str(item[field]).replace("'", "’"))
		fields = ','.join(field_list)
		values = "','".join(value_list)
		insert_db = "insert into " + "guest_list" + "( " + fields + " ) values ( '" + values + "' )"

		cursor.execute(insert_db)
		con.commit()

	except Exception as e:
		print('problem in data insert ', str(e))


def sql_insert(df):
	conn_string = 'postgresql://postgres:1234@localhost/PdfExtractor'
	db = create_engine(conn_string)
	con = db.connect()
	df.to_sql('guest_list', con=con, index=False, if_exists='append')
	con.commit()


def arrival_df_insert(df):
	conn_string = 'postgresql://postgres:1234@localhost/PdfExtractor'
	db = create_engine(conn_string)
	con = db.connect()
	df.to_sql('arrival_data', con=con, index=False, if_exists='append')
	con.commit()