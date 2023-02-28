from __future__ import print_function
import datetime
import glob
import pickle
import csv
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64
from main_functions import expected_arrivals, gstchkin_csv, guestlist_csv, inhouseguests, arrival_landscape_new, \
	guest_list, remaining_arrivals
from utils import mail_sent, send_log, logger
import psycopg2

SCOPES = ['https://www.googleapis.com/auth/gmail.modify', 'https://www.googleapis.com/auth/gmail.send']

main_dir = os.getcwd()

con = psycopg2.connect(database="PdfExtractor", user="postgres", password="1234", host="localhost", port="5432")

cursor = con.cursor()


def prep_service():
	creds = None

	if os.path.exists(f'{main_dir}\\token.pickle'):
		with open(f'{main_dir}\\token.pickle', 'rb') as token:
			creds = pickle.load(token)

	if not creds or not creds.valid:
		if creds and creds.expired and creds.refresh_token:
			creds.refresh(Request())
		else:
			flow = InstalledAppFlow.from_client_secrets_file(
				f'{main_dir}\\credentials.json', SCOPES)
			creds = flow.run_local_server()

		with open(f'{main_dir}\\token.pickle', 'wb') as token:
			pickle.dump(creds, token)

	service = build('gmail', 'v1', credentials=creds)
	return service


def create_filter(label, archiveLabel):
	filter = "-label:" + archiveLabel
	filter += " label:" + label
	return "has:attachment " + filter


def get_archive_label(archiveLabelName):
	service = prep_service()
	response = service.users().labels().list(userId="me").execute()
	labels = response["labels"]
	for pulled_label in labels:
		if pulled_label["name"] == archiveLabelName:
			return pulled_label

	body = {
		"type": "user",
		"name": archiveLabelName,
		"messageListVisibility": "show",
		"labelListVisibility": "labelShow"
	}

	response = service.users().labels().create(userId="me",
											   body=body).execute()
	return response


service = prep_service()

with open('property_codes.csv') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:

		label_name = row["label"]

		attachment_format = row["save"]

		property_name = row["property_name"]

		days = 0

		today_date = (datetime.datetime.today() - datetime.timedelta(days)).strftime('%Y-%m-%d')

		file_path = f'{main_dir}\\RajChudasama\\{today_date}\\{property_name}'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		logger.debug(create_filter(label_name, "Saved"))

		response = service.users().messages().list(userId="me", q=create_filter(label_name, "Saved")).execute()

		if 'messages' in response:
			messages = response['messages']

			# get first message only
			message = messages[0]
			a_message = service.users().messages().get(userId="me",
													   id=message["id"]
													   ).execute()

			for part in a_message['payload']['parts']:
				save_flag = False

				file_name = part['filename']

				if file_name:

					if attachment_format == "all":
						save_flag = True
					elif file_name.split(".")[-1] == attachment_format:
						save_flag = True

					if save_flag:
						if 'data' in part['body']:
							file_data = base64.urlsafe_b64decode(part['body']['data'].encode('UTF-8'))
						else:
							attachment_id = part['body']['attachmentId']
							attachment = service.users().messages().attachments().get(userId="me",
																					  messageId=message["id"],
																					  id=attachment_id).execute()
							data = attachment['data']
							file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))

						full_path = os.path.join(file_path, file_name)
						with open(full_path, 'wb') as fd:
							fd.write(file_data)
							fd.close()

						# save message asap

						archive_label = get_archive_label("Saved")
						label_apply_body = {
							"addLabelIds": archive_label["id"]
						}
						response = service.users().messages().modify(userId="me",
																	 id=message["id"],
																	 body=label_apply_body).execute()

					else:
						logger.error(f"Attachment format match fail for message "
									 f"{a_message['id']} for {attachment_format}"
									 f" and {file_name.split('.')[-1]}")

			saved_messages_ids = []
			for message in messages:
				saved_messages_ids.append(message["id"])
			# logger.info(saved_messages_ids)
			archive_label = get_archive_label("Saved")
			logger.info(f"{archive_label['name']} : {archive_label['id']}")

			# Apply archive label to saved messages
			label_apply_body = {
				"addLabelIds": archive_label["id"],
				"ids": saved_messages_ids
			}

			if saved_messages_ids:
				response = service.users().messages().batchModify(userId="me",
																  body=label_apply_body
																  ).execute()
				saved_messages_count = len(saved_messages_ids)
				logger.info(f"Saved label applied to {saved_messages_count} messages.")
			else:
				logger.info("No messages to save")

			sender_mail = ''

			try:

				pdf_files = []

				logger.info(f'Scraping starts for {property_name}.')
				cursor.execute(f"select id,client_id from mst_property where property_name='{property_name}'")
				id = cursor.fetchone()

				insert = f"INSERT INTO as_of_run (property_id, client_id, date,is_error,status) VALUES ('{str(id[0])}', '{str(id[1])}', '{str(today_date)}','false','Processing');"
				cursor.execute(insert)
				con.commit()

				for file in glob.glob(file_path + '\\*'):
					if '.pdf' in file or '.csv' in file:

						file_name = file.split('\\')[-1]

						logger.info(f'Getting data for {property_name}/{file_name}.')
						if '.pdf' in file_name:
							pdf_files.append(file_name.split('.pdf')[0])
						elif '.csv' in file_name:
							pdf_files.append(file_name.split('.csv')[0])

						if file_name.startswith('EXPECTED ARRIVALS') and file_name.endswith(".pdf"):
							expected_arrivals(file, id)

							logger.info(f'Excel is generated for {property_name}/{file_name}.')

						if file_name.startswith('IN HOUSE') and file_name.endswith(".pdf"):
							inhouseguests(file, id)

							logger.info(f'Excel is generated for {property_name}/{file_name}.')

						if file_name.startswith(('guest', 'gstlist')) and file_name.endswith(".pdf"):
							guest_list(file, id)

							logger.info(f'Excel is generated for {property_name}/{file_name}.')

						if file_name.startswith(('arrivalllandscape', 'arrivals')) and file_name.endswith(".pdf"):
							arrival_landscape_new(file, id)

							logger.info(f'Excel is generated for {property_name}/{file_name}.')

						if file_name.startswith('remaining') and file_name.endswith(".pdf"):
							remaining_arrivals(file, id)

							logger.info(f'Excel is generated for {property_name}/{file_name}.')

						if file_name.startswith('gstchkin') and file_name.endswith(".csv"):
							gstchkin_csv(file, id)

							logger.info(f'Excel is generated for {property_name}/{file_name}.')

						if file_name.startswith('gstlista') and file_name.endswith(".csv"):
							guestlist_csv(file, id)

							logger.info(f'Excel is generated for {property_name}/{file_name}.')

				if pdf_files:
					logger.info(f'All files are scraped for {property_name}')

					mail_sent(file_path, property_name, sender_mail, pdf_files)

			except Exception as e:
				logger.debug(e)

		else:
			logger.info(f"No new messages for {label_name} label")

	send_log(main_dir, property_name)
