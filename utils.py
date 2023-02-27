import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from db_config import db_data,con,cursor

days=1

today_date = (datetime.datetime.today() - datetime.timedelta(days)).strftime('%Y-%m-%d')

date1=(datetime.datetime.today() - datetime.timedelta(days)).strftime("%d-%b-%Y")

sender_address = 'raj.patel@softqubes.com'
sender_pass = 'hogxjfiiwosuarus'

receiver_address = ['yash.malani@softqubes.com']
ccs = ['']


def get_loggger(filename):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger_formatter = logging.Formatter('[%(asctime)s][%(name)s][Line %(lineno)d]'
                                         '[%(levelname)s]:%(message)s')

    file_handler = logging.FileHandler(f'logs/{filename}.txt', mode='w')

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logger_formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logger_formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


logger = get_loggger(f'Log_{today_date}')


def mail_sent(files, property_name, sender_mail,pdf_files):
    try:
        logger.info(f'Sending mail for {property_name}')
        mail_content = f'<p><h3>Here is the reports for {property_name}</h3></p><br>'

        message = MIMEMultipart()
        message['From'] = sender_address
        message['To'] = ','.join(receiver_address)
        message['Cc'] = ','.join(ccs)
        message['Subject'] = f'Sales Reports for {property_name} : {today_date}'
        message.attach(MIMEText(mail_content, 'html'))

        html=db_data(property_name,pdf_files)

        part2 = MIMEText(html, 'html')

        message.attach(part2)

        session = smtplib.SMTP('smtp.gmail.com', 587)
        session.starttls()
        session.login(sender_address, sender_pass)
        text = message.as_string()
        session.sendmail(sender_address, (receiver_address+ccs), text)
        session.quit()

        logger.info(f'Mail sent for {property_name}\n')

        cursor.execute(f"select id from mst_property where property_name='{property_name}'")
        id = cursor.fetchone()
        update = f"update as_of_run set status='Done' where property_id='{id[0]}'"

        cursor.execute(update)
        con.commit()

    except Exception as e:
        logger.debug(e)


def send_log(main_dir,property_name):

    try:
        mail_content = f"Log for today's pdf extraction"
        logger.info(f'Log mail sent.')
        message = MIMEMultipart()
        message['From'] = sender_address
        message['To'] = ','.join(receiver_address)
        message['Cc'] = ','.join(ccs)
        message['Subject'] = f'Log file for Date : {today_date}'
        message.attach(MIMEText(mail_content, 'plain'))

        logfile_name = f'{main_dir}\\logs\\Log_{today_date}.txt'
        filename=logfile_name.split('\\')[-1]
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(logfile_name, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        message.attach(part)
        session = smtplib.SMTP('smtp.gmail.com', 587)
        session.starttls()
        session.login(sender_address, sender_pass)
        text = message.as_string()
        session.sendmail(sender_address, (receiver_address+ccs), text)
        session.quit()

        cursor.execute(f"select id from mst_property where property_name='{property_name}'")
        id = cursor.fetchone()
        with open(f'{main_dir}\\logs\\Log_{today_date}.txt', 'r') as f1:
            data=f1.read()
        update = f"update as_of_run set log_text='{data}' where property_id='{id}'"
        cursor.execute(update)
        con.commit()

    except Exception as e:
        logger.debug(e)


