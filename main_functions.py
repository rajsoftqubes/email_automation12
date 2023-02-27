import datetime
import re
from utils import logger
from db_config import insert_data, insert_guest_data, sql_insert, arrival_df_insert
import pdfplumber
import pandas as pd
import camelot
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

extraction_date=datetime.datetime.today().strftime('%m/%d/%Y')


def arrival_landscape_new(file,id):


    try:

        pdf = pdfplumber.open(file)

        page = pdf.pages

        for p in page:
            text = p.extract_text()

            data = text.split('AUTH')[-1]
            if '/' not in data:
                data=text.split('AUTH')[-2]

            data = data.splitlines()

            for index, d in enumerate(data):

                if '/' in d:

                    name = re.findall(r'(.*?)\w{1}\s+\-\s+', d)
                    if name == []:
                        name = re.findall(r'(.*?)\s{3}\w{1}\s+\w{1}', d)

                    if name:
                        name = name[0]

                        tier = re.findall(r'(\w{1})(\s{1}\-\s+\d{9})', d)
                        if tier:
                            tier = tier[0][0]
                        else:
                            tier = ''

                        price = re.findall(r'(\$\d+\.\d+)', d)
                        if price:
                            price = price[0]
                        else:
                            price = ''

                        rate_plan = d.split(' ')[-1]

                        d1 = data[index + 1]

                        rt = re.findall(r'(\d{8}\s+\w{3,})', d1)
                        if rt:
                            room_type = rt[0].split(' ')[-1]
                        else:
                            room_type = ''

                        com = re.findall(r'(.*?)\d{8}', d1)
                        if com:
                            com = com[0]

                        try:
                            nights = re.findall(r'(\w{2}\s{1})(\s+\d{,2}\s+)(\d{1}\,\d{1})', d)
                            if nights == []:
                                nights = re.findall(r'(\w{2})(\s+\d{,2}\s+)(\w{1}\s+)(\d{1}\,\d{1})', d)
                            if nights:
                                nights = int(nights[0][1].strip())

                            depart_date = (datetime.datetime.today() + datetime.timedelta(nights)).strftime('%m/%d/%Y')
                        except Exception as e:
                            depart_date = ''

                        if tier:
                            tier=tier.strip()

                        if tier=='B':
                            tier='Blue'
                        elif tier=='S':
                            tier='Silver'
                        elif tier == 'G':
                            tier = 'Gold'
                        elif tier == 'D':
                            tier = 'Diamond'

                        item = {
                            'guest_name': name,
                            'hilton_honor_tier': tier,
                            'company': com,
                            'rate': price,
                            'depart_date':depart_date,
                            'rate_plan': rate_plan,
                            'arrival_date': extraction_date,
                            'room_type': room_type,
                            'property_id':id[0],
                            'extraction_date':extraction_date,
                        }

                        insert_data(item)

    except Exception as e:

        logger.debug(e)


def expected_arrivals(file,id):

    try:

        pdf = pdfplumber.open(file)

        page = pdf.pages

        for p in page:
            text = p.extract_text()

            data = text.splitlines()

            for index, d in enumerate(data):

                code = re.findall(r'(\w{2}\s+\@\d{5})', d)

                if code:

                    roomtype = code[0].split()[0]

                    name = re.findall(r'(\w+\,\s+\w+)|(\w+\-\w+\,\s+\w+)', d)

                    for n in name[0]:
                        if n == '':
                            pass
                        else:
                            name = n
                            break

                    groupcode = re.findall(r'(\w{2}\d{4})', d)
                    if groupcode:
                        groupcode = groupcode[0]
                    else:
                        groupcode = ''

                    departdate = re.findall(r'(\d{2}\-\w{3}\-\d{2})', d)
                    if departdate:
                        departdate = departdate[0]
                        departdate=datetime.datetime.strptime(departdate,'%d-%b-%y').strftime('%m/%d/%Y')
                    else:
                        departdate = ''

                    rateplan = re.findall(r'(\d{2}\-\w{3}\-\d{2}\s+)(\w+)', d)
                    if rateplan:
                        rateplan = rateplan[0][-1]
                    else:
                        rateplan = ''

                    d1 = data[index + 1]

                    price = re.findall(r'(\d+\.\d+)', d1)
                    if price:
                        price = '$'+price[0]
                    else:
                        price = ''

                    m1 = re.findall(r'(\d{8})', d1)[0]
                    com = d1.split(m1)[-1]

                    company = re.findall(r'(.*?)(\d{2}\:\d{2})', com)
                    if company:
                        company = company[0][0]
                    else:

                        company = re.findall(r'(.*?)(\d+\.\d+)', com)
                        if company:
                            company = company[0][0]

                    d2 = data[index + 2]

                    arrivaldate = re.findall(r'(\d{2}\-\w{3}\-\d{2})', d2)
                    if arrivaldate:
                        arrivaldate = arrivaldate[0]
                    else:
                        arrivaldate = ''

                    q1 = re.findall(r'(\d{2}\-\w{3}\-\d{2})(\s\w+)', d2)
                    if q1:

                        q3 = q1[0][-1].strip()
                        if len(q3) > 5:
                            ocn = q3
                            company = company + ' ' + ocn

                    item = {
                        'guest_name': name,
                        'group_code': groupcode,
                        'company': company,
                        'rate': price,
                        'rate_plan': rateplan,
                        'depart_date':departdate,
                        'arrival_date': extraction_date,
                        'room_type': roomtype,
                        'property_id': id[0],
                        'extraction_date': extraction_date,
                    }

                    insert_data(item)

    except Exception as e:
        logger.debug(e)


def gstchkin_csv(file,id):
    
    date=(datetime.datetime.today()+datetime.timedelta(1)).strftime('%m/%d/%Y')
    df=pd.read_csv(file, encoding='utf-8', header=None)
    df=df.fillna('')
    df1 = df.iloc[:, 29:48]

    new=df1[[29,30,43,44,46]].copy()

    new1 = new.rename(columns={29: 'Guest Name', 30: 'Hilton Honor Tier',43:'Rate', 44:'Rate Plan', 46:'Room Type'})

    new1['Company']=''
    new1['Arrival Date']=date

    new1 = new1[['Guest Name', 'Hilton Honor Tier', 'Company', 'Rate', 'Rate Plan', 'Arrival Date', 'Room Type']]

    new1['property_id']=id[0]
    new1['extraction_date']=extraction_date

    new1.rename(columns={'Guest Name': 'guest_name', 'Hilton Honor Tier': 'hilton_honor_tier',
                                'Company': 'company', 'Rate':'rate', 'Rate Plan':'rate_plan','Room Type':'room_type',
                         'Arrival Date':'arrival_date'}, inplace=True)

    new1=new1[['guest_name', 'hilton_honor_tier', 'company', 'rate', 'rate_plan', 'arrival_date', 'room_type', 'property_id', 'extraction_date']]

    new1['hilton_honor_tier'] = new1['hilton_honor_tier'].replace(['B', 'S', 'G', 'D'], ['Blue', 'Silver','Gold', 'Diamond'])

    arrival_df_insert(new1)


def guest_list(file,id):

    try:

        tables = camelot.read_pdf(file, pages='all', encoding="utf-8", flavor='stream', suppress_stdout=False)
        pages = tables.n

        frames = []
        try:

            for i in range(0, pages):
                df = tables[i].df
                
                if i==0:
                    df = df.iloc[3:]
                    df = df.drop(index=4)
                    df.columns = df.iloc[0]
                    df = df.drop(index=3)
                else:
                    df = df.drop(index=1)

                    df.columns = df.iloc[0]
                    df = df.drop(index=0)

                columns=list(df.columns.values)

                if 'ROOM\nTITLE' in columns:
                   
                    dffs = df['ROOM\nTITLE'].str.split('\n', expand=True)
                    total_col=len(dffs.columns)
                    if total_col==1:
                        df['ROOM'] = df['ROOM\nTITLE'].str.split('\n', expand=True)[0]
                        df['TITLE'] = ''
                    elif total_col==2:

                        df[['ROOM', 'TITLE']] = df['ROOM\nTITLE'].str.split("\n", expand=True)
                    df=df.fillna('')
                    df['NAME'] = df[['TITLE', 'NAME']].agg(' '.join, axis=1)
                elif 'TITLE\nNAME' in columns:
                    df['NAME'] = df['TITLE\nNAME'].str.replace("\n",' ')
                else:
                    df['NAME'] = df[['TITLE', 'NAME']].agg(' '.join, axis=1)

                df = df[['ROOM','NAME', 'GUEST', 'COMPANY NAME', 'GROUP']]
                
                frames.append(df)
                
        except Exception as e:
            logger.debug(e)

        result = pd.concat(frames)

        df = result.rename(columns={'ROOM': 'room_number',  'NAME': 'guest_name', 'GUEST': 'guest_status',
                                'COMPANY NAME': 'company', 'GROUP': 'group'})

        df['property_id'] = id[0]
        df['extraction_date'] = (datetime.datetime.today() - datetime.timedelta(0)).strftime('%m/%d/%Y')

        sql_insert(df)

    except Exception as e:

        logger.debug(e)


def guestlist_csv(file,id):
    
    df = pd.read_csv(file, encoding='utf-8', header=None)
    df = df.fillna('')
    df = df.iloc[:, 32:38]

    df = df.rename(columns={32: 'ROOM', 33: 'TITLE', 34: 'NAME', 35: 'GUEST', 36: 'COMPANY NAME', 37:'GROUP'})

    df = df.rename(columns={'ROOM': 'room_number', 'TITLE': 'title', 'NAME': 'guest_name', 'GUEST': 'guest_status', 'COMPANY NAME': 'company', 'GROUP': 'group'})
    df['guest_name'] = df[['title', 'guest_name']].agg(' '.join, axis=1)
    df.pop('title')

    df['property_id'] = id[0]
    df['extraction_date'] = extraction_date

    sql_insert(df)
    
    
def inhouseguests(file,id):

    try:

        pdf = pdfplumber.open(file)

        page = pdf.pages

        for p in page:
            text = p.extract_text()

            data = text.splitlines()

            for index, d in enumerate(data):

                roomnum = re.findall(r'(^\d{3}\s+)', d)

                if roomnum:

                    d1 = data[index + 1].split()

                    if len(d1) == 1:
                        name = re.findall(r'(\w+\,\s+\w+)', d)[0]
                        guest_name = name
                    else:
                        try:
                            name = re.findall(r'(\w+\,)', d)[0]
                            guest_name = f'{name}{d1[-1]}'
                        except:

                            name = d1[-1]
                            gt11 = re.findall(r'\@[A-Z]', d)[0]
                            gt12 = d.split(gt11)
                            d = f'{gt12[0]}  {gt11} {name} {gt12[-1]}'

                    d2 = d.split(name)

                    d3 = d2[0].split()

                    roomnum = d3[0]
                    room_type = d3[1]
                    room_stat = d3[2]
                    gt = d3[3]

                    m4 = d2[-1]

                    dates = ' '.join(re.findall(r'(\d{2}\-\w{3}\-\d{2})', m4))

                    m41 = m4.split(dates)[0]

                    code1 = re.findall(r'[A-Z]\s{1}', m41)
                    if code1:
                        code = code1[0]
                        company = m41.split(code)[-1]
                    else:
                        code = ''
                        company = m41

                    if code:
                        code = code.strip()

                    if code == 'R':
                        code = 'Member'
                    elif code == 'G':
                        code = 'Gold Elite'
                    elif code == 'S':
                        code = 'Silver Elite'
                    elif code == 'P':
                        code = 'Platinum Elite'
                    elif code == 'T':
                        code = 'Titanium Elite'
                    elif code == 'U':
                        code = 'Ambassador Elite'

                    arrivaldate = dates.split(' ')[0]
                    deprtdate = dates.split(' ')[-1]
                    p1 = deprtdate + r'(.*?)\s+\d{1}'

                    city = re.findall(p1, m4, re.IGNORECASE)
                    if city:
                        city = city[0]
                    else:
                        city = ''

                    item = {
                        'room_number': roomnum,
                        'room_type': room_type,
                        'room_status': room_stat,
                        'gt': gt,
                        'guest_name': guest_name,
                        'mbv_level': code,
                        'company': company,
                        'arrival_date': arrivaldate,
                        'depart_date': deprtdate,
                        'city': city,
                        'property_id': id[0],
                        'extraction_date': extraction_date,
                    }

                    insert_guest_data(item)

    except Exception as e:
        logger.debug(e)
        

def remaining_arrivals(file , id):

    try:

        pdf = pdfplumber.open(file)

        page = pdf.pages

        for p in page:
            text = p.extract_text()

            data = text.split('MARKET ')[1]

            data = data.splitlines()

            for index, d in enumerate(data):

                if '/' in d:

                    name = re.findall(r'(.*?)\w{1}\s+\-\s+', d)
                    if name == []:
                        name = re.findall(r'(.*?)\s{3}\w{1}\s+\w{1}', d)

                    if name:
                        name = name[0]

                        tier = re.findall(r'(\w{1})(\s{1}\-\s+\d{9})', d)
                        if tier:
                            tier = tier[0][0]
                        else:
                            tier = ''

                        rt = re.findall(r'([A-Z]\s{1}[A-Z]\s{1}\d{3}\s+\w{3,})', d)
                        
                        if rt == []:
                            rt = re.findall(r'([A-Z]\s{1}[A-Z]\s{1}\w{3,})', d)
                        if rt:
                            room_type = rt[0].split(' ')[-1]

                        d1 = data[index + 1]

                        com = re.findall(r'(.*?)\d{8}', d1)
                        if com:
                            com = com[0]

                        price = re.findall(r'(\$\d+\.\d+)', d1)
                        if price:
                            price = price[0]

                        rate_plan = d1.split(' ')[-1]

                        try:
                            nights=re.findall(r'(\w{2}\s{1})(\s+\d{,2}\s+)(\d{1}\,\d{1})',d)
                            if nights==[]:
                                nights=re.findall(r'(\w{2})(\s+\d{,2}\s+)(\w{1}\s+)(\d{1}\,\d{1})',d)
                            if nights:
                                nights=int(nights[0][1].strip())

                            depart_date = (datetime.datetime.today() + datetime.timedelta(nights)).strftime('%m/%d/%Y')
                        except Exception as e:
                            depart_date=''

                        if tier:
                            tier=tier.strip()

                        if tier=='B':
                            tier='Blue'
                        elif tier=='S':
                            tier='Silver'
                        elif tier == 'G':
                            tier = 'Gold'
                        elif tier == 'D':
                            tier = 'Diamond'

                        item = {
                            'guest_name': name,
                            'hilton_honor_tier': tier,
                            'company': com,
                            'rate': price,
                            'depart_date':depart_date,
                            'rate_plan': rate_plan,
                            'arrival_date': extraction_date,
                            'room_type': room_type,
                            'property_id': id[0],
                            'extraction_date': extraction_date,
                        }

                        insert_data(item)

    except Exception as e:
        logger.debug(e)
