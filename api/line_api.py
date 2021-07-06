import configparser
import requests
import db.db as db
import time
import logger

config = configparser.ConfigParser()
config.read('api/line_conf.ini')
TOKEN = config['DEFAULT']['access_token']
logger = logger.get_logger('report')

def report(content):
    url = 'https://notify-api.line.me/api/notify'
    headers = {'Authorization': f'Bearer {TOKEN}'}
    logger.debug('content: ' + content)

    done = False
    retry = 0
    max_retry = 2
    while (not done) and (retry <= max_retry):
        try:
            response = requests.post(url,headers=headers,data=content) 
            if response.status != 200:
                raise Exception('failed')
            else:
                logger.debug('scceeded')
                done = True
        except Exception as e:
            logger.debug(str(e))
            # 高速連投を避けるためのsleep
            time.sleep(5)
            continue
        finally:
            retry +=1

    if not done:
        raise Exception('failed')
