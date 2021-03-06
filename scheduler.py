import schedule
import time
import datetime
import trader
import report
import recorder
import db.db as db
import api.oanda_api as oanda_api
import logger
import traceback

trader = trader.Trader()
logger = logger.get_logger('scheduler')
exception_count = 0
MAX_RETRY = 20

def trader_loop():
    trader.loop()

def update_trade_data():
    recorder.update_trade_data('trades')

def update_price_data():
    recorder.update_price_data()

def report_loop():
    report.trade_report()

def delete_old_records():
    recorder.delete_old_trade_data()
    report.delete_old_records()

def sleep_trader():
    trader.is_sleeping = True

def wakeup_trader():
    trader.is_sleeping = False

def deactivate_if_market_closed():
    if not oanda_api.is_market_open():
        schedule.clear('fx')

def activate():
    # 最初にfxタグのスケジュールをクリアする
    schedule.clear('fx')
    # fxタグのスケジュールを登録
    schedule.every(5).to(10).seconds.do(trader_loop).tag('fx')
    schedule.every(30).seconds.do(update_trade_data).tag('fx')
    schedule.every(5).to(10).seconds.do(update_price_data).tag('fx')
    schedule.every(60).seconds.do(report_loop).tag('fx')
    schedule.every(2).hours.do(deactivate_if_market_closed).tag('fx')

def deactivate():
    trader.exit()
    schedule.clear('fx')

def pl_report():
    report.pl_report()

def is_now_sleeptime():
    now = datetime.datetime.now(datetime.timezone.utc).time()
    start = datetime.time(hour=21, minute=30)
    end = datetime.time.max
    if start < now and now < end:
        return True

    start = datetime.time.min
    end = datetime.time(hour=8, minute=0)
    if start < now and now < end:
        return True

    return False

# このファイル最初の実行時にprice data更新とactivateを実行
recorder.update_price_data()
activate()

# sleep時間帯だったらsleep
if is_now_sleeptime():
    sleep_trader()

# 毎日04:00UTC(13:00JST)に古いレコード削除
schedule.every().day.at('04:00').do(delete_old_records)

# 日〜木23:00UTC(月〜金08:00JST)にactivateを実行
schedule.every().sunday.at('23:00').do(activate)
schedule.every().monday.at('23:00').do(activate)
schedule.every().tuesday.at('23:00').do(activate)
schedule.every().wednesday.at('23:00').do(activate)
schedule.every().thursday.at('23:00').do(activate)

# 毎日21:30-08:00UTC(06:30-17:00JST)はsleep
schedule.every().day.at('21:30').do(sleep_trader)
schedule.every().day.at('08:00').do(wakeup_trader)

# 金曜21:00UTC(土曜06:00JST)にdeactivateを実行
schedule.every().friday.at('21:00').do(deactivate)

# 土曜00:00UTC(土曜09:00JST)に損益報告
schedule.every().saturday.at('00:00').do(pl_report)

while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        logger.debug(traceback.format_exc())
        schedule.clear('fx')
        exception_count += 1
        if exception_count < MAX_RETRY:
            activate()
            continue
        else:
            logger.debug('too much exception')
            # しばらく待ってから再起動
            time.sleep(1200)
            exception_count = 0
            activate()
            continue
