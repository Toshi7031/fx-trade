import db.db as db
import pandas as pd
import api.line_api as line_api
import datetime
import time
import logger
import db.table_defs as table_defs

conn = db.conn
time_format = db.time_format
logger = logger.get_logger('report')

def create_trade_report_state_table():
    sql = table_defs.get_create_table_sql(
        'trade_report_states', 'trade_report_states'
    )
    conn.execute(sql)

def update_trade_states():
    create_trade_report_state_table()

    table_name = 'trade_report_states'
    header = table_defs.get_columns(table_name)
    state_records = pd.read_sql_query(
        'select * from ' + table_name + ';'
        , conn
    ).reindex(columns=header)

    # tradesテーブルにあるデータから、すでにstatesに存在するものを取得
    exist_trades = pd.read_sql_query(
        'select tradeId, state from trades '
        + 'where exists ('
        + 'select * from ' + table_name + ' as states '
        + 'where trades.tradeId = states.trade_id '
        + ');'
        , conn
    )

    new_trades = pd.read_sql_query(
        'select tradeId, openTime, state from trades '
        + 'where not exists ('
        + 'select * from ' + table_name + ' as states '
        + 'where trades.tradeId = states.trade_id '
        + ');'
        , conn
    )

    # statesテーブルから取ったレコードとtradesテーブルから取ったレコードを結合
    merge_exist = pd.merge(state_records, exist_trades,
        left_on='trade_id', right_on='tradeId')

    # 既にstatesテーブルにあるtrade_stateの値を
    # tradeテーブルから取得したstateで上書き
    for i, row in merge_exist.iterrows():
        merge_exist.at[i, 'trade_state'] = row['state']

    # 結合したtradeの列を削除して代入
    state_records = merge_exist.drop(['tradeId', 'state'], axis=1)

    # statesテーブルにないtradeの行を追加する
    for i, row in new_trades.iterrows():
        new_record = pd.Series()
        new_record['trade_id'] = row['tradeId']
        new_record['open_time'] = row['openTime']
        new_record['trade_state'] = row['state']
        new_record['reported_state'] = ''
        # 行をappend
        state_records = state_records.append(new_record, ignore_index=True)

    # ソートして、DBに書き込み
    state_records.to_sql(table_name, conn, if_exists='replace', index=False)

    logger.debug('trade state updated')

def trade_report(test=False):
    update_trade_states()

    table_name = 'trade_report_states'
    header = table_defs.get_columns(table_name)
    state_records = pd.read_sql_query(
        'select * from ' + table_name + ' '
        + ' order by open_time;'
        , conn
    ).reindex(columns=header)

    unsent_records = state_records.query('trade_state != reported_state')

    for i, row in unsent_records.iterrows():
        # trade_idが一致するレコードをtradesテーブルから取得
        trade = pd.read_sql_query(
            'select * from trades where tradeId = {};'.format(row['trade_id'])
            , conn
        )
        if len(trade) > 0:
            # tradeのSeriesを代入
            # trade_idが一致するレコードは1行だけのはずなのでdfの1番目を取る
            trade = trade.iloc[0]
        else:
            # trade_idが一致するレコードが無ければcontinue
            continue

        # エントリー時のLINEを投稿
        if row['trade_state'] == 'OPEN':
            action = 'entry'
            instrument = trade['instrument'].replace('_', '/')
            start_side = '買い' if int(trade['initialUnits']) > 0 else '売り'
            start_price = format(float(trade['price']), '.3f')
            kunits = format(abs(trade['initialUnits'])/1000, '.1f')
            info = "【エントリー】\n"\
                + start_side + " " + instrument + "@" + start_price\
                + " ×" + kunits + "kUnits"
            # report
            tags = "#USDJPY"
            content = {
                "message": 
                + info + "\n"
                + tags
            }
            if test:
                print(content)
            else:
                line_api.report(content)
            # reported_state更新
            state_records.at[i, 'reported_state'] = 'OPEN'

        # イグジット時のツイートを投稿
        if row['trade_state'] == 'CLOSED':
            instrument = trade['instrument'].replace('_', '/')
            start_side = '買い' if int(trade['initialUnits']) > 0 else '売り'
            start_price = format(float(trade['price']), '.3f')
            kunits = format(abs(trade['initialUnits'])/1000, '.1f')
            end_side = '買い' if start_side == '売り' else '売り'
            end_price = format(float(trade['averageClosePrice']), '.3f')
            pips = format(trade['realizedPL']/abs(trade['initialUnits'])*100, '.1f')
            money = format(trade['realizedPL'], '.1f')
            plus = "+" if trade['realizedPL'] > 0 else ""

            action = 'take_profit' if trade['realizedPL'] > 0 else 'losscut'
            info = "【トレード終了】\n"\
                + start_side + " " + instrument + "@" + start_price\
                + " ×" + kunits + "kUnits\n"\
                + end_side + " " + instrument + "@" + end_price\
                + " ×" + kunits + "kUnits\n"\
                + plus + money + "円(" + plus + pips + " pips)"
            # report
            tags = "#USDJPY"
            content = {
                "message": 
                + info + "\n"
                + tags
            }
            if test:
                print(content)
            else:
                line_api.report(content)
            # reported_state更新
            state_records.at[i, 'reported_state'] = 'CLOSED'

        # 高速連投を避けるためのsleep
        time.sleep(5)

    # DBに書き込み
    state_records.to_sql(table_name, conn, if_exists='replace', index=False)

def clear_pending_report():
    table_name = 'trade_report_states'
    conn.execute(
        'update ' + table_name + ' '
        + 'set reported_state = trade_state;'
    )
    conn.commit()

def delete_old_records():
    table_name = 'trade_report_states'
    keep_span = datetime.timedelta(weeks=1)
    keep_from = (datetime.datetime.now(datetime.timezone.utc)
        - keep_span).strftime(time_format)
    conn.execute(
        'delete from ' + table_name + ' where open_time < '
        + '\'' + keep_from + '\' ;'
    )
    conn.commit()

def pl_report(test=False):
    # 日付を日曜日にするために引く日数
    days_shift = datetime.datetime.now(datetime.timezone.utc).weekday() + 1
    # 今日からdays_shiftを引いた日付
    start_date = (datetime.datetime.now(datetime.timezone.utc)\
        - datetime.timedelta(days=days_shift)).strftime('%Y-%m-%d')
    # 時間
    start_time = '23:00'
    start_datetime = start_date + ' ' + start_time
    trades = pd.read_sql_query(
        'select * from trades '
        + 'where openTime > \'' + start_datetime + '\' '
        + 'and state = \'CLOSED\';'
        , conn
    )

    # tradesのレコードが無ければreturn
    if len(trades) < 1:
        return

    pips_total = 0
    money_total = 0

    for i, row in trades.iterrows():
        pips = row['realizedPL']/abs(row['initialUnits'])*100
        pips_total += pips
        money_total += float(row['realizedPL'])

    plus = "+" if pips_total > 0 else ""

    info = "【今週の損益発表】\n"\
        + "今週の損益は\n"\
        + plus + str(round(money_total, 1)) + "円("\
        + plus + str(format(pips_total, '.1f')) + "pips)\n"\
        + "でした"
    tags = "#USDJPY"

    content = info + "\n"\
        + tags

    if test:
        print(content)
    else:
        line_api.report(content)
