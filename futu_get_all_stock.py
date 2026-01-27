from futu import *
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
ret, data = quote_ctx.get_stock_basicinfo(Market.US, SecurityType.WARRANT)
# StockType可以改成SecurityType.ETF, SecurityType.IDX, SecurityType.PLATE, SecurityType.PLATESET, SecurityType.STOCK, SecurityType.WARRANT 等查看不同类型的证券
if ret == RET_OK:
    print('OK', len(data))
    data.to_csv('futu_us_warrant_basic_info.csv', index=False, encoding='utf-8-sig')
else:
    print('error:', data)
print('******************************************')
quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
