from futu import *
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

class OrderBookTest(OrderBookHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret_code, data = super(OrderBookTest,self).on_recv_rsp(rsp_pb)
        if ret_code != RET_OK:
            print("OrderBookTest: error, msg: %s" % data)
            return RET_ERROR, data
        print("OrderBookTest ", data) # OrderBookTest 自己的处理逻辑
        return RET_OK, data

handler = OrderBookTest()
quote_ctx.set_handler(handler)  # 设置实时摆盘回调
quote_ctx.subscribe(['US.AAPL'], [SubType.ORDER_BOOK])  # 订阅买卖摆盘类型，OpenD 开始持续收到服务器的推送
time.sleep(15)  #  设置脚本接收 OpenD 的推送持续时间为15秒


ret, data = quote_ctx.get_market_snapshot(['HK.00700', 'US.AAPL'])
if ret == RET_OK:
    print(data)
    print(data['code'][0])    # 取第一条的股票代码
    print(data['code'].values.tolist())   # 转为 list
else:
    print('error:', data)
quote_ctx.close() # 结束后记得关闭当条连接，防止连接条数用尽


