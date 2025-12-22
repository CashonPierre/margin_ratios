from futu import *
trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
ret, data = trd_ctx.get_margin_ratio(code_list=['US.xx','US.TSLA'])  
if ret == RET_OK:
    print(data)
    print(data['is_long_permit'][0])  # 取第一条的是否允许融资
    print(data['im_short_ratio'].values.tolist())  # 转为 list
else:
    print('error:', data)
trd_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
