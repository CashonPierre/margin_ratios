from ibapi.client import EClient
from ibapi.wrapper import EWrapper

class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def nextValidId(self, orderId):
        print(f"Connected! Next valid Order ID is: {orderId}")
        self.disconnect()

app = TestApp()
app.connect("127.0.0.1", 7497, clientId=1) # Ensure port matches TWS
app.run()