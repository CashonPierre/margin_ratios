from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import time

class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    # This callback receives the margin data
    def openOrder(self, orderId, contract, order, orderState):
        print(f"--- WHAT-IF MARGIN IMPACT ---")
        print(f"Init Margin Change: {orderState.initMarginChange}")
        print(f"Maint Margin Change: {orderState.maintMarginChange}")
        print(f"Equity With Loan: {orderState.equityWithLoanAfter}")
        print(f"Commission: {orderState.commission} {orderState.commissionCurrency}")
        # Disconnect after getting the data if you wish
        self.disconnect()

    def nextValidId(self, orderId):
        # 1. Define Contract
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        # 2. Define Order
        order = Order()
        order.action = "BUY"
        order.totalQuantity = 100
        order.orderType = "MKT"
        order.whatIf = True 
        
        # --- FIX FOR ERROR 10268 ---
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        # ---------------------------

        print(f"Sending What-If order for {contract.symbol}...")
        self.placeOrder(orderId, contract, order)
        
def run_loop():
    app.run()

app = TestApp()
app.connect("127.0.0.1", 7497, clientId=1) # 7497 is for Paper Trading

# Start the socket in a thread
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

time.sleep(1) # Wait for connection
# The logic starts in nextValidId once connected