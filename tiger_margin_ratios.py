from tigeropen.quote.quote_client import QuoteClient
from tigeropen.tiger_open_config import TigerOpenClientConfig
client_config = TigerOpenClientConfig(props_path='/path/to/your/properties/file/')

quote_client = QuoteClient(client_config)

symbol_names = quote_client.get_symbol_names(market=Market.ALL)
print(symbol_names)