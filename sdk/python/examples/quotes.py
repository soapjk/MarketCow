from marketcow_sdk import MarketCowClient

with MarketCowClient("http://127.0.0.1:8790") as client:
    result = client.quotes.get(
        ["AAPL.XNAS", "700.XHKG", "600519.XSHG"], refresh=False
    )
    for item in result.items:
        print(item.instrument_id, item.price, item.source, item.stale)
    for error in result.errors:
        print("unavailable", error)
