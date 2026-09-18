from marketcow_sdk import MarketCowClient

with MarketCowClient("http://127.0.0.1:8790") as client:
    health = client.health.check()
    print(health.model_dump(mode="json"))
