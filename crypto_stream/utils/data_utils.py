import pandas as pd


def format_quote_data(data):
    """Format the tick data into a structured dictionary"""
    return {
        "market_data": {
            "symbol": data.get("symbol"),
            "exchange": data.get("exchange"),
            "type": data.get("type"),
            "name": data.get("name"),
            "depth": data.get("depth"),
            "interval": data.get("interval"),
        },
        "pricing": {
            "best_bid": {
                "price": data["bids"][0]["price"] if data.get("bids") else None,
                "amount": data["bids"][0]["amount"] if data.get("bids") else None,
            },
            "best_ask": {
                "price": data["asks"][0]["price"] if data.get("asks") else None,
                "amount": data["asks"][0]["amount"] if data.get("asks") else None,
            },
        },
        "timestamps": {
            "event_time": data.get("timestamp"),
            "local_time": data.get("localTimestamp"),
            "receive_time": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%S.%f")[
                :-3
            ]
            + "Z",
        },
    }


def prepare_storage_quote_data(tick_data, spreads):
    """Prepare flattened quote data for storage"""
    return {
        "timestamp": tick_data["timestamps"]["event_time"],
        "local_timestamp": tick_data["timestamps"]["local_time"],
        "receive_timestamp": tick_data["timestamps"]["receive_time"],
        "bid_price": tick_data["pricing"]["best_bid"]["price"],
        "bid_size": tick_data["pricing"]["best_bid"]["amount"],
        "ask_price": tick_data["pricing"]["best_ask"]["price"],
        "ask_size": tick_data["pricing"]["best_ask"]["amount"],
    }


def prepare_storage_quote_sampling_data(tick_data, spreads):
    """Prepare flattened data for storage with forward sampling timestamp"""
    try:
        event_time = pd.Timestamp(tick_data["timestamps"]["event_time"])
        sampling_timestamp = (event_time.ceil("min")).strftime("%Y-%m-%dT%H:%M:00.000Z")

        return {
            "timestamp": tick_data["timestamps"]["event_time"],
            "local_timestamp": tick_data["timestamps"]["local_time"],
            "receive_timestamp": tick_data["timestamps"]["receive_time"],
            "sampling_timestamp": sampling_timestamp,
            "symbol": tick_data["market_data"]["symbol"],
            "exchange": tick_data["market_data"]["exchange"],
            "type": tick_data["market_data"]["type"],
            "bid_price": tick_data["pricing"]["best_bid"]["price"],
            "bid_size": tick_data["pricing"]["best_bid"]["amount"],
            "ask_price": tick_data["pricing"]["best_ask"]["price"],
            "ask_size": tick_data["pricing"]["best_ask"]["amount"],
            "spread": spreads.get("spread") if spreads else None,
            "spread_bps": spreads.get("spread_bps") if spreads else None,
            "mid_price": spreads.get("mid_price") if spreads else None,
        }
    except Exception as e:
        print(f"Error preparing storage data: {e}")
        raise


def calculate_quote_spreads(tick_data):
    """Calculate market spreads and mid price"""
    best_bid = tick_data["pricing"]["best_bid"]["price"]
    best_ask = tick_data["pricing"]["best_ask"]["price"]

    if best_bid and best_ask:
        spread = best_ask - best_bid
        spread_bps = (spread / best_bid) * 10000
        mid_price = (best_bid + best_ask) / 2

        return {"spread": spread, "spread_bps": spread_bps, "mid_price": mid_price}
    return None
