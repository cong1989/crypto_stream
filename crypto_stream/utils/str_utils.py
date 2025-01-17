def parse_topic(topic: str) -> tuple[str, str]:
    """
    Parse exchange and data type from Kafka topic name
    Args:
        topic: Topic name e.g. 'crypto-ticks-binance-futures-quote'
    Returns:
        Tuple of (exchange, data_type)
    """
    # Remove prefix 'crypto-ticks-'
    without_prefix = topic.replace("crypto-ticks-", "")

    # Split remaining string by '-'
    parts = without_prefix.split("-")

    # Last part is the data type
    data_type = parts[-1]

    # Everything before the data type is the exchange
    exchange = "-".join(parts[:-1])

    return exchange, data_type


if __name__ == "__main__":
    topics = [
        "crypto-ticks-binance-futures-quote",
        "crypto-ticks-binance-quote",
        "crypto-ticks-bitmex-quote",
        "crypto-ticks-bitmex-trade",
    ]

    for topic in topics:
        exchange, data_type = parse_topic(topic)
        print(f"Topic: {topic}")
        print(f"  Exchange: {exchange}")
        print(f"  Data Type: {data_type}")
