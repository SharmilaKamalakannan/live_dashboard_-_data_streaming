import pathway as pw
fname = "ticker.csv"
schema = pw.schema_from_csv(fname)
print(schema.generate_class(class_name="DataSchema"))
class DataSchema(pw.Schema):
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    t: int
    transactions: int
    otc: str

data = pw.demo.replay_csv(fname, schema=DataSchema, input_rate=1000)

rdkafka_producer_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
}

pw.io.kafka.write(data, rdkafka_producer_settings, topic_name="ticker")
pw.run()