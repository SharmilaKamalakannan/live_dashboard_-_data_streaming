# live_dashboard_-_data_streaming
# Streaming Stock Ticker Data Analysis

This project demonstrates a real-time data analysis pipeline for stock ticker data using Pathway, Bokeh, and Panel. The pipeline performs the following tasks:

1. Reads stock ticker data from a Kafka topic.
2. Computes 20-minute sliding window Bollinger Bands and 1-minute VWAP (Volume-Weighted Average Price).
3. Identifies trading signals based on VWAP crossing the Bollinger Bands.
4. Sends Slack alerts for potential buy/sell signals.
5. Visualizes the data with a Bokeh plot and a table of alerts.

## Components

### `dashboard.py`

This script sets up the data processing pipeline, visualization, and Slack alerting system. It consists of the following steps:

1. Define the data schema for the stock ticker data.
2. Read data from the Kafka topic `ticker`.
3. Compute 20-minute Bollinger Bands and 1-minute VWAP using Pathway windowing operations.
4. Join the two data streams and identify trading signals.
5. Send Slack alerts for potential buy/sell signals.
6. Visualize the data with a Bokeh plot and a table of alerts using Panel.

### `kafka-data-streamer.py`

This script simulates a data stream by reading a CSV file (`ticker.csv`) and writing the data to a Kafka topic (`ticker`). It uses the Pathway library to define the data schema and replay the CSV data at a specified rate.

## Usage

1. Start a Kafka cluster and ensure it is accessible at `kafka:9092`.
2. Set the `slack_alert_channel_id` and `slack_alert_token` variables in `dashboard.py` with your Slack channel ID and token, respectively.
3. Run `kafka-data-streamer.py` to start streaming data to the Kafka topic.
4. Run `dashboard.py` to start the data analysis pipeline, visualization, and Slack alerting system.
5. Access the visualization at `http://localhost:8080`.

## Dependencies

- Pathway
- Bokeh
- Panel
- Kafka
- requests

Make sure to install the required dependencies before running the scripts.

![image](https://github.com/SharmilaKamalakannan/live_dashboard_-_data_streaming/assets/110477268/5d224c3f-f8b4-4b3a-b010-1f7665f809f2)

![Screenshot 2024-03-29 214358](https://github.com/SharmilaKamalakannan/live_dashboard_-_data_streaming/assets/110477268/de444a35-0e76-44c5-93c1-4f6f25c1c484)
