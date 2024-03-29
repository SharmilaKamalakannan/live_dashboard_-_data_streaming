import datetime
import bokeh.models
import bokeh.plotting
import panel as pn
import requests
import pathway as pw

rdkafka_consumer_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": "kafka-group-0",
    "auto.offset.reset": "earliest",
}

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

data = pw.io.kafka.read(
    rdkafka_consumer_settings, topic="ticker", format="json", schema=DataSchema
)

data = data.with_columns(t=data.t.dt.utc_from_timestamp(unit="ms"))
minute_20_stats = (
    data.windowby(
        pw.this.t,
        window=pw.temporal.sliding(
            hop=datetime.timedelta(minutes=1), duration=datetime.timedelta(minutes=20)
        ),
        behavior=pw.temporal.exactly_once_behavior(),
        instance=pw.this.ticker,
    )
    .reduce(
        ticker=pw.this._pw_instance,
        t=pw.this._pw_window_end,
        volume=pw.reducers.sum(pw.this.volume),
        transact_total=pw.reducers.sum(pw.this.volume * pw.this.vwap),
        transact_total2=pw.reducers.sum(pw.this.volume * pw.this.vwap**2),
    )
    .with_columns(vwap=pw.this.transact_total / pw.this.volume)
    .with_columns(
        vwstd=(pw.this.transact_total2 / pw.this.volume - pw.this.vwap**2) ** 0.5
    )
    .with_columns(
        bollinger_upper=pw.this.vwap + 2 * pw.this.vwstd,
        bollinger_lower=pw.this.vwap - 2 * pw.this.vwstd,
    )
)
minute_1_stats = (
    data.windowby(
        pw.this.t,
        window=pw.temporal.tumbling(datetime.timedelta(minutes=1)),
        behavior=pw.temporal.exactly_once_behavior(),
        instance=pw.this.ticker,
    )
    .reduce(
        ticker=pw.this._pw_instance,
        t=pw.this._pw_window_end,
        volume=pw.reducers.sum(pw.this.volume),
        transact_total=pw.reducers.sum(pw.this.volume * pw.this.vwap),
    )
    .with_columns(vwap=pw.this.transact_total / pw.this.volume)
)

joint_stats = (
    minute_1_stats.join(
        minute_20_stats, pw.left.t == pw.right.t, pw.left.ticker == pw.right.ticker
    )
    .select(
        *pw.left,
        bollinger_lower=pw.right.bollinger_lower,
        bollinger_upper=pw.right.bollinger_upper,
    )
    .with_columns(
        is_alert=(
            (pw.this.volume > 10000)
            & (
                (pw.this.vwap > pw.this.bollinger_upper)
                | (pw.this.vwap < pw.this.bollinger_lower)
            )
        )
    )
    .with_columns(
        action=pw.if_else(
            pw.this.is_alert,
            pw.if_else(pw.this.vwap > pw.this.bollinger_upper, "sell", "buy"),
            "hodl",
        )
    )
)
alerts = joint_stats.filter(pw.this.is_alert).select(
    pw.this.ticker, pw.this.t, pw.this.vwap, pw.this.action
)

def stats_plotter(src):
    actions = ["buy", "sell", "hodl"]
    color_map = bokeh.models.CategoricalColorMapper(
        factors=actions, palette=("#00ff00", "#ff0000", "#00000000")
    )

    fig = bokeh.plotting.figure(
        height=400,
        width=600,
        title="20 minutes Bollinger bands with last 1 minute average",
        x_axis_type="datetime",
    )

    fig.line("t", "vwap", source=src)

    fig.line("t", "bollinger_lower", source=src, line_alpha=0.3)
    fig.line("t", "bollinger_upper", source=src, line_alpha=0.3)
    fig.varea(
        x="t",
        y1="bollinger_lower",
        y2="bollinger_upper",
        fill_alpha=0.3,
        fill_color="gray",
        source=src,
    )

    fig.scatter(
        "t",
        "vwap",
        size=10,
        marker="circle",
        color={"field": "action", "transform": color_map},
        source=src,
    )

    return fig

viz = pn.Row(
    joint_stats.plot(stats_plotter, sorting_col="t"),
    alerts.show(include_id=False, sorters=[{"field": "t", "dir": "desc"}]),
)

slack_alert_channel_id = "SLACK_CHANNEL_ID"
slack_alert_token = "SLACK_TOKEN"


def send_slack_alert(key, row, time, is_addition):
    if not is_addition:
        return
    alert_message = f'Please {row["action"]} {row["ticker"]}'
    print(f'Sending alert "{alert_message}"')
    requests.post(
        "https://slack.com/api/chat.postMessage",
        data="text={}&channel={}".format(alert_message, slack_alert_channel_id),
        headers={
            "Authorization": "Bearer {}".format(slack_alert_token),
            "Content-Type": "application/x-www-form-urlencoded",
        },
    ).raise_for_status()


pw.io.subscribe(alerts, send_slack_alert)

viz_thread = viz.show(threaded=True, port=8080)

try:
    pw.run(monitoring_level=pw.MonitoringLevel.ALL)
finally:
    viz_thread.stop()
