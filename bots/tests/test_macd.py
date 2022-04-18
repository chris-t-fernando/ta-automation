from bots import macd_bot

fake_buy = {
    "type": "buy",
    "symbol": "XRP",
    "broker": "swyftx",
    "interval": "5m",
    "timestamp": "2022-04-FAKEFAKEFAKE",
    "signal_strength": None,
    "macd_value": -0.05596663025085036,
    "signal_value": -0.1,  # bigger signal gap
    "macd_signal_gap": -0.04403337,
    "histogram_value": 0.005450574852315156,
    "sma_value": 51.72239999771118,
    "sma_recent": 48,
    "sma_gap": 3.722399998,  # steeper climb
    "stop_loss_price": 51.975,
    "last_price": 51.63999938964844,
    "current_cycle_duration": 11,
    "target_price": 52,
    "unit_risk": 0.15,
}
