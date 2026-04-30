"""Tests for cocapn-watch."""
import pytest
import tempfile
import os
from cocapn_plato.watch import Watchdog, WatchConfig


def test_watchdog_detects_down():
    config = WatchConfig(
        services=[
            {"name": "up", "host": "httpbin.org", "port": 80, "path": "/get"},
            {"name": "down", "host": "127.0.0.1", "port": 59999, "path": "/"},
        ],
        interval=1,
        alert_on_down=True,
        alert_on_recover=True,
    )
    dog = Watchdog(config)
    alerts = dog.check()
    
    # Should alert for the down service
    down_alerts = [a for a in alerts if a["event"] == "down"]
    assert len(down_alerts) == 1
    assert down_alerts[0]["service"] == "down"
    
    # Should not alert for the up service
    assert not any(a["service"] == "up" for a in alerts)


def test_watchdog_consecutive_failures():
    config = WatchConfig(
        services=[
            {"name": "down", "host": "127.0.0.1", "port": 59999, "path": "/"},
        ],
        interval=1,
        consecutive_failures=2,
    )
    dog = Watchdog(config)
    
    # First check: 1 failure, no alert
    alerts1 = dog.check()
    assert len(alerts1) == 0
    assert dog.state["down"]["failures"] == 1
    
    # Second check: 2 failures, alert
    alerts2 = dog.check()
    assert len(alerts2) == 1
    assert alerts2[0]["event"] == "down"


def test_watchdog_recovery():
    config = WatchConfig(
        services=[
            {"name": "svc", "host": "127.0.0.1", "port": 59999, "path": "/"},
        ],
        interval=1,
        alert_on_recover=True,
    )
    dog = Watchdog(config)
    
    # Mark as down
    dog.check()
    assert dog.state["svc"]["ok"] == False
    
    # Now change to a real up service
    dog.config.services[0] = {"name": "svc", "host": "httpbin.org", "port": 80, "path": "/get"}
    alerts = dog.check()
    
    recover_alerts = [a for a in alerts if a["event"] == "recovered"]
    assert len(recover_alerts) == 1
    assert recover_alerts[0]["service"] == "svc"


def test_watchdog_log_file():
    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".log") as f:
        log_path = f.name
    try:
        config = WatchConfig(
            services=[
                {"name": "down", "host": "127.0.0.1", "port": 59999, "path": "/"},
            ],
            interval=1,
            log_file=log_path,
            consecutive_failures=2,
        )
        dog = Watchdog(config)
        dog.check()  # First check: 1 failure, no alert
        alerts = dog.check()  # Second check: 2 failures, alert
        dog.send_alerts(alerts)
        
        with open(log_path) as f:
            content = f.read()
        assert "down" in content
        assert "DOWN" in content.upper() or "down" in content
    finally:
        os.unlink(log_path)
