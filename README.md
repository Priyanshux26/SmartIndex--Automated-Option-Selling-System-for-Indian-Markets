
# Project: SmartIndex: Automated Option Selling System for Indian Markets

**Brief:** This repository contains algorithmic trading scripts, utilities and historical-data tools focused on Indian markets. It includes broker utilities (Zerodha integrations), historical data collection helpers, and example short-straddle strategies and risk-management utilities.

---

## Table of Contents
- **Overview**
- **Features**
- **Getting Started**
  - Requirements
  - Installation
- **Configuration**
- **Usage Examples**
  - Broker utilities
  - Historical data collection
  - Short-straddle strategies
- **Development & Contributing**
- **License**
- **Contact / Support**

---

## Overview ✨
This repo provides tools and example strategies for algorithmic trading in India:
- Broker utilities and Zerodha Kite integrations for login, LTP subscription and MTM monitoring.
- Scripts to download and manage historical market data (Shoonya, Finvasia, Zerodha).
- Short-straddle strategy implementations (BankNifty, Nifty50, Finnifty, Sensex) with variations for stop-loss, MTM-based targets, and trailing stops.

---

## Features ✅
- Zerodha login automation and LTP subscription for live market data.
- MTM-based auto square-off and risk-management settings.
- Historical data download and aggregation utilities.
- Multiple short-straddle strategy variants and account-level risk controls.

---

## Getting Started 🔧

### Requirements
- Python 3.10+ recommended
- Review `requirements.txt` for dependencies:

```bash
pip install -r requirements.txt
```

### Installation
1. Clone the repo:

```bash
git clone https://github.com/<your-org>/algo_trading_strategies_india.git
cd algo_trading_strategies_india-main
```

2. Create a virtual environment and install:

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
```

---

## Configuration ⚙️
Most broker utilities use YAML or environment variables for credentials.

Example `broker-utilities/mtm_square_off_zerodha/config.yaml` (sensitive values should be kept out of source control):

```yaml
zerodha:
  api_key: "YOUR_API_KEY"
  api_secret: "YOUR_API_SECRET"

trading:
  mtm_loss_threshold: -20000
  daily_max_profit_target: 2000

risk_management:
  auto_square_off_enabled: true
  max_order_retries: 2
  order_retry_delay: 3
```

> **Note:** Store credentials securely (env vars or secrets manager). Never commit API secrets.

---

## Usage Examples 🚀

### Broker utilities (Zerodha)
- Automated login:

```bash
python broker-utilities/zerodha-kite-connect-auto-login.py
```
- MTM monitor (uses `config.yaml`):

```bash
python broker-utilities/mtm_square_off_zerodha/zerodha_mtm_monitor.py --config broker-utilities/mtm_square_off_zerodha/config.yaml
```
- LTP subscriber / position tracking:

```bash
python broker-utilities/mtm_square_off_zerodha/zerodha_ltp_subscriber.py
```

### Historical data collection
- Shoonya / Finvasia downloader:

```bash
python historical-data-collection/shoonya-finvasia/stock_historical_data.py --symbol RELIANCE --from 2024-01-01 --to 2025-01-01
```
- Zerodha historical helper:

```bash
python historical-data-collection/zerodha-kite-api/zerodha_kite_historical_data.py --symbol NIFTY --interval 5minute
```

### Short-straddle strategies
- BankNifty example:

```bash
python short-straddle/0920_short_straddle/banknifty_0920_short_straddle.py
```
- Adjusted variants (trailing stop, percentage stops, MTM targets) are in the respective subfolders.

> **Tip:** Check each script's top-level docstring or `--help` to learn available CLI options.

---

## Development & Contributing 🛠️
- Follow the repo structure and add tests for new strategies.
- Open issues for bugs or feature requests.
- Make a feature branch, add tests, and submit a pull request with a clear description.

Contributing checklist:
- Add/update tests
- Document usage in README or script docstrings
- Keep secrets out of commits

---

## Testing
- Add and run unit tests where possible.
- Simulate order placements on a paper trading account before using real capital.
- Validate config settings and risk parameters in a controlled environment.

---

## License & Attribution 📜
If you want an open-source license, consider adding a `LICENSE` (e.g., MIT). If you want, I can add a license file for you.

---

## Contact / Support
- For questions, open an issue in this repo.
- Include logs, config (with sensitive fields redacted), and steps to reproduce when reporting issues.

```
