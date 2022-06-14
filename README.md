# pycoinglass

## Requirements

- python > 3.9.0

## Install

```bash
pip install -e .
```

## How to use

```python

from pycoinglass import API

api = API("YOUR_API_KEY")

# You can set API with an environment variable "COINGLASS_API_KEY" 
# api = API()

# TOP table at https://www.coinglass.com/
api.margin_market_capture("BTC")

```