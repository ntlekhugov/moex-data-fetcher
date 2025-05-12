# MOEX Data Fetcher

A reusable Python module for fetching data from the Moscow Exchange (MOEX) Information & Statistical Server (ISS) API.

## Project Structure

```
MOEX data fetcher/
├── src/                  # Source code
│   ├── moex_api_client.py        # Core API client
│   ├── fetch_moex_iss_bond_indices.py  # Bond indices fetcher
│   └── moex_bond_data.py         # Russian bond data fetcher
├── examples/             # Example usage scripts
│   ├── example_moex_api_usage.py       # Basic API usage examples
│   ├── fetch_money_market_instruments.py  # Money market data fetcher
│   ├── fetch_rgbi_index.py              # Government bond index fetcher
│   ├── fetch_rucbhytr_index.py          # High-yield corporate bond index fetcher
│   ├── fetch_rucbitr_index.py           # Corporate bond index fetcher
│   └── fetch_corp_bonds_data.py         # Russian domestic bond data fetcher
├── data/                 # Directory for storing fetched data
├── tests/                # Test scripts (to be implemented)
└── docs/                 # Documentation (to be implemented)
```

## Core Features

- Fetch data from MOEX ISS API with proper error handling and pagination
- Support for various market segments (equities, bonds, indices, etc.)
- Organized data storage with proper timestamping
- Flexible configuration options

## Quick Start

1. Set up your environment:
   ```bash
   # Optional: Create a virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install pandas requests
   ```

2. Basic usage:
   ```python
   from src.moex_api_client import MOEXDataSource
   
   # Initialize the client
   moex = MOEXDataSource()
   
   # Fetch IMOEX (Moscow Exchange Index) data
   df = moex.get_historical_data(
       engine='stock',
       market='index',
       board='SNDX',
       security='IMOEX',
       from_date='2020-01-01'
   )
   
   print(df.head())
   ```

## Available Data Types

- **Equity Indices**: IMOEX, MOEXBC, etc.
- **Bond Indices**: RGBITR, RUCBITR, RUGBITR, etc.
- **Individual Securities**: Stocks, bonds, ETFs
- **Money Market Instruments**: MOEXREPO, etc.
- **Russian Domestic Bonds**: Detailed bond data including:
  - Instrument parameters
  - Daily trading results
  - Coupon payment schedules
  - Amortization schedules
  - Offer (put/call) information

## Authentication

Some API endpoints may require authentication:

```python
# With authentication
moex = MOEXDataSource(username='your_username', password='your_password')
```

## Data Integrity

This module follows strict data integrity principles:
- Fetches data directly from the official MOEX ISS API
- Preserves raw data exactly as retrieved
- Provides clear provenance through metadata

## Examples

See the `examples/` directory for specific use cases and data fetching scripts:

### General Market Data

- `example_moex_api_usage.py`: Basic API usage examples
- `fetch_money_market_instruments.py`: Money market data fetcher

### Bond Indices

- `fetch_rgbi_index.py`: Government bond index fetcher
- `fetch_rucbhytr_index.py`: High-yield corporate bond index fetcher
- `fetch_rucbitr_index.py`: Corporate bond index fetcher

### Russian Domestic Bonds

- `fetch_corp_bonds_data.py`: Fetches detailed data for Russian domestic bonds, including:
  - Instrument parameters
  - Daily trading results
  - Coupon payment schedules
  - Amortization schedules
  
  This example specifically targets bonds issued by Russian entities within Russia for the period from September 19, 2022, to April 4, 2025.

## License

This project is for personal use only. Commercial use requires explicit permission from the author.

## Acknowledgments

This module was extracted from the Russia Macro Analysis project, which analyzes the impact of CBR policy rates on Russian asset classes.