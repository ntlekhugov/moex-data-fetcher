# MOEX Data Fetcher

A reusable Python module for fetching data from the Moscow Exchange (MOEX) Information & Statistical Server (ISS) API.

## Project Structure

```
MOEX data fetcher/
├── src/                  # Source code
│   ├── moex_api_client.py        # Core API client
│   └── fetch_moex_iss_bond_indices.py  # Bond indices fetcher
├── examples/             # Example usage scripts
│   ├── example_moex_api_usage.py       # Basic API usage examples
│   ├── fetch_money_market_instruments.py  # Money market data fetcher
│   ├── fetch_rgbi_index.py              # Government bond index fetcher
│   ├── fetch_rucbhytr_index.py          # High-yield corporate bond index fetcher
│   └── fetch_rucbitr_index.py           # Corporate bond index fetcher
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

See the `examples/` directory for specific use cases and data fetching scripts.

## License

**IMPORTANT**: This project is available for **personal use only**. Commercial use is prohibited without explicit written permission. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

This module was extracted from the Russia Macro Analysis project, which analyzes the impact of CBR policy rates on Russian asset classes.