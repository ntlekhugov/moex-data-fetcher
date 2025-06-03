"""Constants for MOEX Data Fetcher."""

# MOEX ISS base URLs
BASE_URL = "http://iss.moex.com/iss"
HISTORY_URL = "http://iss.moex.com/iss/history"

# Engines
ENGINE_STOCK = "stock"
ENGINE_CURRENCY = "currency"
ENGINE_FUTURES = "futures"
ENGINE_COMMODITY = "commodity"

# Markets
MARKET_INDEX = "index"
MARKET_SHARES = "shares"
MARKET_BONDS = "bonds"
MARKET_REPO = "repo"

# Boards
BOARD_SNDX = "SNDX"  # Main indices board
BOARD_RTSI = "RTSI"  # RTS indices board
BOARD_TQBR = "TQBR"  # Main shares board
BOARD_TQCB = "TQCB"  # Main corporate bonds board
BOARD_TQOB = "TQOB"  # Main government bonds board
