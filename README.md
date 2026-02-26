# Binance USDM Futures Low-Latency Market Data Connector (C++)

This connector maintains parallel websocket sessions and emits a **generic** market data payload.

- low-overhead JSON decode path using `Boost.JSON` parser reuse (thread-local parser) for reduced per-message allocations

## What the callback receives

`binance::futures::MarketData` is market-agnostic and always carries:

- `venue` and `instrument`
- event timestamp + sequence
- a **constructed top-5 order book snapshot** (`bids` / `asks`) maintained from streaming updates
- optional `last_trade` when the event is trade-driven

The order book is continuously updated from `depth@0ms` deltas (and optionally bookTicker updates) and emitted as a constructed snapshot for every callback.

## Subscriptions

Config supports enabling:

- `bookTicker`
- `depth@0ms`
- `trade`

and setting symbol list, parallel connection count, and IO thread count.

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

## Run demo

```bash
./build/binance_connector_demo
```

## Main APIs

- `include/market_data.hpp`: generic callback payload (constructed top-5 order book + optional trade)
- `include/binance_usdm_connector.hpp`: connector config and lifecycle API
- `src/binance_usdm_connector.cpp`: websocket sessions, delta-to-book construction, dedup, callback emission


## Latency-oriented implementation notes

- Uses `boost::json::parser` with thread-local reuse to minimize JSON allocation churn on the hot path.
- Keeps in-memory order book state and emits pre-constructed top-5 snapshots directly from maintained maps.
- Uses fast numeric conversion (`strtod`) for string-encoded price/qty fields common in Binance payloads.
- Maintains parallel websocket sessions with fast reconnect backoff and stream-sequence deduplication.
