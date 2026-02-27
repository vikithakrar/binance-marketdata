# Low-Latency Market Data Connector (C++)

This project now follows a **multi-exchange adapter architecture**:

- `connectors::Connector` is transport/runtime orchestration (IO threads, websocket sessions, reconnects).
- You can now pass one or many adapters at construction time.
- `connectors::ExchangeAdapter` is venue-specific behavior (host/port, stream path, message mapping).
- `marketdata::MarketData` remains generic and market-agnostic.

Current implementation includes a production adapter for **Binance USDⓈ-M Futures** (`binance::futures::Adapter`).

## Architecture

- Generic connector core parses incoming websocket JSON using thread-local `boost::json::parser` reuse.
- Decoded events are pushed through a lock-free MPMC queue; a busy-polling thread consumes that queue and performs adapter-side dedup/book update.
- Adapter handles venue stream semantics and converts payloads into generic `marketdata::MarketData`.
- In-memory top-5 book snapshot is maintained in the Binance Futures adapter.
- Connector can fan out parallel sessions across resolved exchange IP endpoints with auto-reconnect.

## Key APIs

- `include/market_data.hpp`: generic market data payload.
- `include/connector.hpp`:
  - `connectors::ExchangeAdapter` (adapter interface)
  - `connectors::Connector` (generic connector runtime)
  - `binance::futures::Adapter` + `binance::futures::AdapterConfig` (venue adapter)

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

## Run demo

```bash
./build/binance_connector_demo
```
