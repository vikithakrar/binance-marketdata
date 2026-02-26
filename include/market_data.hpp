#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace binance::futures {

enum class UpdateKind {
    Book,
    Trade,
};

struct PriceLevel {
    double price {};
    double qty {};
};

struct TradeInfo {
    std::int64_t trade_id {};
    double price {};
    double qty {};
    bool buyer_is_maker {};
};

// Generic market-data payload independent from a specific exchange market type.
// The order book is always emitted as a constructed top-N snapshot from streamed updates.
struct MarketData {
    static constexpr std::size_t kBookLevels = 5;

    std::string venue;
    std::string instrument;
    std::int64_t event_time_ms {};
    std::int64_t sequence {};
    UpdateKind kind {UpdateKind::Book};

    std::array<PriceLevel, kBookLevels> bids {};
    std::array<PriceLevel, kBookLevels> asks {};
    std::size_t bid_count {0};
    std::size_t ask_count {0};

    std::optional<TradeInfo> last_trade;
};

} // namespace binance::futures
