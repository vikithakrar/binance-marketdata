#pragma once

#include "market_data.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace binance::futures {

using MarketDataCallback = std::function<void(const MarketData&)>;

class Connector {
public:
    struct Config {
        std::vector<std::string> symbols;
        bool subscribe_book_ticker {true};
        bool subscribe_depth_0ms {true};
        bool subscribe_trade {true};
        std::size_t parallel_connections {2};
        std::size_t io_threads {2};
    };

    explicit Connector(Config config);
    ~Connector();

    Connector(const Connector&) = delete;
    Connector& operator=(const Connector&) = delete;

    void start(MarketDataCallback callback);
    void stop();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace binance::futures
