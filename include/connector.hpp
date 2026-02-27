#pragma once

#include "market_data.hpp"

#include <boost/json/object.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace connectors {

using MarketData = ::marketdata::MarketData;
using UpdateKind = ::marketdata::UpdateKind;
using PriceLevel = ::marketdata::PriceLevel;
using TradeInfo = ::marketdata::TradeInfo;
using MarketDataCallback = std::function<void(const MarketData&)>;

class ExchangeAdapter {
public:
    virtual ~ExchangeAdapter() = default;

    virtual std::string host() const = 0;
    virtual std::string port() const = 0;
    virtual std::string stream_path() const = 0;

    virtual std::optional<MarketData> on_message(std::string_view stream,
                                                 const boost::json::object& data) = 0;
};

class Connector {
public:
    struct RuntimeConfig {
        std::size_t parallel_connections {2};
        std::size_t io_threads {2};
    };

    Connector(RuntimeConfig runtime_config, std::unique_ptr<ExchangeAdapter> adapter);
    Connector(RuntimeConfig runtime_config, std::vector<std::unique_ptr<ExchangeAdapter>> adapters);
    ~Connector();

    Connector(const Connector&) = delete;
    Connector& operator=(const Connector&) = delete;

    void start(MarketDataCallback callback);
    void stop();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace connectors

namespace binance::futures {

struct AdapterConfig {
    std::vector<std::string> symbols;
    bool subscribe_book_ticker {true};
    bool subscribe_depth_0ms {true};
    bool subscribe_trade {true};
};

class Adapter final : public connectors::ExchangeAdapter {
public:
    explicit Adapter(AdapterConfig config);
    ~Adapter() override;

    std::string host() const override;
    std::string port() const override;
    std::string stream_path() const override;

    std::optional<connectors::MarketData> on_message(std::string_view stream,
                                                     const boost::json::object& data) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace binance::futures
