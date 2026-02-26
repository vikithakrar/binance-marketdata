#include "binance_usdm_connector.hpp"

#include <atomic>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <thread>

namespace {
std::atomic_bool keep_running {true};

void signal_handler(int) { keep_running = false; }

const char* kind_name(binance::futures::UpdateKind kind) {
    switch (kind) {
    case binance::futures::UpdateKind::Book:
        return "book";
    case binance::futures::UpdateKind::Trade:
        return "trade";
    }
    return "unknown";
}
} // namespace

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    binance::futures::Connector::Config cfg;
    cfg.symbols = {"BTCUSDT", "ETHUSDT"};
    cfg.parallel_connections = 2;
    cfg.io_threads = 2;
    cfg.subscribe_book_ticker = true;
    cfg.subscribe_depth_0ms = true;
    cfg.subscribe_trade = true;

    binance::futures::Connector connector(cfg);
    connector.start([](const binance::futures::MarketData& md) {
        std::cout << std::fixed << std::setprecision(8)
                  << "venue=" << md.venue
                  << " instrument=" << md.instrument
                  << " kind=" << kind_name(md.kind)
                  << " t=" << md.event_time_ms
                  << " seq=" << md.sequence;

        std::cout << " bids[" << md.bid_count << "]=";
        for (std::size_t i = 0; i < md.bid_count; ++i) {
            std::cout << md.bids[i].price << "@" << md.bids[i].qty << (i + 1 < md.bid_count ? "," : "");
        }
        std::cout << " asks[" << md.ask_count << "]=";
        for (std::size_t i = 0; i < md.ask_count; ++i) {
            std::cout << md.asks[i].price << "@" << md.asks[i].qty << (i + 1 < md.ask_count ? "," : "");
        }

        if (md.last_trade.has_value()) {
            std::cout << " trade=" << md.last_trade->price
                      << " qty=" << md.last_trade->qty
                      << " maker=" << (md.last_trade->buyer_is_maker ? "1" : "0");
        }
        std::cout << '\n';
    });

    while (keep_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    connector.stop();
    return 0;
}
