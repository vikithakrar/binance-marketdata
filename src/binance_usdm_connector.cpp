#include "binance_usdm_connector.hpp"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/json.hpp>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string_view>
#include <thread>
#include <unordered_map>

namespace binance::futures {

namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace json = boost::json;
using tcp = asio::ip::tcp;

constexpr auto kHost = "fstream.binance.com";
constexpr auto kPort = "443";

std::string to_lower(std::string_view s) {
    std::string out(s);
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

double fast_to_double(std::string_view s) {
    char* end = nullptr;
    return std::strtod(s.data(), &end);
}

double value_to_double(const json::value& v) {
    if (v.is_string()) {
        const auto sv = v.as_string();
        return fast_to_double({sv.data(), sv.size()});
    }
    if (v.is_double()) return v.as_double();
    if (v.is_int64()) return static_cast<double>(v.as_int64());
    if (v.is_uint64()) return static_cast<double>(v.as_uint64());
    return 0.0;
}

std::int64_t value_to_i64(const json::value& v) {
    if (v.is_int64()) return v.as_int64();
    if (v.is_uint64()) return static_cast<std::int64_t>(v.as_uint64());
    if (v.is_string()) {
        const auto sv = v.as_string();
        return std::strtoll(sv.c_str(), nullptr, 10);
    }
    return 0;
}

} // namespace

class Connector::Impl {
public:
    explicit Impl(Config config)
        : ioc_(static_cast<int>(std::max<std::size_t>(1, config.io_threads))),
          work_guard_(asio::make_work_guard(ioc_)),
          ssl_ctx_(asio::ssl::context::tlsv12_client),
          config_(std::move(config)) {
        ssl_ctx_.set_default_verify_paths();
        ssl_ctx_.set_verify_mode(asio::ssl::verify_peer);
    }

    ~Impl() {
        stop();
        shutdown_io();
    }

    void start(MarketDataCallback callback) {
        std::scoped_lock lock(state_mutex_);
        callback_ = std::move(callback);
        if (!io_threads_started_) {
            start_io_threads();
        }
        stopped_ = false;
        sessions_.clear();

        const auto stream_path = build_stream_path();
        for (std::size_t i = 0; i < std::max<std::size_t>(1, config_.parallel_connections); ++i) {
            auto session = std::make_shared<WebsocketSession>(*this, stream_path, static_cast<std::uint32_t>(i));
            sessions_.push_back(session);
            session->start();
        }
    }

    void stop() {
        std::vector<std::shared_ptr<WebsocketSession>> sessions;
        {
            std::scoped_lock lock(state_mutex_);
            if (stopped_) {
                return;
            }
            stopped_ = true;
            sessions = sessions_;
            sessions_.clear();
        }
        for (auto& s : sessions) {
            s->stop();
        }
    }

private:
    struct BookState {
        std::map<double, double, std::greater<>> bids;
        std::map<double, double, std::less<>> asks;
        std::int64_t last_update_id {0};
    };

    class WebsocketSession : public std::enable_shared_from_this<WebsocketSession> {
    public:
        WebsocketSession(Impl& owner, std::string stream_path, std::uint32_t session_id)
            : owner_(owner),
              stream_path_(std::move(stream_path)),
              session_id_(session_id),
              resolver_(asio::make_strand(owner_.ioc_)),
              stream_(asio::make_strand(owner_.ioc_), owner_.ssl_ctx_),
              reconnect_timer_(asio::make_strand(owner_.ioc_)) {}

        void start() { resolve(); }

        void stop() {
            beast::error_code ec;
            reconnect_timer_.cancel(ec);
            resolver_.cancel();
            beast::get_lowest_layer(stream_).cancel(ec);
            stream_.close(websocket::close_code::normal, ec);
        }

    private:
        void resolve() {
            auto self = shared_from_this();
            resolver_.async_resolve(kHost, kPort, [self](beast::error_code ec, const tcp::resolver::results_type& results) {
                if (ec) {
                    self->schedule_reconnect("resolve", ec);
                    return;
                }
                self->connect(results);
            });
        }

        void connect(const tcp::resolver::results_type& results) {
            auto self = shared_from_this();
            beast::get_lowest_layer(stream_).expires_after(std::chrono::seconds(3));
            beast::get_lowest_layer(stream_).async_connect(
                results,
                [self](beast::error_code ec, const tcp::resolver::results_type::endpoint_type&) {
                    if (ec) {
                        self->schedule_reconnect("connect", ec);
                        return;
                    }
                    self->handshake_ssl();
                });
        }

        void handshake_ssl() {
            auto self = shared_from_this();
            stream_.next_layer().async_handshake(asio::ssl::stream_base::client, [self](beast::error_code ec) {
                if (ec) {
                    self->schedule_reconnect("ssl handshake", ec);
                    return;
                }
                self->handshake_ws();
            });
        }

        void handshake_ws() {
            auto self = shared_from_this();
            beast::get_lowest_layer(stream_).expires_never();
            stream_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
            stream_.async_handshake(kHost, stream_path_, [self](beast::error_code ec) {
                if (ec) {
                    self->schedule_reconnect("ws handshake", ec);
                    return;
                }
                self->read_loop();
            });
        }

        void read_loop() {
            auto self = shared_from_this();
            stream_.async_read(read_buffer_, [self](beast::error_code ec, std::size_t) {
                if (ec) {
                    self->schedule_reconnect("read", ec);
                    return;
                }
                auto payload = beast::buffers_to_string(self->read_buffer_.data());
                self->read_buffer_.consume(self->read_buffer_.size());
                self->owner_.on_message(payload, self->session_id_);
                self->read_loop();
            });
        }

        void schedule_reconnect(std::string_view stage, beast::error_code ec) {
            if (owner_.stopped_) return;
            std::cerr << "session=" << session_id_ << " stage=" << stage << " error=" << ec.message() << '\n';
            beast::error_code ignored;
            beast::get_lowest_layer(stream_).socket().close(ignored);
            reconnect_timer_.expires_after(std::chrono::milliseconds(150));
            auto self = shared_from_this();
            reconnect_timer_.async_wait([self](beast::error_code timer_ec) {
                if (timer_ec || self->owner_.stopped_) return;
                self->resolve();
            });
        }

        Impl& owner_;
        std::string stream_path_;
        std::uint32_t session_id_;
        tcp::resolver resolver_;
        websocket::stream<beast::ssl_stream<beast::tcp_stream>> stream_;
        beast::flat_buffer read_buffer_;
        asio::steady_timer reconnect_timer_;
    };

    void start_io_threads() {
        io_threads_started_ = true;
        const auto n = std::max<std::size_t>(1, config_.io_threads);
        io_threads_.reserve(n);
        for (std::size_t i = 0; i < n; ++i) io_threads_.emplace_back([this]() { ioc_.run(); });
    }

    void shutdown_io() {
        work_guard_.reset();
        ioc_.stop();
        for (auto& t : io_threads_) if (t.joinable()) t.join();
        io_threads_.clear();
        io_threads_started_ = false;
    }

    std::string build_stream_path() const {
        std::vector<std::string> streams;
        streams.reserve(config_.symbols.size() * 3);
        for (const auto& symbol : config_.symbols) {
            const auto lower = to_lower(symbol);
            if (config_.subscribe_book_ticker) streams.push_back(lower + "@bookTicker");
            if (config_.subscribe_depth_0ms) streams.push_back(lower + "@depth@0ms");
            if (config_.subscribe_trade) streams.push_back(lower + "@trade");
        }
        std::ostringstream oss;
        oss << "/stream?streams=";
        for (std::size_t i = 0; i < streams.size(); ++i) {
            if (i != 0) oss << '/';
            oss << streams[i];
        }
        return oss.str();
    }

    void on_message(const std::string& payload, std::uint32_t session_id) {
        try {
            thread_local json::parser parser;
            parser.reset();
            parser.write(payload);
            parser.finish();
            const auto& root = parser.release();
            if (!root.is_object()) return;
            const auto& obj = root.as_object();
            auto stream_it = obj.find("stream");
            auto data_it = obj.find("data");
            if (stream_it == obj.end() || data_it == obj.end()) return;
            if (!stream_it->value().is_string() || !data_it->value().is_object()) return;

            const auto stream_sv = stream_it->value().as_string();
            const std::string stream(stream_sv.data(), stream_sv.size());
            const auto& data = data_it->value().as_object();
            const auto maybe_md = build_market_data(stream, data);
            if (maybe_md && callback_) callback_(*maybe_md);
        } catch (const std::exception& ex) {
            std::cerr << "decode error session=" << session_id << " message=" << ex.what() << '\n';
        }
    }

    std::optional<MarketData> build_market_data(const std::string& stream, const json::object& data) {
        auto s_it = data.find("s");
        if (s_it == data.end() || !s_it->value().is_string()) return std::nullopt;
        const auto symbol_sv = s_it->value().as_string();
        const std::string symbol(symbol_sv.data(), symbol_sv.size());

        if (stream.find("@depth@0ms") != std::string::npos) return on_depth_update(symbol, data);
        if (stream.ends_with("@trade")) return on_trade_update(symbol, data);
        if (stream.ends_with("@bookTicker")) return on_book_ticker_update(symbol, data);
        return std::nullopt;
    }

    std::optional<MarketData> on_depth_update(const std::string& symbol, const json::object& data) {
        const auto update_id = get_i64(data, "u");
        if (!should_emit(symbol + "@depth", update_id)) return std::nullopt;

        std::scoped_lock lock(book_mutex_);
        auto& book = books_[symbol];
        book.last_update_id = update_id;
        apply_levels(book.bids, data, "b");
        apply_levels(book.asks, data, "a");

        MarketData md{};
        md.venue = "binance";
        md.instrument = symbol;
        md.event_time_ms = get_i64(data, "E");
        md.sequence = update_id;
        md.kind = UpdateKind::Book;
        fill_top_levels(book, md);
        return md;
    }

    std::optional<MarketData> on_trade_update(const std::string& symbol, const json::object& data) {
        const auto trade_id = get_i64(data, "t");
        if (!should_emit(symbol + "@trade", trade_id)) return std::nullopt;

        MarketData md{};
        md.venue = "binance";
        md.instrument = symbol;
        md.event_time_ms = get_i64(data, "E");
        md.sequence = trade_id;
        md.kind = UpdateKind::Trade;
        md.last_trade = TradeInfo{trade_id,
                                  get_double(data, "p"),
                                  get_double(data, "q"),
                                  get_bool(data, "m")};

        std::scoped_lock lock(book_mutex_);
        auto it = books_.find(symbol);
        if (it != books_.end()) fill_top_levels(it->second, md);
        return md;
    }

    std::optional<MarketData> on_book_ticker_update(const std::string& symbol, const json::object& data) {
        const auto update_id = get_i64(data, "u");
        if (!should_emit(symbol + "@bookTicker", update_id)) return std::nullopt;

        std::scoped_lock lock(book_mutex_);
        auto& book = books_[symbol];
        book.last_update_id = std::max(book.last_update_id, update_id);

        if (data.contains("b") && data.contains("B")) {
            const auto px = get_double(data, "b");
            const auto qty = get_double(data, "B");
            if (qty <= 0.0) book.bids.erase(px); else book.bids[px] = qty;
        }
        if (data.contains("a") && data.contains("A")) {
            const auto px = get_double(data, "a");
            const auto qty = get_double(data, "A");
            if (qty <= 0.0) book.asks.erase(px); else book.asks[px] = qty;
        }

        MarketData md{};
        md.venue = "binance";
        md.instrument = symbol;
        md.event_time_ms = get_i64(data, "E");
        md.sequence = update_id;
        md.kind = UpdateKind::Book;
        fill_top_levels(book, md);
        return md;
    }

    template <typename BookSide>
    void apply_levels(BookSide& side, const json::object& data, std::string_view key) {
        auto it = data.find(key);
        if (it == data.end() || !it->value().is_array()) return;
        for (const auto& level : it->value().as_array()) {
            if (!level.is_array()) continue;
            const auto& arr = level.as_array();
            if (arr.size() < 2) continue;
            const auto px = value_to_double(arr[0]);
            const auto qty = value_to_double(arr[1]);
            if (qty <= 0.0) side.erase(px); else side[px] = qty;
        }
    }

    static double get_double(const json::object& obj, std::string_view key) {
        auto it = obj.find(key);
        if (it == obj.end()) return 0.0;
        return value_to_double(it->value());
    }

    static std::int64_t get_i64(const json::object& obj, std::string_view key) {
        auto it = obj.find(key);
        if (it == obj.end()) return 0;
        return value_to_i64(it->value());
    }

    static bool get_bool(const json::object& obj, std::string_view key) {
        auto it = obj.find(key);
        if (it == obj.end()) return false;
        return it->value().is_bool() ? it->value().as_bool() : false;
    }

    void fill_top_levels(const BookState& book, MarketData& md) const {
        md.bid_count = 0;
        md.ask_count = 0;
        for (const auto& [price, qty] : book.bids) {
            if (md.bid_count >= MarketData::kBookLevels) break;
            md.bids[md.bid_count++] = PriceLevel{price, qty};
        }
        for (const auto& [price, qty] : book.asks) {
            if (md.ask_count >= MarketData::kBookLevels) break;
            md.asks[md.ask_count++] = PriceLevel{price, qty};
        }
    }

    bool should_emit(const std::string& stream_key, std::int64_t sequence) {
        std::scoped_lock lock(dedup_mutex_);
        auto& last = max_source_id_by_stream_[stream_key];
        if (sequence <= last) return false;
        last = sequence;
        return true;
    }

    asio::io_context ioc_;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
    asio::ssl::context ssl_ctx_;
    Config config_;
    MarketDataCallback callback_;
    bool stopped_ {true};
    bool io_threads_started_ {false};
    std::vector<std::thread> io_threads_;
    std::vector<std::shared_ptr<WebsocketSession>> sessions_;
    std::mutex dedup_mutex_;
    std::mutex state_mutex_;
    std::mutex book_mutex_;
    std::unordered_map<std::string, std::int64_t> max_source_id_by_stream_;
    std::unordered_map<std::string, BookState> books_;
};

Connector::Connector(Config config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

Connector::~Connector() = default;

void Connector::start(MarketDataCallback callback) { impl_->start(std::move(callback)); }

void Connector::stop() { impl_->stop(); }

} // namespace binance::futures
