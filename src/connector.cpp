#include "connector.hpp"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/json.hpp>
#include <boost/lockfree/queue.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>

namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace json = boost::json;
using tcp = asio::ip::tcp;

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

namespace binance::futures {

struct Adapter::Impl {
    explicit Impl(AdapterConfig cfg)
        : config(std::move(cfg)) {}

    struct BookState {
        std::map<double, double, std::greater<>> bids;
        std::map<double, double, std::less<>> asks;
        std::int64_t last_update_id {0};
    };

    AdapterConfig config;
    std::unordered_map<std::string, std::int64_t> max_source_id_by_stream;
    std::unordered_map<std::string, BookState> books;
};

Adapter::Adapter(AdapterConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

Adapter::~Adapter() = default;

std::string Adapter::host() const { return "fstream.binance.com"; }

std::string Adapter::port() const { return "443"; }

std::string Adapter::stream_path() const {
    std::vector<std::string> streams;
    streams.reserve(impl_->config.symbols.size() * 3);
    for (const auto& symbol : impl_->config.symbols) {
        const auto lower = to_lower(symbol);
        if (impl_->config.subscribe_book_ticker) streams.push_back(lower + "@bookTicker");
        if (impl_->config.subscribe_depth_0ms) streams.push_back(lower + "@depth@0ms");
        if (impl_->config.subscribe_trade) streams.push_back(lower + "@trade");
    }

    std::ostringstream oss;
    oss << "/stream?streams=";
    for (std::size_t i = 0; i < streams.size(); ++i) {
        if (i != 0) oss << '/';
        oss << streams[i];
    }
    return oss.str();
}

namespace {

bool should_emit(std::unordered_map<std::string, std::int64_t>& max_source_id_by_stream,
                 const std::string& key,
                 std::int64_t sequence) {
    auto& last = max_source_id_by_stream[key];
    if (sequence <= last) return false;
    last = sequence;
    return true;
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
        if (qty <= 0.0)
            side.erase(px);
        else
            side[px] = qty;
    }
}

std::int64_t get_i64(const json::object& obj, std::string_view key) {
    auto it = obj.find(key);
    if (it == obj.end()) return 0;
    return value_to_i64(it->value());
}

double get_double(const json::object& obj, std::string_view key) {
    auto it = obj.find(key);
    if (it == obj.end()) return 0.0;
    return value_to_double(it->value());
}

bool get_bool(const json::object& obj, std::string_view key) {
    auto it = obj.find(key);
    if (it == obj.end()) return false;
    return it->value().is_bool() ? it->value().as_bool() : false;
}

template <typename BidMap, typename AskMap>
void fill_top_levels(const BidMap& bids, const AskMap& asks, connectors::MarketData& md) {
    md.bid_count = 0;
    md.ask_count = 0;
    for (const auto& [price, qty] : bids) {
        if (md.bid_count >= connectors::MarketData::kBookLevels) break;
        md.bids[md.bid_count++] = connectors::PriceLevel{price, qty};
    }
    for (const auto& [price, qty] : asks) {
        if (md.ask_count >= connectors::MarketData::kBookLevels) break;
        md.asks[md.ask_count++] = connectors::PriceLevel{price, qty};
    }
}

} // namespace

std::optional<connectors::MarketData> Adapter::on_message(std::string_view stream, const json::object& data) {
    auto s_it = data.find("s");
    if (s_it == data.end() || !s_it->value().is_string()) return std::nullopt;
    const auto symbol_sv = s_it->value().as_string();
    const std::string symbol(symbol_sv.data(), symbol_sv.size());

    if (stream.find("@depth@0ms") != std::string_view::npos) {
        const auto update_id = get_i64(data, "u");
        if (!should_emit(impl_->max_source_id_by_stream, symbol + "@depth", update_id)) return std::nullopt;

        auto& book = impl_->books[symbol];
        book.last_update_id = update_id;
        apply_levels(book.bids, data, "b");
        apply_levels(book.asks, data, "a");

        connectors::MarketData md{};
        md.venue = "binance";
        md.instrument = symbol;
        md.event_time_ms = get_i64(data, "E");
        md.sequence = update_id;
        md.kind = connectors::UpdateKind::Book;
        fill_top_levels(book.bids, book.asks, md);
        return md;
    }

    if (stream.ends_with("@trade")) {
        const auto trade_id = get_i64(data, "t");
        if (!should_emit(impl_->max_source_id_by_stream, symbol + "@trade", trade_id)) return std::nullopt;

        connectors::MarketData md{};
        md.venue = "binance";
        md.instrument = symbol;
        md.event_time_ms = get_i64(data, "E");
        md.sequence = trade_id;
        md.kind = connectors::UpdateKind::Trade;
        md.last_trade = connectors::TradeInfo{trade_id, get_double(data, "p"), get_double(data, "q"), get_bool(data, "m")};

        auto it = impl_->books.find(symbol);
        if (it != impl_->books.end()) fill_top_levels(it->second.bids, it->second.asks, md);
        return md;
    }

    if (stream.ends_with("@bookTicker")) {
        const auto update_id = get_i64(data, "u");
        if (!should_emit(impl_->max_source_id_by_stream, symbol + "@bookTicker", update_id)) return std::nullopt;

        auto& book = impl_->books[symbol];
        book.last_update_id = std::max(book.last_update_id, update_id);

        if (data.contains("b") && data.contains("B")) {
            const auto px = get_double(data, "b");
            const auto qty = get_double(data, "B");
            if (qty <= 0.0)
                book.bids.erase(px);
            else
                book.bids[px] = qty;
        }
        if (data.contains("a") && data.contains("A")) {
            const auto px = get_double(data, "a");
            const auto qty = get_double(data, "A");
            if (qty <= 0.0)
                book.asks.erase(px);
            else
                book.asks[px] = qty;
        }

        connectors::MarketData md{};
        md.venue = "binance";
        md.instrument = symbol;
        md.event_time_ms = get_i64(data, "E");
        md.sequence = update_id;
        md.kind = connectors::UpdateKind::Book;
        fill_top_levels(book.bids, book.asks, md);
        return md;
    }

    return std::nullopt;
}

} // namespace binance::futures

namespace connectors {

class Connector::Impl {
public:
    Impl(RuntimeConfig runtime_config, std::vector<std::unique_ptr<ExchangeAdapter>> adapters)
        : ioc_(static_cast<int>(std::max<std::size_t>(1, runtime_config.io_threads))),
          work_guard_(asio::make_work_guard(ioc_)),
          ssl_ctx_(asio::ssl::context::tlsv12_client),
          runtime_config_(runtime_config),
          event_queue_(65536) {
        ssl_ctx_.set_default_verify_paths();
        ssl_ctx_.set_verify_mode(asio::ssl::verify_peer);

        adapter_runtimes_.reserve(adapters.size());
        for (auto& adapter : adapters) {
            adapter_runtimes_.push_back(std::make_unique<AdapterRuntime>(ioc_, std::move(adapter)));
        }
    }

    ~Impl() {
        stop();
        shutdown_io();
        drain_queue();
    }

    void start(MarketDataCallback callback) {
        callback_ = std::move(callback);
        if (!io_threads_started_) {
            start_io_threads();
        }

        stopped_.store(false, std::memory_order_release);

        polling_thread_ = std::thread([this]() { polling_loop(); });

        for (std::size_t i = 0; i < adapter_runtimes_.size(); ++i) {
            adapter_runtimes_[i]->sessions.clear();
            resolve_and_connect(i);
        }
    }

    void stop() {
        if (stopped_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }

        std::vector<std::shared_ptr<WebsocketSession>> sessions;
        for (auto& runtime : adapter_runtimes_) {
            beast::error_code ignored;
            runtime->resolver.cancel();
            runtime->resolve_retry_timer.cancel(ignored);
            sessions.insert(sessions.end(), runtime->sessions.begin(), runtime->sessions.end());
            runtime->sessions.clear();
        }

        for (auto& s : sessions) {
            s->stop();
        }

        if (polling_thread_.joinable()) {
            polling_thread_.join();
        }
    }

private:
    struct ParsedEvent {
        std::size_t adapter_index {};
        std::string stream;
        json::object data;
    };

    class WebsocketSession;

    struct AdapterRuntime {
        AdapterRuntime(asio::io_context& ioc, std::unique_ptr<ExchangeAdapter> in_adapter)
            : adapter(std::move(in_adapter)),
              resolver(asio::make_strand(ioc)),
              resolve_retry_timer(asio::make_strand(ioc)) {}

        std::unique_ptr<ExchangeAdapter> adapter;
        tcp::resolver resolver;
        asio::steady_timer resolve_retry_timer;
        std::vector<std::shared_ptr<WebsocketSession>> sessions;
    };

    class WebsocketSession : public std::enable_shared_from_this<WebsocketSession> {
    public:
        WebsocketSession(Impl& owner,
                         std::size_t adapter_index,
                         std::string stream_path,
                         std::string host,
                         tcp::endpoint endpoint,
                         std::uint32_t session_id)
            : owner_(owner),
              adapter_index_(adapter_index),
              stream_path_(std::move(stream_path)),
              host_(std::move(host)),
              endpoint_(endpoint),
              session_id_(session_id),
              stream_(asio::make_strand(owner_.ioc_), owner_.ssl_ctx_),
              reconnect_timer_(asio::make_strand(owner_.ioc_)) {}

        void start() { connect(); }

        void stop() {
            beast::error_code ec;
            reconnect_timer_.cancel(ec);
            beast::get_lowest_layer(stream_).cancel(ec);
            stream_.close(websocket::close_code::normal, ec);
        }

    private:
        void connect() {
            auto self = shared_from_this();
            beast::get_lowest_layer(stream_).expires_after(std::chrono::seconds(3));
            beast::get_lowest_layer(stream_).async_connect(endpoint_, [self](beast::error_code ec) {
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
            stream_.async_handshake(host_, stream_path_, [self](beast::error_code ec) {
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
                self->owner_.on_message(payload, self->adapter_index_, self->session_id_);
                self->read_loop();
            });
        }

        void schedule_reconnect(std::string_view stage, beast::error_code ec) {
            if (owner_.stopped_.load(std::memory_order_acquire)) return;
            std::cerr << "adapter=" << adapter_index_ << " session=" << session_id_ << " endpoint=" << endpoint_
                      << " stage=" << stage << " error=" << ec.message() << '\n';

            beast::error_code ignored;
            beast::get_lowest_layer(stream_).socket().close(ignored);

            reconnect_timer_.expires_after(std::chrono::milliseconds(150));
            auto self = shared_from_this();
            reconnect_timer_.async_wait([self](beast::error_code timer_ec) {
                if (timer_ec || self->owner_.stopped_.load(std::memory_order_acquire)) return;
                self->connect();
            });
        }

        Impl& owner_;
        std::size_t adapter_index_;
        std::string stream_path_;
        std::string host_;
        tcp::endpoint endpoint_;
        std::uint32_t session_id_;
        websocket::stream<beast::ssl_stream<beast::tcp_stream>> stream_;
        beast::flat_buffer read_buffer_;
        asio::steady_timer reconnect_timer_;
    };

    void resolve_and_connect(std::size_t adapter_index) {
        auto& runtime = *adapter_runtimes_[adapter_index];
        runtime.resolver.async_resolve(runtime.adapter->host(), runtime.adapter->port(),
            [this, adapter_index](beast::error_code ec, const tcp::resolver::results_type& results) {
                if (ec) {
                    schedule_resolve_retry(adapter_index, ec);
                    return;
                }
                connect_to_all_endpoints(adapter_index, results);
            });
    }

    void schedule_resolve_retry(std::size_t adapter_index, beast::error_code ec) {
        if (stopped_.load(std::memory_order_acquire)) return;
        std::cerr << "adapter=" << adapter_index << " resolve failed error=" << ec.message() << '\n';
        auto& timer = adapter_runtimes_[adapter_index]->resolve_retry_timer;
        timer.expires_after(std::chrono::milliseconds(300));
        timer.async_wait([this, adapter_index](beast::error_code timer_ec) {
            if (timer_ec || stopped_.load(std::memory_order_acquire)) return;
            resolve_and_connect(adapter_index);
        });
    }

    void connect_to_all_endpoints(std::size_t adapter_index, const tcp::resolver::results_type& results) {
        if (stopped_.load(std::memory_order_acquire)) return;

        std::vector<tcp::endpoint> endpoints;
        std::set<std::string> uniq;
        for (const auto& entry : results) {
            const auto ep = entry.endpoint();
            const auto key = ep.address().to_string() + ":" + std::to_string(ep.port());
            if (uniq.insert(key).second) endpoints.push_back(ep);
        }

        if (endpoints.empty()) {
            schedule_resolve_retry(adapter_index, beast::error_code{});
            return;
        }

        auto& runtime = *adapter_runtimes_[adapter_index];
        const auto stream_path = runtime.adapter->stream_path();
        const auto host = runtime.adapter->host();

        std::vector<std::shared_ptr<WebsocketSession>> new_sessions;
        const auto fanout = std::max<std::size_t>(1, runtime_config_.parallel_connections);
        for (std::size_t i = 0; i < fanout; ++i) {
            const auto& ep = endpoints[i % endpoints.size()];
            new_sessions.push_back(std::make_shared<WebsocketSession>(
                *this, adapter_index, stream_path, host, ep, static_cast<std::uint32_t>(i)));
        }

        runtime.sessions = new_sessions;
        for (auto& s : new_sessions) s->start();
    }

    void start_io_threads() {
        io_threads_started_ = true;
        const auto n = std::max<std::size_t>(1, runtime_config_.io_threads);
        io_threads_.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            io_threads_.emplace_back([this]() { ioc_.run(); });
        }
    }

    void shutdown_io() {
        work_guard_.reset();
        ioc_.stop();
        for (auto& t : io_threads_) if (t.joinable()) t.join();
        io_threads_.clear();
        io_threads_started_ = false;
    }

    void on_message(const std::string& payload, std::size_t adapter_index, std::uint32_t session_id) {
        try {
            thread_local json::parser parser;
            parser.reset();
            parser.write(payload);
            parser.finish();
            const auto root = parser.release();
            if (!root.is_object()) return;

            const auto& obj = root.as_object();
            auto stream_it = obj.find("stream");
            auto data_it = obj.find("data");
            if (stream_it == obj.end() || data_it == obj.end()) return;
            if (!stream_it->value().is_string() || !data_it->value().is_object()) return;

            auto* event = new ParsedEvent;
            event->adapter_index = adapter_index;
            const auto stream_sv = stream_it->value().as_string();
            event->stream.assign(stream_sv.data(), stream_sv.size());
            event->data = data_it->value().as_object();

            while (!stopped_.load(std::memory_order_acquire) && !event_queue_.push(event)) {
            }
            if (stopped_.load(std::memory_order_acquire)) {
                delete event;
            }
        } catch (const std::exception& ex) {
            std::cerr << "decode error adapter=" << adapter_index << " session=" << session_id
                      << " message=" << ex.what() << '\n';
        }
    }

    void polling_loop() {
        while (!stopped_.load(std::memory_order_acquire) || !event_queue_.empty()) {
            ParsedEvent* event = nullptr;
            if (!event_queue_.pop(event)) {
                continue;
            }

            std::unique_ptr<ParsedEvent> holder(event);
            if (holder->adapter_index >= adapter_runtimes_.size()) {
                continue;
            }

            const auto maybe_md = adapter_runtimes_[holder->adapter_index]->adapter->on_message(holder->stream, holder->data);
            if (maybe_md.has_value() && callback_) {
                callback_(*maybe_md);
            }
        }
    }

    void drain_queue() {
        ParsedEvent* event = nullptr;
        while (event_queue_.pop(event)) {
            delete event;
        }
    }

    asio::io_context ioc_;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard_;
    asio::ssl::context ssl_ctx_;
    RuntimeConfig runtime_config_;
    std::vector<std::unique_ptr<AdapterRuntime>> adapter_runtimes_;
    MarketDataCallback callback_;
    std::atomic_bool stopped_ {true};
    bool io_threads_started_ {false};
    std::vector<std::thread> io_threads_;
    std::thread polling_thread_;
    boost::lockfree::queue<ParsedEvent*> event_queue_;
};

Connector::Connector(RuntimeConfig runtime_config, std::unique_ptr<ExchangeAdapter> adapter) {
    std::vector<std::unique_ptr<ExchangeAdapter>> adapters;
    adapters.push_back(std::move(adapter));
    impl_ = std::make_unique<Impl>(runtime_config, std::move(adapters));
}

Connector::Connector(RuntimeConfig runtime_config, std::vector<std::unique_ptr<ExchangeAdapter>> adapters)
    : impl_(std::make_unique<Impl>(runtime_config, std::move(adapters))) {}

Connector::~Connector() = default;

void Connector::start(MarketDataCallback callback) { impl_->start(std::move(callback)); }

void Connector::stop() { impl_->stop(); }

} // namespace connectors
