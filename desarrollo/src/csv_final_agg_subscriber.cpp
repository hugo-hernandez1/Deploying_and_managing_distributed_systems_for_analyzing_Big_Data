#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/core/status/SubscriptionMatchedStatus.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <curl/curl.h>
#include "AirSamplePubSubTypes.hpp"
#include "AirQualityStatsPubSubTypes.hpp"

using namespace eprosima::fastdds::dds;

namespace
{

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct AppConfig
{
    bool        use_xml  = false;
    std::string xml_path;

    std::string influx_url    = "http://10.106.45.50:8086";
    std::string influx_token  = "tfm-influx-admin-token-changeme";
    std::string influx_org    = "tfm";
    std::string influx_bucket = "air_quality";

    // Reporte final CSV (ver estructura más abajo).
    std::string report_csv    = "air_quality_report.csv";
    std::string node_id;
};

std::atomic_bool g_running(true);
void signal_handler(int) { g_running = false; }

std::string default_node_id()
{
    if (const char* env = std::getenv("NODE_ID"))
    {
        if (env[0] != '\0') return env;
    }
    if (const char* env = std::getenv("HOSTNAME"))
    {
        if (env[0] != '\0') return env;
    }
    return "csv_final_agg_subscriber";
}

long long now_ns()
{
    using namespace std::chrono;
    return duration_cast<nanoseconds>(
        system_clock::now().time_since_epoch()).count();
}

std::string wall_clock_now_iso()
{
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

void print_usage(const char* p)
{
    std::cout
        << "Usage: " << p << " -c profile.xml [options]\n\n"
        << "  -c <path>         XML QoS profile (required)\n"
        << "  --influx-url      InfluxDB URL       (default: http://10.106.45.50:8086)\n"
        << "  --influx-token    InfluxDB token     (default: tfm-influx-admin-token-changeme)\n"
        << "  --influx-org      InfluxDB org       (default: tfm)\n"
        << "  --influx-bucket   InfluxDB bucket    (default: air_quality)\n"
        << "  --report-csv <p>  Final report CSV   (default: air_quality_report.csv)\n"
        << "  --node-id <s>     Node identifier    (default: $NODE_ID/$HOSTNAME)\n";
}

AppConfig parse_args(int argc, char** argv)
{
    AppConfig cfg;
    std::vector<std::string> extra;

    for (int i = 1; i < argc; ++i)
    {
        std::string a = argv[i];
        auto need = [&]() {
            if (i + 1 >= argc)
                throw std::runtime_error("Missing value after " + a);
        };

        if      (a == "-h" || a == "--help")  { print_usage(argv[0]); std::exit(0); }
        else if (a == "-c")                   { need(); cfg.use_xml = true; cfg.xml_path = argv[++i]; }
        else if (a == "--influx-url")         { need(); cfg.influx_url    = argv[++i]; }
        else if (a == "--influx-token")       { need(); cfg.influx_token  = argv[++i]; }
        else if (a == "--influx-org")         { need(); cfg.influx_org    = argv[++i]; }
        else if (a == "--influx-bucket")      { need(); cfg.influx_bucket = argv[++i]; }
        else if (a == "--report-csv")         { need(); cfg.report_csv    = argv[++i]; }
        else if (a == "--node-id")            { need(); cfg.node_id       = argv[++i]; }
        else extra.push_back(a);
    }

    if (!extra.empty())
        throw std::runtime_error("Unrecognized argument: " + extra[0]);

    if (cfg.node_id.empty()) cfg.node_id = default_node_id();
    return cfg;
}

// ---------------------------------------------------------------------------
// Final report CSV
// ---------------------------------------------------------------------------
//
// Un solo fichero, orientado a eventos. Cada fila representa una muestra
// recibida o un evento de instrumentación.
//
// Columnas:
//   timestamp                 ISO-8601 UTC del momento de recepción
//   node_id                   identificador del nodo que escribe la fila
//   scope_visible             topic/scope al que se refiere la fila
//   source_writer             origin_writer_id de la muestra (o "-" en
//                             eventos no ligados a un writer concreto)
//   target_reader             node_id del receptor (redundante con node_id,
//                             pero se deja por el formato pedido)
//   topic                     nombre del topic
//   magnitude_id              id lógico del flujo (long)
//   sequence_id               sequence_id de la muestra (long long)
//   src_first_seq,src_last_seq,src_sample_count
//                             sólo para agg; "" para raw y para eventos
//   event_type                SAMPLE | GAP | OUT_OF_ORDER | RECOVERY |
//                             FIRST_SEEN | MATCH | UNMATCH
//   lost_messages_detected    cuando event_type == GAP
//   seq_gap_detected          1/0
//   recovery_time_ms          cuando event_type == RECOVERY
//   latency_mean_us           de la muestra individual (recv_ns - publish_ts_ns)
//                             convertido a µs
//   publication_throughput_Bps lo deja el monitor; aquí siempre "" (ver notas)
//   subscription_throughput_Bps idem
//   resent_datas              idem, viene del monitor
//   status                    "ok" | "0 real" | "NaN" | "sin muestras" |
//                             "discontinuity" | "duplicate" | "recovered" …
//   notes                     texto libre
// ---------------------------------------------------------------------------

class ReportCsv
{
public:
    ReportCsv(const std::string& path, const std::string& node_id)
        : path_(path), node_id_(node_id)
    {
        file_.open(path_, std::ios::app | std::ios::binary);
        if (!file_.is_open())
        {
            std::cerr << "[REPORT] Cannot open report CSV: " << path_ << "\n";
            return;
        }
        file_.seekp(0, std::ios::end);
        if (file_.tellp() == 0)
        {
            file_
                << "timestamp,node_id,scope_visible,source_writer,target_reader,"
                   "topic,magnitude_id,sequence_id,"
                   "src_first_seq,src_last_seq,src_sample_count,"
                   "event_type,lost_messages_detected,seq_gap_detected,"
                   "recovery_time_ms,latency_mean_us,"
                   "publication_throughput_Bps,subscription_throughput_Bps,"
                   "resent_datas,status,notes\n";
        }
        std::cout << "[REPORT] Final report CSV: " << path_ << "\n";
    }

    struct Row
    {
        std::string scope_visible;
        std::string source_writer;
        std::string topic;
        int32_t     magnitude_id = 0;
        int64_t     sequence_id  = 0;
        std::string src_first_seq;  // vacío si no aplica
        std::string src_last_seq;
        std::string src_sample_count;
        std::string event_type;
        std::string lost_messages_detected;
        std::string seq_gap_detected;
        std::string recovery_time_ms;
        std::string latency_mean_us;
        std::string status;
        std::string notes;
    };

    void write(const Row& r)
    {
        if (!file_.is_open()) return;
        std::lock_guard<std::mutex> lk(mu_);
        file_
            << wall_clock_now_iso() << ','
            << escape(node_id_)        << ','
            << escape(r.scope_visible) << ','
            << escape(r.source_writer) << ','
            << escape(node_id_)        << ','   // target_reader == este nodo
            << escape(r.topic)         << ','
            << r.magnitude_id          << ','
            << r.sequence_id           << ','
            << r.src_first_seq         << ','
            << r.src_last_seq          << ','
            << r.src_sample_count      << ','
            << escape(r.event_type)    << ','
            << r.lost_messages_detected << ','
            << r.seq_gap_detected      << ','
            << r.recovery_time_ms      << ','
            << r.latency_mean_us       << ','
            << ""                      << ','   // publication_throughput (monitor)
            << ""                      << ','   // subscription_throughput (monitor)
            << ""                      << ','   // resent_datas (monitor)
            << escape(r.status)        << ','
            << escape(r.notes)         << '\n';
        file_.flush();
    }

private:
    static std::string escape(const std::string& s)
    {
        if (s.find_first_of(",\"\n") == std::string::npos) return s;
        std::string out = "\"";
        for (char c : s) { if (c == '"') out += '"'; out += c; }
        out += '"';
        return out;
    }

    std::string     path_;
    std::string     node_id_;
    std::ofstream   file_;
    std::mutex      mu_;
};

// ---------------------------------------------------------------------------
// Gap/recovery tracker end-to-end (compartido por ambos listeners)
// ---------------------------------------------------------------------------

class FlowTracker
{
public:
    explicit FlowTracker(ReportCsv& report) : report_(report) {}

    struct Observation
    {
        bool    in_order     = true;
        bool    gap          = false;
        bool    out_of_order = false;
        bool    duplicate    = false;
        bool    first_seen   = false;
        int64_t lost_messages = 0;
        int64_t gap_duration_ms = 0;
    };

    Observation observe(
        const std::string& origin,
        int32_t magnitude_id,
        int64_t seq)
    {
        Observation obs;
        Key k{origin, magnitude_id};
        int64_t wall = now_ns();

        std::lock_guard<std::mutex> lk(mu_);
        auto it = state_.find(k);
        if (it == state_.end())
        {
            obs.first_seen = true;
            state_[k] = State{seq, wall};
            return obs;
        }
        auto& st = it->second;
        int64_t last = st.last_seq;

        if (seq == last + 1)
        {
            st.last_seq = seq;
            st.last_seen_ns = wall;
            return obs;
        }
        if (seq > last + 1)
        {
            obs.gap = true;
            obs.lost_messages   = seq - last - 1;
            obs.gap_duration_ms = (wall - st.last_seen_ns) / 1'000'000LL;
            st.last_seq = seq;
            st.last_seen_ns = wall;
            return obs;
        }
        if (seq == last)
        {
            obs.duplicate = true;
        }
        else
        {
            obs.out_of_order = true;
        }
        return obs;
    }

    // Registro de unmatch / rematch por writer_identity (origin).
    // Como un mismo writer puede servir varios magnitude_id, el evento de
    // recovery se atribuye al writer y dejamos el magnitude_id del primer
    // mensaje que lo dispara. Si quieres granularidad por magnitud puedes
    // registrar recoveries separadas por (origin,magnitude_id); aquí se
    // registra al nivel del writer porque el unmatch afecta al writer.
    void on_unmatch(const std::string& origin_hint, const std::string& topic)
    {
        (void)origin_hint; (void)topic;
        std::lock_guard<std::mutex> lk(rec_mu_);
        last_unmatch_ns_ = now_ns();
        pending_recovery_ = false;
    }

    void on_rematch()
    {
        std::lock_guard<std::mutex> lk(rec_mu_);
        if (last_unmatch_ns_ != 0)
        {
            pending_recovery_ = true;
        }
    }

    // Devuelve >0 ms si esta muestra es la primera tras un rematch;
    // 0 en caso contrario. Resetea el pending.
    int64_t consume_pending_recovery_ms()
    {
        std::lock_guard<std::mutex> lk(rec_mu_);
        if (!pending_recovery_ || last_unmatch_ns_ == 0) return 0;
        int64_t ms = (now_ns() - last_unmatch_ns_) / 1'000'000LL;
        pending_recovery_ = false;
        last_unmatch_ns_ = 0;
        return ms;
    }

private:
    struct Key
    {
        std::string origin;
        int32_t     magnitude_id;
        bool operator<(const Key& o) const
        {
            if (origin != o.origin) return origin < o.origin;
            return magnitude_id < o.magnitude_id;
        }
    };
    struct State { int64_t last_seq = 0; int64_t last_seen_ns = 0; };

    ReportCsv&           report_;
    std::mutex           mu_;
    std::map<Key, State> state_;

    std::mutex rec_mu_;
    int64_t    last_unmatch_ns_ = 0;
    bool       pending_recovery_ = false;
};

// ---------------------------------------------------------------------------
// InfluxDB writer (sin cambios funcionales relevantes)
// ---------------------------------------------------------------------------

class InfluxWriter
{
public:
    InfluxWriter()  { curl_global_init(CURL_GLOBAL_DEFAULT); }
    ~InfluxWriter() { curl_global_cleanup(); }

    bool init(const AppConfig& cfg)
    {
        url_   = cfg.influx_url + "/api/v2/write"
                 + "?org="    + cfg.influx_org
                 + "&bucket=" + cfg.influx_bucket
                 + "&precision=s";
        token_ = "Token " + cfg.influx_token;

        CURL* curl = curl_easy_init();
        if (!curl) { std::cerr << "[INFLUX] curl init failed\n"; return false; }

        std::string ping = cfg.influx_url + "/ping";
        curl_easy_setopt(curl, CURLOPT_URL,           ping.c_str());
        curl_easy_setopt(curl, CURLOPT_NOBODY,        1L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT,       5L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_cb);

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK)
        {
            std::cerr << "[INFLUX] Cannot reach InfluxDB: " << curl_easy_strerror(res) << "\n"
                      << "[INFLUX] Running in stdout-only mode.\n";
            enabled_ = false;
        }
        else
        {
            std::cout << "[INFLUX] Connected to " << cfg.influx_url << "\n";
            enabled_ = true;
        }

        return true; // non-fatal
    }

    void write_raw(const AirSample& s)
    {
        if (!enabled_) return;

        std::ostringstream lp;
        lp << "raw_measurements"
           << ",station="  << escape_tag(s.station())
           << ",magnitud=" << s.magnitud()
           << " "
           << "value="    << s.value()    << ","
           << "hour="     << s.hour()     << "i,"
           << "sequence_id=" << s.sequence_id() << "i"
           << " "
           << now_unix_s();

        post(lp.str(), "raw");
    }

    void write_agg(const AirQualityStats& s)
    {
        if (!enabled_) return;

        std::ostringstream lp;
        lp << "agg_stats"
           << ",station="  << escape_tag(s.station())
           << ",magnitud=" << s.magnitud()
           << ",reason="   << escape_tag(s.publish_reason())
           << " "
           << "daily_mean="   << s.daily_mean()   << ","
           << "daily_min="    << s.daily_min()     << ","
           << "daily_max="    << s.daily_max()     << ","
           << "daily_count="  << s.daily_count()   << "i,"
           << "trend_slope="  << s.trend_slope()   << ","
           << "zscore="       << s.zscore()        << ","
           << "zscore_mu="    << s.zscore_mu()     << ","
           << "zscore_sigma=" << s.zscore_sigma()  << ","
           << "zscore_valid=" << (s.zscore_valid() ? "true" : "false") << ","
           << "is_anomaly="   << (s.is_anomaly()   ? "true" : "false") << ","
           << "is_spike="     << (s.is_spike()     ? "true" : "false") << ","
           << "sequence_id="  << s.sequence_id()   << "i,"
           << "src_first_seq=" << s.src_first_seq() << "i,"
           << "src_last_seq="  << s.src_last_seq()  << "i,"
           << "src_sample_count=" << s.src_sample_count() << "i,"
           << "trend_label="  << "\"" << s.trend_label() << "\""
           << " "
           << now_unix_s();

        post(lp.str(), "agg");
    }

    std::size_t written_raw() const { return written_raw_.load(); }
    std::size_t written_agg() const { return written_agg_.load(); }

private:
    static size_t discard_cb(void*, size_t sz, size_t n, void*) { return sz * n; }

    void post(const std::string& body, const char* label)
    {
        CURL* curl = curl_easy_init();
        if (!curl) return;

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, ("Authorization: " + token_).c_str());
        headers = curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");

        curl_easy_setopt(curl, CURLOPT_URL,           url_.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER,    headers);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS,    body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)body.size());
        curl_easy_setopt(curl, CURLOPT_TIMEOUT,       5L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_cb);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

        if (res != CURLE_OK)
            std::cerr << "[INFLUX][" << label << "] " << curl_easy_strerror(res) << "\n";
        else if (http_code != 204)
            std::cerr << "[INFLUX][" << label << "] HTTP " << http_code << "\n";
        else
        {
            if (label[0] == 'r') ++written_raw_;
            else                 ++written_agg_;
        }

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }

    static std::string escape_tag(const std::string& s)
    {
        std::string out;
        for (char c : s)
        {
            if (c == ' ' || c == ',' || c == '=') out += '\\';
            out += c;
        }
        return out;
    }

    static int64_t unix_ts(int32_t y, int32_t mo, int32_t d, int32_t h)
    {
        int32_t years = y - 1970;
        int32_t leaps = years > 0
            ? (years-1)/4 - (years-1)/100 + (years-1)/400 + 1 : 0;
        static const int32_t dim[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
        int32_t yday = 0;
        for (int32_t i = 1; i < mo; ++i) yday += dim[i];
        bool leap = (y%4==0 && (y%100!=0 || y%400==0));
        if (leap && mo > 2) yday += 1;
        int64_t days = (int64_t)years * 365 + leaps + yday + (d - 1);
        return days * 86400 + (int64_t)h * 3600;
    }
    long long now_unix_s()
    {
        using namespace std::chrono;
        return duration_cast<seconds>(
            system_clock::now().time_since_epoch()
        ).count();
    }

    std::string url_, token_;
    bool        enabled_ = false;
    std::atomic<std::size_t> written_raw_{0};
    std::atomic<std::size_t> written_agg_{0};
};

// ---------------------------------------------------------------------------
// Listener for air_quality_raw_fwd  (AirSample)
// ---------------------------------------------------------------------------

class RawListener : public DataReaderListener
{
public:
    RawListener(InfluxWriter& influx, ReportCsv& report, FlowTracker& flow)
        : influx_(influx), report_(report), flow_(flow) {}

    std::atomic_int      matched_{0};
    std::atomic_uint64_t received_{0};

    void on_subscription_matched(
            DataReader*, const SubscriptionMatchedStatus& info) override
    {
        matched_.store(info.current_count);
        ReportCsv::Row row;
        row.scope_visible = "raw";
        row.topic         = "air_quality_raw_fwd";
        if (info.current_count_change == 1)
        {
            std::cout << "[RAW][MATCH] Publisher matched. total=" << info.current_count << "\n";
            row.event_type = "MATCH";
            row.status     = "ok";
            row.notes      = "publisher matched";
            flow_.on_rematch();
        }
        else if (info.current_count_change == -1)
        {
            std::cout << "[RAW][MATCH] Publisher unmatched. total=" << info.current_count << "\n";
            row.event_type = "UNMATCH";
            row.status     = "sin muestras";
            row.notes      = "publisher unmatched";
            flow_.on_unmatch("-", "air_quality_raw_fwd");
        }
        else
        {
            return;
        }
        report_.write(row);
    }

    void on_data_available(DataReader* reader) override
    {
        AirSample  sample;
        SampleInfo info;

        while (reader->take_next_sample(&sample, &info) == RETCODE_OK)
        {
            if (!info.valid_data) continue;

            const auto n = ++received_;

            // Latencia: publish_ts_ns vs reloj local. Sólo significativa si
            // ambos procesos comparten reloj; en Kubernetes monolítico suele
            // ser el mismo nodo; en distribuido puede tener sesgo de clock,
            // pero se usa SÓLO como métrica informativa, no para identificar
            // muestras.
            int64_t lat_us = 0;
            if (sample.publish_ts_ns() > 0)
            {
                lat_us = (now_ns() - sample.publish_ts_ns()) / 1'000LL;
            }

            auto obs = flow_.observe(
                sample.origin_writer_id(),
                sample.magnitude_id(),
                sample.sequence_id());

            int64_t rec_ms = flow_.consume_pending_recovery_ms();

            ReportCsv::Row row;
            row.scope_visible  = "raw";
            row.source_writer  = sample.origin_writer_id();
            row.topic          = "air_quality_raw_fwd";
            row.magnitude_id   = sample.magnitude_id();
            row.sequence_id    = sample.sequence_id();
            row.event_type     = "SAMPLE";
            row.latency_mean_us = (lat_us > 0)
                ? std::to_string(lat_us)
                : std::string("");

            if (obs.first_seen)
            {
                row.status = "ok";
                row.notes  = "first sample for this flow";
            }
            else if (obs.gap)
            {
                row.event_type           = "GAP";
                row.lost_messages_detected = std::to_string(obs.lost_messages);
                row.seq_gap_detected     = "1";
                row.status               = "discontinuity";
                std::ostringstream n;
                n << "gap_ms=" << obs.gap_duration_ms;
                row.notes                = n.str();
            }
            else if (obs.out_of_order)
            {
                row.event_type = "OUT_OF_ORDER";
                row.status     = "out_of_order";
            }
            else if (obs.duplicate)
            {
                row.event_type = "OUT_OF_ORDER";
                row.status     = "duplicate";
            }
            else
            {
                row.status = "ok";
            }

            if (rec_ms > 0)
            {
                // Escribimos una fila adicional para el evento RECOVERY.
                ReportCsv::Row rec;
                rec.scope_visible  = "raw";
                rec.source_writer  = sample.origin_writer_id();
                rec.topic          = "air_quality_raw_fwd";
                rec.magnitude_id   = sample.magnitude_id();
                rec.sequence_id    = sample.sequence_id();
                rec.event_type     = "RECOVERY";
                rec.recovery_time_ms = std::to_string(rec_ms);
                rec.status         = "recovered";
                rec.notes          = "first sample after publisher re-match";
                report_.write(rec);
                std::cout << "[RAW][RECOVERY] origin=" << sample.origin_writer_id()
                          << " mag_id=" << sample.magnitude_id()
                          << " recovery_ms=" << rec_ms << "\n";
            }

            report_.write(row);

            std::cout << std::fixed << std::setprecision(3)
                      << "[RAW #" << n << "]"
                      << " station=" << sample.station()
                      << " mag_id="  << sample.magnitude_id()
                      << " seq="     << sample.sequence_id()
                      << " origin="  << sample.origin_writer_id()
                      << " value="   << sample.value()
                      << " lat_us="  << lat_us
                      << " influx_raw=" << influx_.written_raw();

            if (obs.gap)
            {
                std::cout << " [GAP lost=" << obs.lost_messages << "]";
            }
            else if (obs.out_of_order) std::cout << " [OUT_OF_ORDER]";
            else if (obs.duplicate)    std::cout << " [DUPLICATE]";
            else if (obs.first_seen)   std::cout << " [FIRST_SEEN]";
            std::cout << "\n";

            influx_.write_raw(sample);
        }
    }

private:
    InfluxWriter& influx_;
    ReportCsv&    report_;
    FlowTracker&  flow_;
};

// ---------------------------------------------------------------------------
// Listener for air_quality_agg  (AirQualityStats)
// ---------------------------------------------------------------------------

class AggListener : public DataReaderListener
{
public:
    AggListener(InfluxWriter& influx, ReportCsv& report, FlowTracker& flow)
        : influx_(influx), report_(report), flow_(flow) {}

    std::atomic_int      matched_{0};
    std::atomic_uint64_t received_{0};

    void on_subscription_matched(
            DataReader*, const SubscriptionMatchedStatus& info) override
    {
        matched_.store(info.current_count);
        ReportCsv::Row row;
        row.scope_visible = "agg";
        row.topic         = "air_quality_agg";
        if (info.current_count_change == 1)
        {
            std::cout << "[AGG][MATCH] Publisher matched. total=" << info.current_count << "\n";
            row.event_type = "MATCH";
            row.status     = "ok";
            row.notes      = "agg publisher matched";
            flow_.on_rematch();
        }
        else if (info.current_count_change == -1)
        {
            std::cout << "[AGG][MATCH] Publisher unmatched. total=" << info.current_count << "\n";
            row.event_type = "UNMATCH";
            row.status     = "sin muestras";
            row.notes      = "agg publisher unmatched";
            flow_.on_unmatch("-", "air_quality_agg");
        }
        else
        {
            return;
        }
        report_.write(row);
    }

    void on_data_available(DataReader* reader) override
    {
        AirQualityStats sample;
        SampleInfo      info;

        while (reader->take_next_sample(&sample, &info) == RETCODE_OK)
        {
            if (!info.valid_data) continue;

            const auto n = ++received_;

            int64_t lat_us = 0;
            if (sample.publish_ts_ns() > 0)
            {
                lat_us = (now_ns() - sample.publish_ts_ns()) / 1'000LL;
            }

            auto obs = flow_.observe(
                sample.origin_writer_id(),
                sample.magnitude_id(),
                sample.sequence_id());

            int64_t rec_ms = flow_.consume_pending_recovery_ms();

            ReportCsv::Row row;
            row.scope_visible   = "agg";
            row.source_writer   = sample.origin_writer_id();
            row.topic           = "air_quality_agg";
            row.magnitude_id    = sample.magnitude_id();
            row.sequence_id     = sample.sequence_id();
            row.src_first_seq   = std::to_string(sample.src_first_seq());
            row.src_last_seq    = std::to_string(sample.src_last_seq());
            row.src_sample_count = std::to_string(sample.src_sample_count());
            row.event_type      = "SAMPLE";
            row.latency_mean_us = (lat_us > 0) ? std::to_string(lat_us) : "";

            if (obs.first_seen)
            {
                row.status = "ok";
                row.notes  = "first agg snapshot for this flow";
            }
            else if (obs.gap)
            {
                row.event_type           = "GAP";
                row.lost_messages_detected = std::to_string(obs.lost_messages);
                row.seq_gap_detected     = "1";
                row.status               = "discontinuity";
                std::ostringstream nn;
                nn << "gap_ms=" << obs.gap_duration_ms
                   << " kind="  << sample.publish_reason();
                row.notes                = nn.str();
            }
            else if (obs.out_of_order)
            {
                row.event_type = "OUT_OF_ORDER";
                row.status     = "out_of_order";
            }
            else if (obs.duplicate)
            {
                row.event_type = "OUT_OF_ORDER";
                row.status     = "duplicate";
            }
            else
            {
                row.status = "ok";
                row.notes  = "kind=" + std::string(sample.publish_reason());
            }

            if (rec_ms > 0)
            {
                ReportCsv::Row rec;
                rec.scope_visible   = "agg";
                rec.source_writer   = sample.origin_writer_id();
                rec.topic           = "air_quality_agg";
                rec.magnitude_id    = sample.magnitude_id();
                rec.sequence_id     = sample.sequence_id();
                rec.event_type      = "RECOVERY";
                rec.recovery_time_ms = std::to_string(rec_ms);
                rec.status          = "recovered";
                rec.notes           = "first agg snapshot after publisher re-match";
                report_.write(rec);
                std::cout << "[AGG][RECOVERY] origin=" << sample.origin_writer_id()
                          << " mag_id=" << sample.magnitude_id()
                          << " recovery_ms=" << rec_ms << "\n";
            }

            report_.write(row);

            std::cout << std::fixed << std::setprecision(3)
                      << "[AGG #" << n << "]"
                      << " kind="    << sample.publish_reason()
                      << " station=" << sample.station()
                      << " mag_id="  << sample.magnitude_id()
                      << " agg_seq=" << sample.sequence_id()
                      << " src_seq=[" << sample.src_first_seq()
                      << ".." << sample.src_last_seq()
                      << "] src_n=" << sample.src_sample_count()
                      << " mean="    << sample.daily_mean()
                      << " trend="   << sample.trend_label()
                      << " lat_us="  << lat_us;

            if (obs.gap)
            {
                std::cout << " [GAP lost=" << obs.lost_messages << "]";
            }
            else if (obs.out_of_order) std::cout << " [OUT_OF_ORDER]";
            else if (obs.duplicate)    std::cout << " [DUPLICATE]";
            else if (obs.first_seen)   std::cout << " [FIRST_SEEN]";
            std::cout << "\n";

            influx_.write_agg(sample);
        }
    }

private:
    InfluxWriter& influx_;
    ReportCsv&    report_;
    FlowTracker&  flow_;
};

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

class FinalAggSubscriber
{
public:
    FinalAggSubscriber()
        : participant_(nullptr)
        , raw_topic_(nullptr)
        , agg_topic_(nullptr)
        , subscriber_(nullptr)
        , raw_reader_(nullptr)
        , agg_reader_(nullptr)
    {}

    ~FinalAggSubscriber()
    {
        if (subscriber_ && raw_reader_) subscriber_->delete_datareader(raw_reader_);
        if (subscriber_ && agg_reader_) subscriber_->delete_datareader(agg_reader_);
        if (participant_ && subscriber_) participant_->delete_subscriber(subscriber_);
        if (participant_ && raw_topic_)  participant_->delete_topic(raw_topic_);
        if (participant_ && agg_topic_)  participant_->delete_topic(agg_topic_);
        if (participant_)
            DomainParticipantFactory::get_instance()->delete_participant(participant_);
    }

    bool init(const AppConfig& cfg)
    {
        if (!cfg.use_xml)
        {
            std::cerr << "Use -c <profile.xml>\n";
            return false;
        }

        if (!influx_.init(cfg)) return false;

        report_ = std::make_unique<ReportCsv>(cfg.report_csv, cfg.node_id);
        flow_   = std::make_unique<FlowTracker>(*report_);

        raw_listener_ = std::make_unique<RawListener>(influx_, *report_, *flow_);
        agg_listener_ = std::make_unique<AggListener>(influx_, *report_, *flow_);

        DomainParticipantFactory* factory = DomainParticipantFactory::get_instance();

        std::cout << "[INIT] Loading XML: " << cfg.xml_path << "\n";

        if (factory->load_XML_profiles_file(cfg.xml_path) != RETCODE_OK)
        {
            std::cerr << "[INIT] Error loading XML\n";
            return false;
        }

        participant_ = factory->create_participant_with_profile(
            "subscriber_participant_profile");
        if (!participant_)
        {
            std::cerr << "[INIT] DomainParticipant failed\n";
            return false;
        }

        raw_type_.reset(new AirSamplePubSubType());
        agg_type_.reset(new AirQualityStatsPubSubType());
        raw_type_.register_type(participant_);
        agg_type_.register_type(participant_);

        subscriber_ = participant_->create_subscriber_with_profile(
            "subscriber_profile", nullptr);
        if (!subscriber_)
        {
            std::cerr << "[INIT] Subscriber failed\n";
            return false;
        }

        raw_topic_ = participant_->create_topic_with_profile(
            "air_quality_raw_fwd",
            raw_type_.get_type_name(),
            "raw_topic_profile",
            nullptr,
            StatusMask::all());
        if (!raw_topic_)
        {
            std::cerr << "[INIT] raw_topic_ failed\n";
            return false;
        }

        raw_reader_ = subscriber_->create_datareader_with_profile(
            raw_topic_, "raw_reader_profile", raw_listener_.get());
        if (!raw_reader_)
        {
            std::cerr << "[INIT] raw_reader_ failed\n";
            return false;
        }

        agg_topic_ = participant_->create_topic_with_profile(
            "air_quality_agg",
            agg_type_.get_type_name(),
            "agg_topic_profile",
            nullptr,
            StatusMask::all());
        if (!agg_topic_)
        {
            std::cerr << "[INIT] agg_topic_ failed\n";
            return false;
        }

        agg_reader_ = subscriber_->create_datareader_with_profile(
            agg_topic_, "agg_reader_profile", agg_listener_.get());
        if (!agg_reader_)
        {
            std::cerr << "[INIT] agg_reader_ failed\n";
            return false;
        }

        std::cout << "[INIT] FinalAggSubscriber ready on domain "
                  << participant_->get_domain_id()
                  << " | node_id=" << cfg.node_id
                  << " | raw=air_quality_raw_fwd | agg=air_quality_agg\n";
        return true;
    }

    void run()
    {
        std::cout << "[RUN] Listening on both topics (Ctrl+C to stop).\n";
        while (g_running)
            std::this_thread::sleep_for(std::chrono::seconds(1));

        std::cout << "[SHUTDOWN]"
                  << " raw_received=" << raw_listener_->received_.load()
                  << " agg_received=" << agg_listener_->received_.load()
                  << " influx_raw="   << influx_.written_raw()
                  << " influx_agg="   << influx_.written_agg()
                  << "\n";
    }

private:
    DomainParticipant*            participant_;
    Topic*                        raw_topic_;
    Topic*                        agg_topic_;
    Subscriber*                   subscriber_;
    DataReader*                   raw_reader_;
    DataReader*                   agg_reader_;
    TypeSupport                   raw_type_;
    TypeSupport                   agg_type_;
    InfluxWriter                  influx_;
    std::unique_ptr<ReportCsv>    report_;
    std::unique_ptr<FlowTracker>  flow_;
    std::unique_ptr<RawListener>  raw_listener_;
    std::unique_ptr<AggListener>  agg_listener_;
};

} // namespace

int main(int argc, char** argv)
{
    std::signal(SIGINT, signal_handler);

    try
    {
        AppConfig cfg = parse_args(argc, argv);
        FinalAggSubscriber subscriber;
        if (!subscriber.init(cfg)) return 1;
        subscriber.run();
        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        print_usage(argv[0]);
        return 1;
    }
}
