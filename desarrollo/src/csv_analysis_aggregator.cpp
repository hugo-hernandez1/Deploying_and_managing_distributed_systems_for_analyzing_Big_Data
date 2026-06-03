// csv_analysis_aggregator.cpp
//
// Pipeline:
//   DDS topic (air_quality_raw_fwd) [subscriber]
//     └─> detección de gaps por (origin_writer_id, magnitude_id)
//     └─> per-(station,magnitud) analytics
//           ├─> Adaptive Z-score  → stdout alerts + published in AirQualityStats
//           ├─> Hourly values     → feed Trend & Correlation silently
//           ├─> Trend detector    → stdout + published in AirQualityStats
//           ├─> Pearson Corr      → stdout cross-station r
//           └─> Running stats     → published on:
//                 - trend change
//                 - anomaly/spike
//                 - every 12 logical hours
//
//   DDS topic (air_quality_agg) [publisher]
//     └─> snapshot per (station, magnitud) cuando se dispara el evento.
//         Cada snapshot lleva su propio sequence_id por magnitude_id, y
//         preserva el rango src_first_seq..src_last_seq de las AirSample
//         que ha resumido (trazabilidad end-to-end sin depender de
//         timestamps para identificar muestras).

#include <fastdds/dds/core/status/PublicationMatchedStatus.hpp>
#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/core/status/SubscriptionMatchedStatus.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/DataWriterListener.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <tinyxml2.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>


#include "AirSamplePubSubTypes.hpp"
#include "AirQualityStatsPubSubTypes.hpp"

using namespace eprosima::fastdds::dds;

// ============================================================================
// Configuration
// ============================================================================

namespace
{

struct AppConfig
{
    bool        use_xml      = false;
    std::string xml_path;

    std::size_t zscore_window  = 60;
    std::size_t zscore_min     = 10;
    double      zscore_thresh  = 3.0;
    double      spike_thresh   = 4.5;

    std::size_t trend_window   = 6;
    double      stable_eps     = 0.01;

    std::size_t corr_window    = 24;
    std::size_t corr_min_pairs = 5;

    std::string gap_csv        = "analysis_aggregator_gaps.csv";
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
    return "csv_analysis_aggregator";
}

long long now_ns()
{
    using namespace std::chrono;
    return duration_cast<nanoseconds>(
        system_clock::now().time_since_epoch()).count();
}

void print_usage(const char* p)
{
    std::cout
        << "Data Analysis Aggregator – Z-score + trend + running stats + DDS publish\n\n"
        << "Usage: " << p << " -c profile.xml [options]\n\n"
        << "  -c <path>      XML QoS profile (required)\n"
        << "  --zw   <N>     Z-score window (raw) (default: 60)\n"
        << "  --zmin <N>     Z-score min samples  (default: 10)\n"
        << "  --zt   <f>     Z-score alert thresh (default: 3.0)\n"
        << "  --st   <f>     Spike threshold      (default: 4.5)\n"
        << "  --tw   <N>     Trend window (hours) (default: 6)\n"
        << "  --eps  <f>     Stable epsilon       (default: 0.01)\n"
        << "  --cw   <N>     Corr window (hours)  (default: 24)\n"
        << "  --cmin <N>     Corr min pairs       (default: 5)\n"
        << "  --gap-csv <p>  CSV for gap events   (default: analysis_aggregator_gaps.csv)\n"
        << "  --node-id <s>  Node identifier      (default: $NODE_ID/$HOSTNAME)\n";
}
static std::string read_topic_name_from_xml(
    const std::string& xml_path,
    const std::string& profile_name)
{
    tinyxml2::XMLDocument doc;
    if (doc.LoadFile(xml_path.c_str()) != tinyxml2::XML_SUCCESS)
        return "";

    auto* dds = doc.FirstChildElement("dds");
    if (!dds) return "";
    auto* profiles = dds->FirstChildElement("profiles");
    if (!profiles) return "";

    for (auto* topic = profiles->FirstChildElement("topic");
         topic != nullptr;
         topic = topic->NextSiblingElement("topic"))
    {
        const char* pname = topic->Attribute("profile_name");
        if (pname && std::string(pname) == profile_name)
        {
            auto* name_el = topic->FirstChildElement("name");
            if (name_el && name_el->GetText())
                return name_el->GetText();
        }
    }
    return "";
}
AppConfig parse_args(int argc, char** argv)
{
    AppConfig c;
    std::vector<std::string> extra;
    auto need = [&](int i, int argc_, const char* flag)
    {
        if (i >= argc_) throw std::runtime_error(std::string("Missing value after ") + flag);
    };

    for (int i = 1; i < argc; ++i)
    {
        std::string a = argv[i];
        if      (a == "-h" || a == "--help") { print_usage(argv[0]); std::exit(0); }
        else if (a == "-c")    { need(i+1,argc,"-c");     c.use_xml=true; c.xml_path=argv[++i]; }
        else if (a == "--zw")  { need(i+1,argc,"--zw");   c.zscore_window=std::stoul(argv[++i]); }
        else if (a == "--zmin"){ need(i+1,argc,"--zmin"); c.zscore_min=std::stoul(argv[++i]); }
        else if (a == "--zt")  { need(i+1,argc,"--zt");   c.zscore_thresh=std::stod(argv[++i]); }
        else if (a == "--st")  { need(i+1,argc,"--st");   c.spike_thresh=std::stod(argv[++i]); }
        else if (a == "--tw")  { need(i+1,argc,"--tw");   c.trend_window=std::stoul(argv[++i]); }
        else if (a == "--eps") { need(i+1,argc,"--eps");  c.stable_eps=std::stod(argv[++i]); }
        else if (a == "--cw")  { need(i+1,argc,"--cw");   c.corr_window=std::stoul(argv[++i]); }
        else if (a == "--cmin"){ need(i+1,argc,"--cmin"); c.corr_min_pairs=std::stoul(argv[++i]); }
        else if (a == "--gap-csv") { need(i+1,argc,"--gap-csv"); c.gap_csv = argv[++i]; }
        else if (a == "--node-id") { need(i+1,argc,"--node-id"); c.node_id = argv[++i]; }
        else extra.push_back(a);
    }
    if (!extra.empty()) throw std::runtime_error("Unrecognized argument: " + extra[0]);
    if (c.node_id.empty()) c.node_id = default_node_id();
    return c;
}

// ============================================================================
// Domain keys
// ============================================================================

struct SK
{
    std::string station;
    int32_t     magnitud;
    bool operator<(const SK& o) const
    {
        return station < o.station || (station == o.station && magnitud < o.magnitud);
    }
};

struct HourKey
{
    int32_t year, month, day, hour;
    bool operator<(const HourKey& o) const
    {
        return std::tie(year,month,day,hour) < std::tie(o.year,o.month,o.day,o.hour);
    }
    bool operator==(const HourKey& o) const
    {
        return year==o.year && month==o.month && day==o.day && hour==o.hour;
    }
};

struct DayKey
{
    int32_t year, month, day;
    bool operator==(const DayKey& o) const
    {
        return year==o.year && month==o.month && day==o.day;
    }
    bool operator!=(const DayKey& o) const
    {
        return !(*this == o);
    }
};

static HourKey make_hk(const AirSample& s) { return {s.year(), s.month(), s.day(), s.hour()}; }
static DayKey  make_dk(const AirSample& s) { return {s.year(), s.month(), s.day()}; }

static int hour_index(const HourKey& hk)
{
    return (((hk.year * 12 + hk.month) * 31 + hk.day) * 24 + hk.hour);
}

static int hour_delta(const HourKey& a, const HourKey& b)
{
    return hour_index(a) - hour_index(b);
}

static std::string wall_clock_now()
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

// ============================================================================
// Gap tracker (entrada: detección de pérdidas por discontinuidad de seq)
// ============================================================================

class GapTracker
{
public:
    GapTracker(const std::string& path,
               const std::string& node_id,
               const std::string& scope_topic)
        : path_(path), node_id_(node_id), scope_topic_(scope_topic)
    {
        file_.open(path_, std::ios::app | std::ios::binary);
        if (!file_.is_open())
        {
            std::cerr << "[GAP] Cannot open gap CSV: " << path_ << "\n";
            return;
        }
        file_.seekp(0, std::ios::end);
        if (file_.tellp() == 0)
        {
            file_ << "event_ts,node_id,scope_topic,event_type,"
                     "origin_writer_id,magnitude_id,"
                     "prev_seq,curr_seq,lost_messages,"
                     "gap_duration_ms,notes\n";
        }
        std::cout << "[GAP] Gap/recovery CSV: " << path_ << "\n";
    }

    struct Observation
    {
        bool    in_order     = true;
        bool    gap          = false;
        bool    duplicate    = false;
        bool    out_of_order = false;
        bool    first_seen   = false;
        int64_t lost_messages = 0;
        int64_t gap_duration_ms = 0;
    };

    Observation observe(const std::string& origin_writer_id,
                        int32_t magnitude_id,
                        int64_t seq)
    {
        Observation obs;
        Key k{origin_writer_id, magnitude_id};
        auto wall = now_ns();

        std::lock_guard<std::mutex> lk(mu_);
        auto it = state_.find(k);
        if (it == state_.end())
        {
            obs.first_seen = true;
            state_[k] = State{seq, wall};
            write_event("FIRST_SEEN", k, 0, seq, 0, 0, "first sample for this flow");
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
            obs.lost_messages = seq - last - 1;
            obs.gap_duration_ms =
                (wall - st.last_seen_ns) / 1'000'000LL;
            write_event("GAP", k, last, seq, obs.lost_messages,
                        obs.gap_duration_ms, "discontinuity detected");
            st.last_seq = seq;
            st.last_seen_ns = wall;
            return obs;
        }
        obs.in_order = false;
        obs.out_of_order = true;

        if (seq == last)
        {
            obs.duplicate = true;
        }

        std::string note = obs.duplicate ? "duplicate"
                                        : "out-of-order or writer restart";

        write_event("OUT_OF_ORDER", k, last, seq, 0, 0, note);
return obs;
    }

    void report_recovery(const std::string& origin_writer_id,
                         int32_t magnitude_id,
                         int64_t recovery_ms,
                         const std::string& notes)
    {
        Key k{origin_writer_id, magnitude_id};
        std::lock_guard<std::mutex> lk(mu_);
        write_event("RECOVERY", k, 0, 0, 0, recovery_ms, notes);
    }

private:
    struct Key
    {
        std::string origin_writer_id;
        int32_t     magnitude_id;
        bool operator<(const Key& o) const
        {
            if (origin_writer_id != o.origin_writer_id)
                return origin_writer_id < o.origin_writer_id;
            return magnitude_id < o.magnitude_id;
        }
    };
    struct State { int64_t last_seq = 0; int64_t last_seen_ns = 0; };

    void write_event(const char* type, const Key& k,
                     int64_t prev, int64_t curr,
                     int64_t lost, int64_t gap_ms,
                     const std::string& notes)
    {
        if (!file_.is_open()) return;
        file_ << wall_clock_now() << ','
              << escape(node_id_)     << ','
              << escape(scope_topic_) << ','
              << type                 << ','
              << escape(k.origin_writer_id) << ','
              << k.magnitude_id       << ','
              << prev                 << ','
              << curr                 << ','
              << lost                 << ','
              << gap_ms               << ','
              << escape(notes)        << '\n';
        file_.flush();
    }
    static std::string escape(const std::string& s)
    {
        if (s.find_first_of(",\"\n") == std::string::npos) return s;
        std::string out = "\"";
        for (char c : s) { if (c == '"') out += '"'; out += c; }
        out += '"';
        return out;
    }

    std::string path_, node_id_, scope_topic_;
    std::ofstream file_;
    std::mutex mu_;
    std::map<Key, State> state_;
};

// ============================================================================
// Accumulators
// ============================================================================

struct DailyAccum
{
    DayKey      day     = {0,0,0};
    double      sum     = 0.0;
    double      min_val = std::numeric_limits<double>::max();
    double      max_val = std::numeric_limits<double>::lowest();
    std::size_t count   = 0;

    void add(double v)
    {
        sum += v;
        if (v < min_val) min_val = v;
        if (v > max_val) max_val = v;
        ++count;
    }

    double mean() const
    {
        return count > 0 ? sum / count : 0.0;
    }

    void reset(const DayKey& dk)
    {
        day = dk;
        sum = 0.0;
        min_val = std::numeric_limits<double>::max();
        max_val = std::numeric_limits<double>::lowest();
        count = 0;
    }
};

struct HourAccum
{
    double      sum   = 0.0;
    std::size_t count = 0;

    void add(double v) { sum += v; ++count; }
    double mean() const { return count > 0 ? sum / count : 0.0; }
};

// Rango de sequence_id de las AirSample que han contribuido al acumulador
// diario de una (station, magnitud). Se reinicia al cambiar de día, igual
// que DailyAccum.
struct SeqRange
{
    bool    has_any  = false;
    int64_t first    = 0;
    int64_t last     = 0;
    int32_t count    = 0;

    void add(int64_t seq)
    {
        if (!has_any) { first = last = seq; has_any = true; }
        else
        {
            if (seq < first) first = seq;
            if (seq > last)  last  = seq;
        }
        ++count;
    }
    void reset()
    {
        has_any = false; first = 0; last = 0; count = 0;
    }
};

// ============================================================================
// States: Z-Score, Trend, Correlation
// ============================================================================

struct ZScoreState
{
    std::deque<double> window;
    std::size_t        max_size = 0;

    void push(double v)
    {
        window.push_back(v);
        if (window.size() > max_size) window.pop_front();
    }

    std::tuple<double,double,double,bool> compute(double v, std::size_t min_n) const
    {
        if (window.size() < min_n) return {0,0,0,false};

        double mu = std::accumulate(window.begin(), window.end(), 0.0) / window.size();
        double var = 0.0;
        for (double x : window) var += (x - mu) * (x - mu);
        double sigma = std::sqrt(var / window.size());

        if (sigma < 1e-9) return {0,mu,sigma,false};
        return {(v - mu) / sigma, mu, sigma, true};
    }
};

enum class Trend { STABLE, RISING, FALLING, SPIKE };

static const char* trend_str(Trend t)
{
    switch (t)
    {
        case Trend::STABLE:  return "STABLE";
        case Trend::RISING:  return "RISING";
        case Trend::FALLING: return "FALLING";
        case Trend::SPIKE:   return "SPIKE";
    }
    return "UNKNOWN";
}

struct TrendState
{
    std::deque<double> means;
    std::size_t        max_size   = 0;
    double             stable_eps = 0.01;

    void push_mean(double m)
    {
        means.push_back(m);
        if (means.size() > max_size) means.pop_front();
    }

    std::pair<double,bool> ols_slope() const
    {
        if (means.size() < 2) return {0, false};

        std::size_t n = means.size();
        double sx=0, sy=0, sxy=0, sx2=0;

        for (std::size_t i = 0; i < n; ++i)
        {
            double xi = static_cast<double>(i);
            sx += xi;
            sy += means[i];
            sxy += xi * means[i];
            sx2 += xi * xi;
        }

        double d = n * sx2 - sx * sx;
        if (std::abs(d) < 1e-12) return {0, false};

        return {(n * sxy - sx * sy) / d, true};
    }

    Trend classify(double z, double spike_thresh) const
    {
        if (std::abs(z) > spike_thresh) return Trend::SPIKE;

        auto [slope, valid] = ols_slope();
        if (!valid)              return Trend::STABLE;
        if (slope >  stable_eps) return Trend::RISING;
        if (slope < -stable_eps) return Trend::FALLING;
        return Trend::STABLE;
    }
};

struct CorrState
{
    std::map<std::string, std::deque<std::pair<HourKey,double>>> history;
    std::size_t max_window  = 0;
    std::size_t min_pairs   = 0;

    void push(const std::string& station, const HourKey& hk, double mean)
    {
        auto& dq = history[station];
        dq.push_back({hk, mean});
        if (dq.size() > max_window) dq.pop_front();
    }

    static double pearson(
        const std::deque<std::pair<HourKey,double>>& a,
        const std::deque<std::pair<HourKey,double>>& b,
        std::size_t min_n)
    {
        std::map<HourKey,double> ma;
        for (const auto& [hk,v] : a) ma[hk] = v;

        std::vector<double> xa, xb;
        for (const auto& [hk,v] : b)
        {
            auto it = ma.find(hk);
            if (it != ma.end())
            {
                xa.push_back(it->second);
                xb.push_back(v);
            }
        }

        if (xa.size() < min_n) return std::numeric_limits<double>::quiet_NaN();

        double n   = static_cast<double>(xa.size());
        double ma_ = std::accumulate(xa.begin(), xa.end(), 0.0) / n;
        double mb_ = std::accumulate(xb.begin(), xb.end(), 0.0) / n;

        double num = 0.0, da = 0.0, db = 0.0;
        for (std::size_t i = 0; i < xa.size(); ++i)
        {
            double ai = xa[i] - ma_;
            double bi = xb[i] - mb_;
            num += ai * bi;
            da  += ai * ai;
            db  += bi * bi;
        }

        double denom = std::sqrt(da * db);
        if (denom < 1e-12) return std::numeric_limits<double>::quiet_NaN();

        return num / denom;
    }

    std::vector<std::tuple<std::string,std::string,double,std::size_t>> all_pairs() const
    {
        std::vector<std::tuple<std::string,std::string,double,std::size_t>> out;
        std::vector<std::string> stations;
        for (const auto& [s,_] : history) stations.push_back(s);

        for (std::size_t i = 0; i < stations.size(); ++i)
        {
            for (std::size_t j = i + 1; j < stations.size(); ++j)
            {
                const auto& da = history.at(stations[i]);
                const auto& db = history.at(stations[j]);
                double r = pearson(da, db, min_pairs);

                if (!std::isnan(r))
                {
                    std::map<HourKey,double> tmp;
                    for (const auto& [hk,v] : da) tmp[hk] = v;

                    std::size_t n = 0;
                    for (const auto& [hk,_] : db)
                    {
                        if (tmp.count(hk)) ++n;
                    }

                    out.emplace_back(stations[i], stations[j], r, n);
                }
            }
        }

        return out;
    }
};

// ============================================================================
// Pending stats per (station, magnitud)
// ============================================================================

struct PendingStats
{
    double  last_z       = 0.0;
    double  last_mu      = 0.0;
    double  last_sigma   = 0.0;
    bool    last_z_valid = false;

    double  last_slope   = 0.0;
    bool    slope_valid  = false;
    Trend   last_trend   = Trend::STABLE;

    bool    trend_initialized   = false;
    Trend   last_published_trend = Trend::STABLE;

    bool    has_last_pub_hour = false;
    HourKey last_pub_hour{0,0,0,0};

    bool    has_last_log_hour = false;
    HourKey last_log_hour{0,0,0,0};
};

// ============================================================================
// Analytics engine
// ============================================================================

class StatsWriter;

class AnalyticsEngine
{
public:
    AnalyticsEngine(const AppConfig& cfg, GapTracker& gaps)
        : cfg_(cfg), gaps_(gaps), node_id_(cfg.node_id)
    {
    }

    void set_writer(StatsWriter* w)
    {
        writer_ = w;
    }

    void ingest(const AirSample& s)
    {
        double  val = s.value();
        SK      sk  { s.station(), s.magnitud() };
        HourKey hk  = make_hk(s);
        DayKey  dk  = make_dk(s);

        // Detección de gaps (entrada) por (origin_writer_id, magnitude_id).
        auto obs = gaps_.observe(
            s.origin_writer_id(),
            s.magnitude_id(),
            s.sequence_id());

        // Skip duplicates and out-of-order samples: do not aggregate
        // them into the statistics nor consider them for trend/Z-score.
        if (obs.duplicate || obs.out_of_order)
        {
            std::cout << "[SKIP] origin=" << s.origin_writer_id()
                      << " mag_id=" << s.magnitude_id()
                      << " seq=" << s.sequence_id()
                      << (obs.duplicate ? " [DUPLICATE]" : " [OUT_OF_ORDER]")
                      << "\n";
            return;
        }

        if (obs.gap)
        {
            std::cout << "[INPUT_GAP] origin=" << s.origin_writer_id()
                      << " mag_id=" << s.magnitude_id()
                      << " lost=" << obs.lost_messages
                      << " gap_ms=" << obs.gap_duration_ms << "\n";
        }

        std::lock_guard<std::mutex> lk(mu_);

        // Actualizamos rango de secuencias fuente para este (station,mag)
        seq_range_[sk].add(s.sequence_id());

        // ── Z-score ──────────────────────────────────────────────────────────
        auto& zs = zscore_[sk];
        zs.max_size = cfg_.zscore_window;
        auto [z, mu, sigma, z_valid] = zs.compute(val, cfg_.zscore_min);
        zs.push(val);

        auto& ps = pending_[sk];
        ps.last_z       = z;
        ps.last_mu      = mu;
        ps.last_sigma   = sigma;
        ps.last_z_valid = z_valid;

        // ── Running daily accumulator ────────────────────────────────────────
        auto& d_acc = daily_stats_[sk];
        if (d_acc.count > 0 && d_acc.day != dk)
        {
            d_acc.reset(dk);
            // Al cambiar de día reiniciamos también la ventana de seq que
            // estamos resumiendo.
            seq_range_[sk].reset();
            seq_range_[sk].add(s.sequence_id());
        }
        else if (d_acc.count == 0)
        {
            d_acc.day = dk;
        }
        d_acc.add(val);

        if (should_log_3h(ps, hk))
        {
            std::cout << std::fixed << std::setprecision(3)
                << "[RUNNING] st=" << sk.station
                << " mag=" << sk.magnitud
                << " " << dk.year << "-" << dk.month << "-" << dk.day
                << "T" << hk.hour << "h"
                << " mean_so_far=" << d_acc.mean()
                << " min="  << d_acc.min_val
                << " max="  << d_acc.max_val
                << " n="    << d_acc.count << "\n";

            ps.has_last_log_hour = true;
            ps.last_log_hour = hk;
        }

        hourly_[sk][hk].add(val);

        auto& cur = current_hk_[sk];
        if (cur.year != 0 && !(cur == hk))
        {
            close_hour(sk, cur, ps);
        }
        cur = hk;

        // ── Anomaly / periodic publish ───────────────────────────────────────
        if (z_valid && std::abs(z) > cfg_.zscore_thresh)
        {
            auto& ts = trend_[sk];
            ts.max_size   = cfg_.trend_window;
            ts.stable_eps = cfg_.stable_eps;
            Trend tr = ts.classify(z, cfg_.spike_thresh);

            const char* level = (std::abs(z) > cfg_.spike_thresh) ? "SPIKE" : "ANOMALY";

            std::cout << std::fixed << std::setprecision(3)
                << "[ALERT] " << level
                << " | trend=" << trend_str(tr)
                << " | st=" << sk.station
                << " mag=" << sk.magnitud
                << " " << hk.year << "-" << hk.month << "-" << hk.day
                << "T" << hk.hour << "h"
                << " val=" << val
                << " z=" << z
                << " mu=" << mu
                << " sig=" << sigma << "\n";

            publish_snapshot(sk, dk, d_acc, ps, hk, level);
            ps.has_last_pub_hour = true;
            ps.last_pub_hour = hk;
        }
        else if (should_publish_12h(ps, hk))
        {
            publish_snapshot(sk, dk, d_acc, ps, hk, "PERIODIC_12H");
            ps.has_last_pub_hour = true;
            ps.last_pub_hour = hk;
        }
    }

    void flush_all()
    {
        std::lock_guard<std::mutex> lk(mu_);

        for (auto& [sk, hk] : current_hk_)
        {
            if (hk.year != 0)
            {
                close_hour(sk, hk, pending_[sk]);
            }
        }

        for (auto& [sk, d_acc] : daily_stats_)
        {
            if (d_acc.count > 0)
            {
                HourKey hk = current_hk_.count(sk)
                    ? current_hk_[sk]
                    : HourKey{d_acc.day.year, d_acc.day.month, d_acc.day.day, 0};

                publish_snapshot(sk, d_acc.day, d_acc, pending_[sk], hk, "SHUTDOWN_FLUSH");
            }
        }
    }

private:
    bool should_publish_12h(const PendingStats& ps, const HourKey& hk) const
    {
        if (!ps.has_last_pub_hour) return true;
        return hour_delta(hk, ps.last_pub_hour) >= 12;
    }

    bool should_log_3h(const PendingStats& ps, const HourKey& hk) const
    {
        if (!ps.has_last_log_hour) return true;
        return hour_delta(hk, ps.last_log_hour) >= 3;
    }

    void close_hour(const SK& sk, const HourKey& hk, PendingStats& ps)
    {
        auto it_sk = hourly_.find(sk);
        if (it_sk == hourly_.end()) return;

        auto it = it_sk->second.find(hk);
        if (it == it_sk->second.end()) return;

        double m = it->second.mean();

        auto& ts = trend_[sk];
        ts.max_size   = cfg_.trend_window;
        ts.stable_eps = cfg_.stable_eps;
        ts.push_mean(m);

        auto [slope, s_valid] = ts.ols_slope();
        ps.last_slope  = slope;
        ps.slope_valid = s_valid;

        Trend prev_trend = ps.last_trend;
        ps.last_trend = ts.classify(0.0, cfg_.spike_thresh);

        if (s_valid)
        {
            std::cout << std::fixed << std::setprecision(5)
                << "[TREND] st=" << sk.station
                << " mag=" << sk.magnitud
                << " slope=" << slope
                << " -> " << trend_str(ps.last_trend) << "\n";
        }

        bool trend_changed = false;
        if (!ps.trend_initialized)
        {
            ps.trend_initialized = true;
            ps.last_published_trend = ps.last_trend;
        }
        else if (ps.last_trend != ps.last_published_trend)
        {
            trend_changed = true;
        }

        if (trend_changed)
        {
            auto d_it = daily_stats_.find(sk);
            if (d_it != daily_stats_.end() && d_it->second.count > 0)
            {
                publish_snapshot(sk, d_it->second.day, d_it->second, ps, hk, "TREND_CHANGE");
                ps.has_last_pub_hour = true;
                ps.last_pub_hour = hk;
            }
            ps.last_published_trend = ps.last_trend;
        }

        auto& cs = corr_[sk.magnitud];
        cs.max_window = cfg_.corr_window;
        cs.min_pairs  = cfg_.corr_min_pairs;
        cs.push(sk.station, hk, m);

        for (const auto& [stA, stB, r, n] : cs.all_pairs())
        {
            std::cout << std::fixed << std::setprecision(4)
                << "[CORR] mag=" << sk.magnitud
                << " " << stA << " <-> " << stB
                << " r=" << r << " n=" << n << "\n";
        }

        it_sk->second.erase(it);
    }

    void publish_snapshot(
        const SK& sk,
        const DayKey& dk,
        const DailyAccum& d_acc,
        const PendingStats& ps,
        const HourKey& hk,
        const std::string& reason);

    const AppConfig& cfg_;
    GapTracker&      gaps_;
    std::string      node_id_;
    std::mutex       mu_;
    StatsWriter*     writer_ = nullptr;

    std::map<SK, ZScoreState>                  zscore_;
    std::map<SK, TrendState>                   trend_;
    std::map<SK, DailyAccum>                   daily_stats_;
    std::map<SK, PendingStats>                 pending_;
    std::map<SK, std::map<HourKey,HourAccum>>  hourly_;
    std::map<SK, HourKey>                      current_hk_;
    std::map<int32_t, CorrState>               corr_;

    // Rango de sequence_id (entrada) por (station, magnitud) que se está
    // resumiendo actualmente en el DailyAccum correspondiente.
    std::map<SK, SeqRange>                     seq_range_;

    // sequence_id monotónico de salida por magnitud_id del flujo agregado.
    std::map<int32_t, int64_t>                 agg_seq_per_magnitude_;

    friend class AnalyticsEnginePublishAccess;

public:
    // Helper para que publish_snapshot acceda a la secuencia de salida.
    int64_t next_agg_seq(int32_t magnitude_id)
    {
        return ++agg_seq_per_magnitude_[magnitude_id];
    }
    const SeqRange& seq_range_for(const SK& sk)
    {
        return seq_range_[sk];
    }
    const std::string& node_id() const { return node_id_; }
};

// ============================================================================
// DDS publisher for AirQualityStats
// ============================================================================

class StatsWriterListener : public DataWriterListener
{
public:
    std::atomic_int matched_{0};

    void on_publication_matched(DataWriter*, const PublicationMatchedStatus& info) override
    {
        matched_.store(info.current_count);
        if      (info.current_count_change ==  1) std::cout << "[PUB] Subscriber matched. total="   << info.current_count << "\n";
        else if (info.current_count_change == -1) std::cout << "[PUB] Subscriber unmatched. total=" << info.current_count << "\n";
    }
};

class StatsWriter
{
public:
    StatsWriter() = default;

    bool init(DomainParticipant* participant, const std::string& topic_name)
    {
        type_.reset(new AirQualityStatsPubSubType());
        type_.register_type(participant);

        publisher_ = participant->create_publisher_with_profile(
            "data_analysis_publisher_profile", nullptr);
        if (!publisher_)
        {
            publisher_ = participant->create_publisher(PUBLISHER_QOS_DEFAULT, nullptr);
        }
        if (!publisher_)
        {
            std::cerr << "[PUB] Failed to create Publisher\n";
            return false;
        }

        topic_ = participant->create_topic_with_profile(
            topic_name,
            type_.get_type_name(),
            "data_analysis_output_topic_profile",
            nullptr,
            StatusMask::none());
        if (!topic_)
        {
            TopicQos tqos = TOPIC_QOS_DEFAULT;
            topic_ = participant->create_topic(topic_name, type_.get_type_name(), tqos);
        }
        if (!topic_)
        {
            std::cerr << "[PUB] Failed to create Topic: " << topic_name << "\n";
            return false;
        }

        writer_ = publisher_->create_datawriter_with_profile(
            topic_, "data_analysis_writer_profile", &listener_);
        if (!writer_)
        {
            DataWriterQos wqos = DATAWRITER_QOS_DEFAULT;
            wqos.reliability().kind = RELIABLE_RELIABILITY_QOS;
            wqos.durability().kind  = VOLATILE_DURABILITY_QOS;
            wqos.history().kind     = KEEP_LAST_HISTORY_QOS;
            wqos.history().depth    = 100;
            writer_ = publisher_->create_datawriter(topic_, wqos, &listener_);
        }
        if (!writer_)
        {
            std::cerr << "[PUB] Failed to create DataWriter\n";
            return false;
        }

        std::cout << "[PUB] StatsWriter ready on topic: " << topic_name << "\n";
        return true;
    }

    void cleanup(DomainParticipant* participant)
    {
        if (publisher_ && writer_)      publisher_->delete_datawriter(writer_);
        if (participant && publisher_)  participant->delete_publisher(publisher_);
        if (participant && topic_)      participant->delete_topic(topic_);
        writer_    = nullptr;
        publisher_ = nullptr;
        topic_     = nullptr;
    }

    bool publish(const AirQualityStats& sample)
    {
        if (!writer_) return false;

        bool ok = (writer_->write(const_cast<AirQualityStats*>(&sample)) == RETCODE_OK);
        if (ok)
        {
            std::cout << std::fixed << std::setprecision(3)
                << "[PUB] Published AirQualityStats"
                << " st=" << sample.station()
                << " mag=" << sample.magnitud()
                << " agg_seq=" << sample.sequence_id()
                << " src_seq=[" << sample.src_first_seq()
                << ".." << sample.src_last_seq()
                << "] src_n=" << sample.src_sample_count()
                << " mean=" << sample.daily_mean()
                << " trend=" << sample.trend_label()
                << " anomaly=" << (sample.is_anomaly() ? "YES" : "no")
                << "\n";
        }

        return ok;
    }

private:
    TypeSupport         type_;
    Publisher*          publisher_ = nullptr;
    Topic*              topic_     = nullptr;
    DataWriter*         writer_    = nullptr;
    StatsWriterListener listener_;
};

// ============================================================================
// Publish snapshot implementation
// ============================================================================

void AnalyticsEngine::publish_snapshot(
    const SK& sk,
    const DayKey& dk,
    const DailyAccum& d_acc,
    const PendingStats& ps,
    const HourKey& hk,
    const std::string& reason)
{
    AirQualityStats out;
    out.station(sk.station);
    out.magnitud(sk.magnitud);
    out.year(dk.year);
    out.month(dk.month);
    out.day(dk.day);
    out.hour(hk.hour);
    out.daily_mean(d_acc.mean());
    out.daily_min(d_acc.count > 0 ? d_acc.min_val : 0.0);
    out.daily_max(d_acc.count > 0 ? d_acc.max_val : 0.0);
    out.daily_count(static_cast<int32_t>(d_acc.count));
    out.trend_slope(ps.last_slope);
    out.trend_label(trend_str(ps.last_trend));
    out.zscore(ps.last_z);
    out.zscore_mu(ps.last_mu);
    out.zscore_sigma(ps.last_sigma);
    out.zscore_valid(ps.last_z_valid);
    bool z_based = (reason == "SPIKE" || reason == "ANOMALY");
    out.is_anomaly(z_based && ps.last_z_valid && std::abs(ps.last_z) > cfg_.zscore_thresh);
    out.is_spike(z_based && ps.last_z_valid && std::abs(ps.last_z) > cfg_.spike_thresh);
    out.publish_reason(reason);
    out.emitted_at(wall_clock_now());

    // Trazabilidad del flujo agregado.
    const int32_t agg_mag_id = sk.magnitud;
    int64_t agg_seq = ++agg_seq_per_magnitude_[agg_mag_id];
    out.magnitude_id(agg_mag_id);
    out.sequence_id(agg_seq);
    out.origin_writer_id(node_id_);
    out.publish_ts_ns(now_ns());

    // Rango de muestras crudas que este snapshot resume.
    const auto& sr = seq_range_[sk];
    out.src_first_seq(sr.has_any ? sr.first : 0);
    out.src_last_seq(sr.has_any  ? sr.last  : 0);
    out.src_sample_count(sr.count);

    if (writer_ && writer_->publish(out))
    {
        std::cout << std::fixed << std::setprecision(3)
            << "[PUBLISH] kind=" << reason
            << " st=" << sk.station
            << " mag=" << sk.magnitud
            << " agg_seq=" << agg_seq
            << " " << dk.year << "-" << dk.month << "-" << dk.day
            << "T" << hk.hour << "h"
            << " mean=" << d_acc.mean()
            << " n=" << d_acc.count
            << " src_seq=[" << (sr.has_any ? sr.first : 0)
            << ".." << (sr.has_any ? sr.last : 0)
            << "] src_n=" << sr.count
            << "\n";
    }
}

// ============================================================================
// DDS listener (subscriber side – air_quality_raw_fwd)
// ============================================================================

class AnalyticsListener : public DataReaderListener
{
public:
    explicit AnalyticsListener(AnalyticsEngine& e)
        : engine_(e)
    {
    }

    std::atomic_int matched_{0};

    void on_subscription_matched(DataReader*, const SubscriptionMatchedStatus& info) override
    {
        matched_.store(info.current_count);
        if      (info.current_count_change ==  1) std::cout << "[SUB] Publisher matched. total="   << info.current_count << "\n";
        else if (info.current_count_change == -1) std::cout << "[SUB] Publisher unmatched. total=" << info.current_count << "\n";
    }

    void on_data_available(DataReader* reader) override
    {
        AirSample  sample;
        SampleInfo info;

        while (reader->take_next_sample(&sample, &info) == RETCODE_OK)
        {
            if (info.valid_data)
            {
                engine_.ingest(sample);
            }
        }
    }

private:
    AnalyticsEngine& engine_;
};

// ============================================================================
// Application
// ============================================================================

class DataAnalysisAggregator
{
public:
    DataAnalysisAggregator()
        : participant_(nullptr)
        , sub_topic_(nullptr)
        , subscriber_(nullptr)
        , reader_(nullptr)
    {
    }

    ~DataAnalysisAggregator()
    {
        if (subscriber_ && reader_)      subscriber_->delete_datareader(reader_);
        if (participant_ && subscriber_) participant_->delete_subscriber(subscriber_);
        stats_writer_.cleanup(participant_);
        if (participant_ && sub_topic_)  participant_->delete_topic(sub_topic_);
        if (participant_)                DomainParticipantFactory::get_instance()->delete_participant(participant_);
    }

    bool init(const AppConfig& cfg)
    {
        if (!cfg.use_xml)
        {
            std::cerr << "[INIT] Use -c <profile.xml>\n";
            return false;
        }

        auto* factory = DomainParticipantFactory::get_instance();
        in_type_.reset(new AirSamplePubSubType());

        std::cout << "[INIT] Loading XML: " << cfg.xml_path << "\n";
        if (factory->load_XML_profiles_file(cfg.xml_path) != RETCODE_OK)
        {
            std::cerr << "[INIT] Error loading XML\n";
            return false;
        }

        participant_ = factory->create_participant_with_profile(
            "data_analysis_intermediate_participant_profile");
        if (!participant_)
        {
            std::cerr << "[INIT] DomainParticipant failed\n";
            return false;
        }

        in_type_.register_type(participant_);

        std::string input_topic = read_topic_name_from_xml(
            cfg.xml_path, "data_analysis_intermediate_topic_profile");
        if (input_topic.empty())
        {
            std::cerr << "[INIT] No se pudo leer el topic de entrada del XML "
                         "(profile: data_analysis_intermediate_topic_profile)\n";
            return false;
        }

        std::string output_topic = read_topic_name_from_xml(
            cfg.xml_path, "data_analysis_output_topic_profile");
        if (output_topic.empty())
        {
            std::cerr << "[INIT] No se pudo leer el topic de salida del XML "
                         "(profile: data_analysis_output_topic_profile)\n";
            return false;
        }

        std::cout << "[INIT] Topic entrada: " << input_topic
                  << " | Topic salida: " << output_topic
                  << " | node_id: " << cfg.node_id << "\n";

        gaps_    = std::make_unique<GapTracker>(cfg.gap_csv, cfg.node_id, input_topic);
        engine_  = std::make_unique<AnalyticsEngine>(cfg, *gaps_);
        listener_ = std::make_unique<AnalyticsListener>(*engine_);

        sub_topic_ = participant_->create_topic_with_profile(
            input_topic,
            in_type_.get_type_name(),
            "data_analysis_intermediate_topic_profile",
            nullptr,
            StatusMask::all());
        if (!sub_topic_)
        {
            std::cerr << "[INIT] Subscriber Topic failed\n";
            return false;
        }

        subscriber_ = participant_->create_subscriber_with_profile(
            "data_analysis_intermediate_subscriber_profile");
        if (!subscriber_)
        {
            std::cerr << "[INIT] Subscriber failed\n";
            return false;
        }

        reader_ = subscriber_->create_datareader_with_profile(
            sub_topic_,
            "data_analysis_intermediate_reader_profile",
            listener_.get());
        if (!reader_)
        {
            std::cerr << "[INIT] DataReader failed\n";
            return false;
        }

        if (!stats_writer_.init(participant_, output_topic))
        {
            return false;
        }

        engine_->set_writer(&stats_writer_);

        std::cout << "[INIT] DataAnalysisAggregator ready on domain "
                  << participant_->get_domain_id()
                  << " | in=" << input_topic
                  << " | out=" << output_topic << "\n";
        return true;
    }

    void run()
    {
        std::cout << "[RUN] Listening (Ctrl+C to stop).\n";
        while (g_running)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        std::cout << "\n[SHUTDOWN] Flushing open snapshots...\n";
        engine_->flush_all();
        std::cout << "[SHUTDOWN] Done.\n";
    }

private:
    DomainParticipant*                 participant_;
    Topic*                             sub_topic_;
    Subscriber*                        subscriber_;
    DataReader*                        reader_;
    TypeSupport                        in_type_;
    StatsWriter                        stats_writer_;
    std::unique_ptr<GapTracker>        gaps_;
    std::unique_ptr<AnalyticsEngine>   engine_;
    std::unique_ptr<AnalyticsListener> listener_;
};

} // namespace

// ============================================================================
// main
// ============================================================================

int main(int argc, char** argv)
{
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try
    {
        AppConfig cfg = parse_args(argc, argv);
        DataAnalysisAggregator app;
        if (!app.init(cfg)) return 1;
        app.run();
        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        print_usage(argv[0]);
        return 1;
    }
}
