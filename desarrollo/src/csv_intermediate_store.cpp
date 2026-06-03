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

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
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

#include <tinyxml2.h>
#include "AirSamplePubSubTypes.hpp"

using namespace eprosima::fastdds::dds;

namespace
{

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct AppConfig
{
    bool        use_xml    = false;
    std::string xml_path;
    std::string output_csv  = "air_quality_raw.csv";
    std::string gap_csv     = "intermediate_store_gaps.csv";
    std::size_t flush_every = 100;
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
    return "csv_intermediate_store";
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

void print_usage(const char* progname)
{
    std::cout
        << "Usage:\n"
        << "  " << progname
        << " -c profile.xml [-o output.csv] [-f flush_every]"
           " [--gap-csv path] [--node-id id]\n"
        << "\n"
        << "Options:\n"
        << "  -c <path>     XML QoS profile file (required)\n"
        << "  -o <path>     Output CSV file                 (default: air_quality_raw.csv)\n"
        << "  -f <N>        Flush to disk every N samples   (default: 100)\n"
        << "  --gap-csv <p> CSV file for gap/recovery events (default: intermediate_store_gaps.csv)\n"
        << "  --node-id <s> Node identifier for CSV reports  (default: $NODE_ID/$HOSTNAME)\n";
}

std::string read_topic_name_from_xml(
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
    AppConfig cfg;
    std::vector<std::string> extra;

    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help")
        {
            print_usage(argv[0]);
            std::exit(0);
        }
        else if (arg == "-c")
        {
            if (i + 1 >= argc) throw std::runtime_error("Missing XML path after -c");
            cfg.use_xml  = true;
            cfg.xml_path = argv[++i];
        }
        else if (arg == "-o")
        {
            if (i + 1 >= argc) throw std::runtime_error("Missing CSV path after -o");
            cfg.output_csv = argv[++i];
        }
        else if (arg == "-f")
        {
            if (i + 1 >= argc) throw std::runtime_error("Missing number after -f");
            cfg.flush_every = static_cast<std::size_t>(std::stoul(argv[++i]));
        }
        else if (arg == "--gap-csv")
        {
            if (i + 1 >= argc) throw std::runtime_error("Missing path after --gap-csv");
            cfg.gap_csv = argv[++i];
        }
        else if (arg == "--node-id")
        {
            if (i + 1 >= argc) throw std::runtime_error("Missing value after --node-id");
            cfg.node_id = argv[++i];
        }
        else
        {
            extra.push_back(arg);
        }
    }

    if (!extra.empty())
        throw std::runtime_error("Unrecognized argument: " + extra[0]);

    if (cfg.node_id.empty()) cfg.node_id = default_node_id();
    return cfg;
}

// ---------------------------------------------------------------------------
// In-memory store + CSV writer (ampliado con magnitude_id / sequence_id)
// ---------------------------------------------------------------------------

struct RawRecord
{
    std::string station;
    int32_t     magnitud;
    int32_t     year;
    int32_t     month;
    int32_t     day;
    int32_t     hour;
    double      value;
    std::string validity;
    int32_t     magnitude_id;
    int64_t     sequence_id;
    std::string origin_writer_id;
    int64_t     publish_ts_ns;
    std::string received_at;
};

class CsvStore
{
public:
    explicit CsvStore(const std::string& path, std::size_t flush_every)
        : path_(path)
        , flush_every_(flush_every)
        , total_received_(0)
    {
        file_.open(path_, std::ios::app | std::ios::binary);
        if (!file_.is_open())
            throw std::runtime_error("Cannot open output CSV: " + path_);

        file_.seekp(0, std::ios::end);
        if (file_.tellp() == 0)
        {
            file_ << "station,magnitud,year,month,day,hour,value,validity,"
                     "magnitude_id,sequence_id,origin_writer_id,publish_ts_ns,"
                     "received_at\n";
        }

        std::cout << "[STORE] Output CSV: " << path_ << "\n";
    }

    ~CsvStore() { flush_locked(); }

    void push(const AirSample& s)
    {
        RawRecord rec;
        rec.station          = s.station();
        rec.magnitud         = s.magnitud();
        rec.year             = s.year();
        rec.month            = s.month();
        rec.day              = s.day();
        rec.hour             = s.hour();
        rec.value            = s.value();
        rec.validity         = s.validity();
        rec.magnitude_id     = s.magnitude_id();
        rec.sequence_id      = s.sequence_id();
        rec.origin_writer_id = s.origin_writer_id();
        rec.publish_ts_ns    = s.publish_ts_ns();
        rec.received_at      = wall_clock_now_iso();

        {
            std::lock_guard<std::mutex> lk(mutex_);
            buffer_.push_back(std::move(rec));
            ++total_received_;
            if (buffer_.size() >= flush_every_)
                flush_locked();
        }
    }

    std::size_t total_received() const { return total_received_.load(); }

    void flush()
    {
        std::lock_guard<std::mutex> lk(mutex_);
        flush_locked();
    }

private:
    void flush_locked()
    {
        if (buffer_.empty()) return;

        for (const auto& r : buffer_)
        {
            file_
                << escape(r.station)  << ','
                << r.magnitud         << ','
                << r.year             << ','
                << r.month            << ','
                << r.day              << ','
                << r.hour             << ','
                << r.value            << ','
                << escape(r.validity) << ','
                << r.magnitude_id     << ','
                << r.sequence_id      << ','
                << escape(r.origin_writer_id) << ','
                << r.publish_ts_ns    << ','
                << r.received_at      << '\n';
        }

        file_.flush();
        std::cout << "[STORE] Flushed " << buffer_.size()
                  << " records (total=" << total_received_.load() << ")\n";
        buffer_.clear();
    }

    static std::string escape(const std::string& s)
    {
        if (s.find_first_of(",\"\n") == std::string::npos) return s;
        std::string out = "\"";
        for (char c : s) { if (c == '"') out += '"'; out += c; }
        out += '"';
        return out;
    }

    std::string              path_;
    std::size_t              flush_every_;
    std::ofstream            file_;
    std::mutex               mutex_;
    std::vector<RawRecord>   buffer_;
    std::atomic<std::size_t> total_received_;
};

// ---------------------------------------------------------------------------
// Gap / recovery tracker (instrumentación local)
// ---------------------------------------------------------------------------
//
// Por cada (origin_writer_id, magnitude_id):
//   - mantiene el último sequence_id recibido;
//   - si llega un seq > last + 1  → gap detectado (lost_messages = seq-last-1)
//   - si llega un seq < last      → reordenación o reinicio: se loguea
//   - al recibir la primera muestra tras un período > stall_threshold_ms
//     de silencio, se calcula recovery_time_ms respecto al último recibido.
//
// Nota: los timestamps se usan SOLO para medir tiempos (recuperación), no
// como identificadores. La identidad de cada muestra es (magnitude_id, seq).

class GapTracker
{
public:
    GapTracker(const std::string& path,
               const std::string& node_id,
               const std::string& scope_topic)
        : path_(path)
        , node_id_(node_id)
        , scope_topic_(scope_topic)
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
        auto now_wall_ns = now_ns();

        std::lock_guard<std::mutex> lk(mu_);
        auto it = state_.find(k);
        if (it == state_.end())
        {
            obs.first_seen = true;
            state_[k] = State{seq, now_wall_ns};
            write_event("FIRST_SEEN", k, /*prev*/ 0, /*curr*/ seq,
                        /*lost*/ 0, /*gap_ms*/ 0, "first sample for this flow");
            return obs;
        }

        auto& st = it->second;
        int64_t last = st.last_seq;

        if (seq == last + 1)
        {
            obs.in_order = true;
            st.last_seq = seq;
            st.last_seen_ns = now_wall_ns;
            return obs;
        }

        if (seq > last + 1)
        {
            obs.gap = true;
            obs.lost_messages = seq - last - 1;
            obs.gap_duration_ms =
                (now_wall_ns - st.last_seen_ns) / 1'000'000LL;

            std::ostringstream notes;
            notes << "discontinuity detected";
            write_event("GAP", k, last, seq,
                        obs.lost_messages, obs.gap_duration_ms, notes.str());

            st.last_seq = seq;
            st.last_seen_ns = now_wall_ns;
            return obs;
        }

        // seq <= last
        obs.in_order = false;
        obs.out_of_order = true;

        std::ostringstream notes;
        if (seq == last)
        {
            obs.duplicate = true;
            notes << "duplicate";
        }
        else
        {
            notes << "out-of-order or writer restart (seq<last)";
        }
        write_event("OUT_OF_ORDER", k, last, seq, 0, 0, notes.str());
        // No actualizamos last_seq para no "perder" progreso por un reorder.
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

    struct State
    {
        int64_t last_seq     = 0;
        int64_t last_seen_ns = 0;
    };

    void write_event(const char* type,
                     const Key& k,
                     int64_t prev,
                     int64_t curr,
                     int64_t lost,
                     int64_t gap_ms,
                     const std::string& notes)
    {
        if (!file_.is_open()) return;
        file_ << wall_clock_now_iso() << ','
              << escape(node_id_)       << ','
              << escape(scope_topic_)   << ','
              << type                   << ','
              << escape(k.origin_writer_id) << ','
              << k.magnitude_id         << ','
              << prev                   << ','
              << curr                   << ','
              << lost                   << ','
              << gap_ms                 << ','
              << escape(notes)          << '\n';
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

    std::string      path_;
    std::string      node_id_;
    std::string      scope_topic_;
    std::ofstream    file_;
    std::mutex       mu_;
    std::map<Key, State> state_;
};

// ---------------------------------------------------------------------------
// Raw forwarder
// ---------------------------------------------------------------------------

class ForwarderWriterListener : public DataWriterListener
{
public:
    std::atomic_int matched_{0};

    void on_publication_matched(
            DataWriter*,
            const PublicationMatchedStatus& info) override
    {
        matched_.store(info.current_count);
        if      (info.current_count_change ==  1)
            std::cout << "[FWD] Subscriber matched. total=" << info.current_count << "\n";
        else if (info.current_count_change == -1)
            std::cout << "[FWD] Subscriber unmatched. total=" << info.current_count << "\n";
    }
};

class RawForwarder
{
public:
    RawForwarder() = default;

    bool init(DomainParticipant* participant, const std::string& topic_name)
    {
        type_.reset(new AirSamplePubSubType());
        type_.register_type(participant);

        publisher_ = participant->create_publisher_with_profile(
            "intermediate_store_forward_publisher_profile", nullptr);
        if (!publisher_)
        {
            std::cerr << "[FWD] Failed to create Publisher\n";
            return false;
        }

        topic_ = participant->create_topic_with_profile(
            topic_name,
            type_.get_type_name(),
            "forward_topic_profile",
            nullptr,
            StatusMask::none());
        if (!topic_)
        {
            std::cerr << "[FWD] Failed to create Topic: " << topic_name << "\n";
            return false;
        }

        writer_ = publisher_->create_datawriter_with_profile(
            topic_, "intermediate_store_forward_writer_profile", &listener_);
        if (!writer_)
        {
            std::cerr << "[FWD] Failed to create DataWriter\n";
            return false;
        }

        std::cout << "[FWD] RawForwarder ready on topic: " << topic_name << "\n";
        return true;
    }

    void cleanup(DomainParticipant* participant)
    {
        if (publisher_ && writer_)     publisher_->delete_datawriter(writer_);
        if (participant && publisher_) participant->delete_publisher(publisher_);
        if (participant && topic_)     participant->delete_topic(topic_);
        writer_    = nullptr;
        publisher_ = nullptr;
        topic_     = nullptr;
    }

    // Reenvía la muestra preservando íntegramente magnitude_id / sequence_id /
    // origin_writer_id / publish_ts_ns. El store es transparente para la
    // trazabilidad: sólo observa. Esto permite al final_agg_subscriber
    // detectar pérdidas end-to-end con los mismos ids originales.
    void forward(const AirSample& sample)
    {
        if (!writer_) return;

        if (writer_->write(const_cast<AirSample*>(&sample)) == RETCODE_OK)
        {
            ++forwarded_;
            std::cout << "[FWD] Forwarded sample"
                      << " station=" << sample.station()
                      << " mag_id="  << sample.magnitude_id()
                      << " seq="     << sample.sequence_id()
                      << " origin="  << sample.origin_writer_id()
                      << " total_forwarded=" << forwarded_.load()
                      << " matched_subscribers=" << listener_.matched_.load()
                      << "\n";
        }
        else
        {
            std::cerr << "[FWD] Write failed for station=" << sample.station() << "\n";
        }
    }

    std::size_t total_forwarded() const { return forwarded_.load(); }

private:
    TypeSupport               type_;
    Publisher*                publisher_ = nullptr;
    Topic*                    topic_     = nullptr;
    DataWriter*               writer_    = nullptr;
    ForwarderWriterListener   listener_;
    std::atomic<std::size_t>  forwarded_{0};
};

// ---------------------------------------------------------------------------
// DDS listener
// ---------------------------------------------------------------------------

class StoringListener : public DataReaderListener
{
public:
    StoringListener(CsvStore& store,
                    RawForwarder& forwarder,
                    GapTracker& gaps)
        : store_(store)
        , forwarder_(forwarder)
        , gaps_(gaps)
    {}

    std::atomic_int matched_{0};

    void on_subscription_matched(
            DataReader*,
            const SubscriptionMatchedStatus& info) override
    {
        matched_.store(info.current_count);
        auto wall = now_ns();

        if (info.current_count_change == 1)
        {
            std::cout << "[MATCH] Publisher matched. total=" << info.current_count << "\n";
            // Si antes hubo un unmatch, medimos tiempo de recuperación
            // al recibir la próxima muestra válida.
            std::lock_guard<std::mutex> lk(match_mu_);
            if (last_unmatch_ns_ != 0)
            {
                pending_recovery_ = true;
                // tiempo "t0" para la recuperación será la siguiente muestra.
                // Guardamos el instante del rematcheo para la nota de contexto.
                last_rematch_ns_ = wall;
            }
        }
        else if (info.current_count_change == -1)
        {
            std::cout << "[MATCH] Publisher unmatched. total=" << info.current_count << "\n";
            std::lock_guard<std::mutex> lk(match_mu_);
            last_unmatch_ns_ = wall;
            pending_recovery_ = false;
        }
    }

    void on_data_available(DataReader* reader) override
    {
        AirSample  sample;
        SampleInfo info;

        while (reader->take_next_sample(&sample, &info) == RETCODE_OK)
        {
            if (!info.valid_data) continue;

            auto obs = gaps_.observe(
                sample.origin_writer_id(),
                sample.magnitude_id(),
                sample.sequence_id());

            // Skip duplicates and out-of-order samples: do not store
            // them and do not forward them downstream.
            if (obs.duplicate || obs.out_of_order)
            {
                std::cout
                    << "[SKIP] station="   << sample.station()
                    << " mag_id="          << sample.magnitude_id()
                    << " seq="             << sample.sequence_id()
                    << " origin="          << sample.origin_writer_id()
                    << (obs.duplicate ? " [DUPLICATE]" : " [OUT_OF_ORDER]")
                    << "\n";
                continue;
            }

            store_.push(sample);

            // Recuperación: la primera muestra tras un rematcheo marca el
            // tiempo de recuperación respecto al unmatch anterior.
            bool report_rec = false;
            int64_t rec_ms = 0;
            {
                std::lock_guard<std::mutex> lk(match_mu_);
                if (pending_recovery_ && last_unmatch_ns_ != 0)
                {
                    rec_ms = (now_ns() - last_unmatch_ns_) / 1'000'000LL;
                    report_rec = true;
                    pending_recovery_ = false;
                    last_unmatch_ns_ = 0;
                }
            }
            if (report_rec)
            {
                gaps_.report_recovery(
                    sample.origin_writer_id(),
                    sample.magnitude_id(),
                    rec_ms,
                    "first sample after publisher re-match");
                std::cout << "[RECOVERY] origin=" << sample.origin_writer_id()
                          << " mag=" << sample.magnitude_id()
                          << " recovery_ms=" << rec_ms << "\n";
            }

            // Log de consola
            std::cout
                << "[RX] station="   << sample.station()
                << " mag_id="        << sample.magnitude_id()
                << " seq="           << sample.sequence_id()
                << " origin="        << sample.origin_writer_id()
                << " value="         << sample.value()
                << " total_stored="  << store_.total_received();

            if (obs.gap)
            {
                std::cout << " [GAP lost=" << obs.lost_messages
                          << " gap_ms=" << obs.gap_duration_ms << "]";
            }
            else if (obs.first_seen)
            {
                std::cout << " [FIRST_SEEN]";
            }

            std::cout << "\n";

            forwarder_.forward(sample);
        }
    }

private:
    CsvStore&     store_;
    RawForwarder& forwarder_;
    GapTracker&   gaps_;

    std::mutex match_mu_;
    int64_t    last_unmatch_ns_ = 0;
    int64_t    last_rematch_ns_ = 0;
    bool       pending_recovery_ = false;
};

// ---------------------------------------------------------------------------
// Intermediate storing subscriber
// ---------------------------------------------------------------------------

class IntermediateStore
{
public:
    IntermediateStore()
        : participant_(nullptr)
        , topic_(nullptr)
        , subscriber_(nullptr)
        , reader_(nullptr)
    {}

    ~IntermediateStore()
    {
        if (subscriber_ && reader_)      subscriber_->delete_datareader(reader_);
        if (participant_ && subscriber_) participant_->delete_subscriber(subscriber_);
        forwarder_.cleanup(participant_);
        if (participant_ && topic_)      participant_->delete_topic(topic_);
        if (participant_)                DomainParticipantFactory::get_instance()
                                             ->delete_participant(participant_);
    }

    bool init(const AppConfig& cfg)
    {
        if (!cfg.use_xml)
        {
            std::cerr << "[INIT] XML profile required. Use -c <profile.xml>\n";
            return false;
        }

        DomainParticipantFactory* factory = DomainParticipantFactory::get_instance();
        type_.reset(new AirSamplePubSubType());

        std::cout << "[INIT] Loading XML profile: " << cfg.xml_path << "\n";

        if (factory->load_XML_profiles_file(cfg.xml_path) != RETCODE_OK)
        {
            std::cerr << "[INIT] Error loading XML: " << cfg.xml_path << "\n";
            return false;
        }

        participant_ = factory->create_participant_with_profile(
            "intermediate_store_participant_profile");
        if (!participant_)
        {
            std::cerr << "[INIT] Error creating DomainParticipant\n";
            return false;
        }

        type_.register_type(participant_);

        std::string sub_topic_name = read_topic_name_from_xml(
            cfg.xml_path, "topic_profile");
        if (sub_topic_name.empty())
        {
            std::cerr << "[INIT] Could not read subscription topic from XML (topic_profile)\n";
            return false;
        }
        std::cout << "[INIT] Subscription topic: " << sub_topic_name << "\n";

        std::string fwd_topic_name = read_topic_name_from_xml(
            cfg.xml_path, "forward_topic_profile");
        if (fwd_topic_name.empty())
        {
            std::cerr << "[INIT] Could not read forward topic from XML (forward_topic_profile)\n";
            return false;
        }
        std::cout << "[INIT] Forward topic: " << fwd_topic_name << "\n";

        store_ = std::make_unique<CsvStore>(cfg.output_csv, cfg.flush_every);
        gaps_  = std::make_unique<GapTracker>(cfg.gap_csv, cfg.node_id, sub_topic_name);

        if (!forwarder_.init(participant_, fwd_topic_name))
            return false;

        listener_ = std::make_unique<StoringListener>(*store_, forwarder_, *gaps_);

        topic_ = participant_->create_topic_with_profile(
            sub_topic_name,
            type_.get_type_name(),
            "topic_profile",
            nullptr,
            StatusMask::all());
        if (!topic_)
        {
            std::cerr << "[INIT] Error creating subscription Topic\n";
            return false;
        }

        subscriber_ = participant_->create_subscriber_with_profile(
            "intermediate_store_subscriber_profile");
        if (!subscriber_)
        {
            std::cerr << "[INIT] Error creating Subscriber\n";
            return false;
        }

        reader_ = subscriber_->create_datareader_with_profile(
            topic_, "intermediate_store_reader_profile", listener_.get());
        if (!reader_)
        {
            std::cerr << "[INIT] Error creating DataReader\n";
            return false;
        }

        std::cout << "[INIT] IntermediateStore ready on domain "
                  << participant_->get_domain_id()
                  << " | node_id=" << cfg.node_id
                  << " | sub=" << sub_topic_name
                  << " | fwd=" << fwd_topic_name << "\n";
        return true;
    }

    void run()
    {
        std::cout << "[RUN] Waiting for samples (Ctrl+C to stop)...\n";
        while (g_running)
            std::this_thread::sleep_for(std::chrono::seconds(1));

        std::cout << "\n[SHUTDOWN] Flushing remaining records...\n";
        store_->flush();
        std::cout << "[SHUTDOWN] Total stored:    " << store_->total_received() << "\n";
        std::cout << "[SHUTDOWN] Total forwarded: " << forwarder_.total_forwarded() << "\n";
    }

private:
    DomainParticipant*               participant_;
    Topic*                           topic_;
    Subscriber*                      subscriber_;
    DataReader*                      reader_;
    TypeSupport                      type_;
    RawForwarder                     forwarder_;
    std::unique_ptr<CsvStore>        store_;
    std::unique_ptr<GapTracker>      gaps_;
    std::unique_ptr<StoringListener> listener_;
};

} // namespace

int main(int argc, char** argv)
{
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try
    {
        AppConfig cfg = parse_args(argc, argv);
        IntermediateStore store;
        if (!store.init(cfg)) return 1;
        store.run();
        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        print_usage(argv[0]);
        return 1;
    }
}
