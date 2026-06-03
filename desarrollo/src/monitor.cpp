// Copyright 2021 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file monitor.cpp
 *
 * Per-topic monitor. Para cada topic de usuario descubierto en el dominio
 * imprime en cada intervalo:
 *   · FASTDDS end-to-end latency  (mean / min / max / stddev)  [µs]
 *   · Publication throughput      (mean / min / max / stddev)  [B/s]
 *   · Subscription throughput     (mean / min / max / stddev)  [B/s]
 *   · RESENT_DATAS  (sum over interval, por writer y total)    [packets]
 *
 * Notas de diseño:
 * - Se ha eliminado la dependencia de RTPS_PACKETS_SENT y RTPS_PACKETS_LOST
 *   como métrica principal de pérdida. En muchos despliegues estas métricas
 *   devuelven NaN aunque el backend las descubra. Se sigue consultándolas
 *   a modo diagnóstico opcional (ver --enable-rtps-diag).
 * - RESENT_DATAS es la señal primaria de que empiezan los problemas de
 *   transmisión: si crece, hay retransmisiones, lo que suele preceder a
 *   pérdidas efectivas.
 * - La detección de pérdidas APLICATIVAS por discontinuidad de secuencia
 *   la hacen las apps (csv_intermediate_store, csv_analysis_aggregator,
 *   csv_final_agg_subscriber) y se vuelcan en air_quality_report.csv.
 *   Este monitor escribe monitor_report.csv con métricas de red/domain.
 * - La solución funciona tanto en despliegue monolítico como distribuido:
 *   cada monitor reporta lo visible desde su nodo, sin mezclar información
 *   que no ha observado.
 */

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <csignal>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <fastdds_statistics_backend/listener/DomainListener.hpp>
#include <fastdds_statistics_backend/StatisticsBackend.hpp>
#include <fastdds_statistics_backend/types/EntityId.hpp>
#include <fastdds_statistics_backend/types/types.hpp>
#include <fastdds_statistics_backend/types/utils.hpp>

#include "monitor.hpp"

using namespace eprosima::statistics_backend;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

// ─── static members ──────────────────────────────────────────────────────────
std::atomic<bool>       Monitor::stop_(false);
std::mutex              Monitor::terminate_cv_mtx_;
std::condition_variable Monitor::terminate_cv_;

// ─── helpers ─────────────────────────────────────────────────────────────────

struct Stats
{
    double mean   = 0.0;
    double stddev = 0.0;
    double min    = std::numeric_limits<double>::max();
    double max    = std::numeric_limits<double>::lowest();
    size_t count  = 0;

    // Contadores para distinguir sin_muestras / NaN / 0 real.
    size_t nan_count  = 0;
    size_t zero_count = 0;
    size_t raw_count  = 0;

    void feed(double v)
    {
        ++raw_count;
        if (std::isnan(v))
        {
            ++nan_count;
            return;
        }
        if (!std::isfinite(v))
        {
            return;
        }
        if (v == 0.0)
        {
            ++zero_count;
            // 0.0 es un valor válido: lo incluimos en media.
        }
        if (v < 0.0)
        {
            // Algunos kinds como latency no tienen sentido negativos; los
            // ignoramos.
            return;
        }

        sum_ += v;
        sum_sq_ += v * v;

        if (v < min) min = v;
        if (v > max) max = v;

        ++count;
    }

    void finalize()
    {
        if (count == 0)
        {
            return;
        }

        mean = sum_ / count;
        double var = (sum_sq_ / count) - (mean * mean);
        stddev = (var > 0.0) ? std::sqrt(var) : 0.0;
    }

private:
    double sum_ = 0.0;
    double sum_sq_ = 0.0;
};

// Estado textual del resultado de una métrica, siguiendo el criterio pedido:
//   - "sin muestras" : el backend no devolvió nada
//   - "NaN"          : devolvió sólo NaN
//   - "0 real"       : devolvió valores finitos cuya suma es 0
//   - "finito"       : valor numérico útil
//   - "mixto"        : finitos + NaN mezclados
enum class MetricStatus
{
    NO_SAMPLES,
    NAN_ONLY,
    REAL_ZERO,
    FINITE,
    MIXED
};

static std::string status_to_string(MetricStatus s)
{
    switch (s)
    {
        case MetricStatus::NO_SAMPLES: return "sin muestras";
        case MetricStatus::NAN_ONLY:   return "NaN";
        case MetricStatus::REAL_ZERO:  return "0 real";
        case MetricStatus::FINITE:     return "finito";
        case MetricStatus::MIXED:      return "mixto(finito+NaN)";
    }
    return "desconocido";
}

static MetricStatus classify(const Stats& s)
{
    if (s.raw_count == 0) return MetricStatus::NO_SAMPLES;
    if (s.nan_count > 0 && s.count == 0) return MetricStatus::NAN_ONLY;
    if (s.nan_count > 0 && s.count > 0)  return MetricStatus::MIXED;
    if (s.count > 0 && s.mean == 0.0 && s.max == 0.0) return MetricStatus::REAL_ZERO;
    if (s.count > 0) return MetricStatus::FINITE;
    return MetricStatus::NO_SAMPLES;
}

static std::string safe_info_to_string(const Info& info, const std::string& key)
{
    try
    {
        return std::string(info[key]);
    }
    catch (...)
    {
        return "<unknown>";
    }
}

// Print a Stats block with a label and unit, using the
// sin muestras / NaN / 0 real / finito distinction.
static void print_stats(const std::string& label, const Stats& s, const std::string& unit)
{
    MetricStatus st = classify(s);
    if (st == MetricStatus::NO_SAMPLES)
    {
        std::cout << "  " << label << ": sin muestras\n";
        return;
    }
    if (st == MetricStatus::NAN_ONLY)
    {
        std::cout << "  " << label << ": NaN\n";
        return;
    }
    if (st == MetricStatus::REAL_ZERO)
    {
        std::cout << "  " << label << ": 0 real (count=" << s.count << ")\n";
        return;
    }

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  " << label << ":\n"
              << "    status=" << status_to_string(st)
              << "  count=" << s.count
              << "  mean=" << s.mean << " " << unit
              << "  min=" << s.min << " " << unit
              << "  max=" << s.max << " " << unit
              << "  stddev=" << s.stddev << " " << unit;
    if (s.nan_count > 0) std::cout << "  nan_count=" << s.nan_count;
    std::cout << "\n";
}

// Aggregate a vector<StatisticsData> into a Stats struct (optional scale factor)
static Stats aggregate(const std::vector<StatisticsData>& data, double scale = 1.0)
{
    Stats s;
    for (const auto& d : data)
    {
        s.feed(d.second * scale);
    }
    s.finalize();
    return s;
}

// Sum all finite values and ignore NaNs. Useful for counters like RESENT_DATAS.
static Stats sum_counter(const std::vector<StatisticsData>& data)
{
    Stats s;
    double total = 0.0;
    for (const auto& d : data)
    {
        ++s.raw_count;
        if (std::isnan(d.second))
        {
            ++s.nan_count;
            continue;
        }
        if (!std::isfinite(d.second)) continue;
        if (d.second == 0.0) ++s.zero_count;
        total += d.second;
        ++s.count;
    }
    if (s.count > 0)
    {
        s.mean = total;   // reutilizamos mean como "suma" para counters
        s.min  = total;
        s.max  = total;
        s.stddev = 0.0;
    }
    return s;
}

// ─── Monitor lifecycle ───────────────────────────────────────────────────────

Monitor::Monitor()
{
}

Monitor::~Monitor()
{
    if (report_csv_.is_open()) report_csv_.close();
    StatisticsBackend::stop_monitor(monitor_id_);
}

bool Monitor::is_stopped()
{
    return stop_;
}

void Monitor::stop()
{
    stop_ = true;
    terminate_cv_.notify_all();
}

bool Monitor::init(
        uint32_t domain,
        uint16_t n_bins,
        uint32_t t_interval,
        std::string topic_name,
        std::string dump_file,
        bool reset,
        std::string report_csv,
        std::string node_id)
{
    n_bins_ = n_bins;
    t_interval_ = t_interval;
    topic_name_ = std::move(topic_name);
    dump_file_ = std::move(dump_file);
    reset_ = reset;
    report_csv_path_ = std::move(report_csv);
    node_id_ = std::move(node_id);

    if (node_id_.empty())
    {
        if (const char* env = std::getenv("NODE_ID"))
        {
            if (env[0] != '\0') node_id_ = env;
        }
        if (node_id_.empty())
        {
            if (const char* env = std::getenv("HOSTNAME"))
            {
                if (env[0] != '\0') node_id_ = env;
            }
        }
        if (node_id_.empty()) node_id_ = "monitor";
    }

    // FastDDS loads the security profile automatically via
    // FASTDDS_DEFAULT_PROFILES_FILE, so init_monitor is enough in all cases.
    const char* profile_env = std::getenv("FASTDDS_DEFAULT_PROFILES_FILE");
    if (profile_env && std::string(profile_env) != "")
    {
        std::cout << "Security profile detected: " << profile_env
                  << " (FastDDS will load it automatically)\n";
    }
    else
    {
        std::cout << "Initializing monitor without security profile\n";
    }

    monitor_id_ = StatisticsBackend::init_monitor(
        domain,
        nullptr,
        CallbackMask::all(),
        DataKindMask::all());

    if (!monitor_id_.is_valid())
    {
        std::cerr << "Error: could not create monitor on domain " << domain << "\n";
        return false;
    }

    StatisticsBackend::set_physical_listener(&physical_listener_);

    if (!report_csv_path_.empty())
    {
        ensure_report_header();
        std::cout << "[REPORT] Monitor report CSV: " << report_csv_path_
                  << "  node_id=" << node_id_ << "\n";
    }

    return true;
}

void Monitor::ensure_report_header()
{
    std::lock_guard<std::mutex> lk(report_mu_);
    report_csv_.open(report_csv_path_, std::ios::app | std::ios::binary);
    if (!report_csv_.is_open())
    {
        std::cerr << "[REPORT] Cannot open monitor report CSV: "
                  << report_csv_path_ << "\n";
        return;
    }
    report_csv_.seekp(0, std::ios::end);
    if (report_csv_.tellp() == 0)
    {
        report_csv_
            << "timestamp,node_id,scope_visible,topic,"
               "source_writer,target_reader,"
               "metric,value,unit,status,notes\n";
    }
}

static std::string iso_utc_now()
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

static std::string csv_escape(const std::string& s)
{
    if (s.find_first_of(",\"\n") == std::string::npos) return s;
    std::string out = "\"";
    for (char c : s) { if (c == '"') out += '"'; out += c; }
    out += '"';
    return out;
}

void Monitor::write_report_row(
        const std::string& scope_visible,
        const std::string& topic,
        const std::string& source_writer,
        const std::string& target_reader,
        const std::string& metric,
        const std::string& value,
        const std::string& unit,
        const std::string& status,
        const std::string& notes)
{
    if (report_csv_path_.empty()) return;
    std::lock_guard<std::mutex> lk(report_mu_);
    if (!report_csv_.is_open()) return;
    report_csv_
        << iso_utc_now()          << ','
        << csv_escape(node_id_)    << ','
        << csv_escape(scope_visible) << ','
        << csv_escape(topic)       << ','
        << csv_escape(source_writer) << ','
        << csv_escape(target_reader) << ','
        << csv_escape(metric)      << ','
        << value                   << ','
        << csv_escape(unit)        << ','
        << csv_escape(status)      << ','
        << csv_escape(notes)       << '\n';
    report_csv_.flush();
}

// ─── Main loop ───────────────────────────────────────────────────────────────

void Monitor::run()
{
    stop_ = false;
    std::cout << "Monitor running. Press CTRL+C to stop.\n";

    signal(SIGINT, [](int)
    {
        std::cout << "\nSIGINT received – stopping.\n";
        Monitor::stop();
    });

    while (!is_stopped())
    {
        {
            std::unique_lock<std::mutex> lck(terminate_cv_mtx_);
            terminate_cv_.wait_for(
                lck,
                std::chrono::seconds(t_interval_),
                [] { return is_stopped(); });
        }

        if (is_stopped())
        {
            break;
        }

        auto now_time = now();
        auto t_from = now_time - std::chrono::seconds(t_interval_);

        // ── discover all topics ──────────────────────────────────────────────
        std::vector<EntityId> all_topics =
            StatisticsBackend::get_entities(EntityKind::TOPIC);

        std::vector<EntityId> user_topics;
        bool found_resent_topic = false;

        for (auto tid : all_topics)
        {
            Info info = StatisticsBackend::get_info(tid);
            std::string discovered_topic_name = safe_info_to_string(info, NAME_TAG);

            if (discovered_topic_name == "_fastdds_statistics_resent_datas")
            {
                found_resent_topic = true;
            }

            if (!info[METATRAFFIC_TAG])
            {
                user_topics.push_back(tid);
            }
        }

        // ── discover all non-metatraffic participants ────────────────────────
        std::vector<EntityId> all_participants =
            StatisticsBackend::get_entities(EntityKind::PARTICIPANT);

        std::vector<EntityId> user_participants;
        for (auto pid : all_participants)
        {
            Info info = StatisticsBackend::get_info(pid);
            if (!info[METATRAFFIC_TAG])
            {
                user_participants.push_back(pid);
            }
        }

        // ── print separator ──────────────────────────────────────────────────
        auto now_ms = duration_cast<milliseconds>(
            steady_clock::now().time_since_epoch()).count();

        std::cout << "\n════════════════════════════════════════════════════════\n";
        std::cout << "  Report @ " << now_ms << " ms"
                  << "  (window: last " << t_interval_ << " s)"
                  << "  node_id=" << node_id_ << "\n";
        std::cout << "════════════════════════════════════════════════════════\n";

        // ════════════════════════════════════════════════════════════════════
        //  Debug: discovered topics
        // ════════════════════════════════════════════════════════════════════
        std::cout << "\n┌─ Discovered topics\n";
        std::cout << "  total_topics=" << all_topics.size() << "\n";

        for (auto tid : all_topics)
        {
            Info info = StatisticsBackend::get_info(tid);
            std::string discovered_topic_name = safe_info_to_string(info, NAME_TAG);
            bool is_metatraffic = static_cast<bool>(info[METATRAFFIC_TAG]);

            std::cout << "  - " << discovered_topic_name
                      << "  [metatraffic=" << (is_metatraffic ? "true" : "false") << "]\n";
        }

        std::cout << "  _fastdds_statistics_resent_datas discovered: "
                  << (found_resent_topic ? "YES" : "NO") << "\n";
        std::cout << "└─────────────────────────────────────────────────\n";

        // ════════════════════════════════════════════════════════════════════
        //  Per-topic metrics: latency, pub throughput, sub throughput, resent
        // ════════════════════════════════════════════════════════════════════
        if (user_topics.empty())
        {
            std::cout << "  (no user topics discovered yet)\n";
            write_report_row(
                "domain", "-", "-", node_id_,
                "user_topics", "0", "count", "sin muestras",
                "no user topics discovered yet");
        }

        for (auto topic_id : user_topics)
        {
            Info topic_info = StatisticsBackend::get_info(topic_id);
            std::string topic_name = safe_info_to_string(topic_info, NAME_TAG);

            std::cout << "\n┌─ Topic: " << topic_name << "\n";

            std::vector<EntityId> writers =
                StatisticsBackend::get_entities(EntityKind::DATAWRITER, topic_id);
            std::vector<EntityId> readers =
                StatisticsBackend::get_entities(EntityKind::DATAREADER, topic_id);

            // ── End-to-end latency (writers → readers) ────────────────────
            if (!writers.empty() && !readers.empty())
            {
                try
                {
                    auto latency_data = StatisticsBackend::get_data(
                        DataKind::FASTDDS_LATENCY,
                        writers,
                        readers,
                        n_bins_,
                        t_from,
                        now_time,
                        StatisticKind::MEAN);

                    Stats lat = aggregate(latency_data, 1.0 / 1000.0); // ns → µs
                    print_stats("Latency (e2e)", lat, "µs");

                    MetricStatus st = classify(lat);
                    std::string value = (st == MetricStatus::FINITE ||
                                         st == MetricStatus::MIXED ||
                                         st == MetricStatus::REAL_ZERO)
                        ? std::to_string(lat.mean) : "";
                    write_report_row(
                        "topic", topic_name, "aggregated_writers", "aggregated_readers",
                        "latency_mean_us", value, "us",
                        status_to_string(st),
                        "writers=" + std::to_string(writers.size()) +
                        " readers=" + std::to_string(readers.size()));
                }
                catch (const std::exception& e)
                {
                    std::cout << "  Latency (e2e): " << e.what() << "\n";
                    write_report_row(
                        "topic", topic_name, "-", "-",
                        "latency_mean_us", "", "us", "NaN", e.what());
                }
            }
            else
            {
                std::cout << "  Latency (e2e): not enough endpoints"
                          << " (writers=" << writers.size()
                          << " readers=" << readers.size() << ")\n";
                write_report_row(
                    "topic", topic_name, "-", "-",
                    "latency_mean_us", "", "us", "sin muestras",
                    "writers=" + std::to_string(writers.size()) +
                    " readers=" + std::to_string(readers.size()));
            }

            // ── Publication throughput (per writer, luego suma) ───────────
            if (!writers.empty())
            {
                try
                {
                    auto pub_data = StatisticsBackend::get_data(
                        DataKind::PUBLICATION_THROUGHPUT,
                        writers,
                        n_bins_,
                        t_from,
                        now_time,
                        StatisticKind::MEAN);

                    Stats pub = aggregate(pub_data);
                    print_stats("Pub throughput (mean per writer)", pub, "B/s");

                    // Además de la media, sumamos para obtener el throughput
                    // del topic (útil en escenario con varios writers).
                    double total = 0.0;
                    size_t finite = 0;
                    for (const auto& d : pub_data)
                    {
                        if (std::isfinite(d.second)) { total += d.second; ++finite; }
                    }
                    std::cout << "  Pub throughput (sum over writers): "
                              << total << " B/s"
                              << " (finite=" << finite << "/" << pub_data.size() << ")\n";

                    MetricStatus st = classify(pub);
                    write_report_row(
                        "topic", topic_name, "aggregated_writers", "-",
                        "publication_throughput_sum_Bps",
                        std::to_string(total), "B/s",
                        status_to_string(st),
                        "n_writers=" + std::to_string(writers.size()));
                }
                catch (const std::exception& e)
                {
                    std::cout << "  Pub throughput: " << e.what() << "\n";
                    write_report_row(
                        "topic", topic_name, "-", "-",
                        "publication_throughput_sum_Bps", "", "B/s", "NaN", e.what());
                }
            }
            else
            {
                std::cout << "  Pub throughput: no writers\n";
                write_report_row(
                    "topic", topic_name, "-", "-",
                    "publication_throughput_sum_Bps", "", "B/s", "sin muestras",
                    "no writers visible from this node");
            }

            // ── Subscription throughput (per reader, luego suma) ──────────
            if (!readers.empty())
            {
                try
                {
                    auto sub_data = StatisticsBackend::get_data(
                        DataKind::SUBSCRIPTION_THROUGHPUT,
                        readers,
                        n_bins_,
                        t_from,
                        now_time,
                        StatisticKind::MEAN);

                    Stats sub = aggregate(sub_data);
                    print_stats("Sub throughput (mean per reader)", sub, "B/s");

                    double total = 0.0;
                    size_t finite = 0;
                    for (const auto& d : sub_data)
                    {
                        if (std::isfinite(d.second)) { total += d.second; ++finite; }
                    }
                    std::cout << "  Sub throughput (sum over readers): "
                              << total << " B/s"
                              << " (finite=" << finite << "/" << sub_data.size() << ")\n";

                    MetricStatus st = classify(sub);
                    write_report_row(
                        "topic", topic_name, "-", "aggregated_readers",
                        "subscription_throughput_sum_Bps",
                        std::to_string(total), "B/s",
                        status_to_string(st),
                        "n_readers=" + std::to_string(readers.size()));
                }
                catch (const std::exception& e)
                {
                    std::cout << "  Sub throughput: " << e.what() << "\n";
                    write_report_row(
                        "topic", topic_name, "-", "-",
                        "subscription_throughput_sum_Bps", "", "B/s", "NaN", e.what());
                }
            }
            else
            {
                std::cout << "  Sub throughput: no readers\n";
                write_report_row(
                    "topic", topic_name, "-", "-",
                    "subscription_throughput_sum_Bps", "", "B/s", "sin muestras",
                    "no readers visible from this node");
            }

            // ── RESENT_DATAS (señal adelantada de problemas de transmisión)
            if (!writers.empty())
            {
                try
                {
                    auto resent_data = StatisticsBackend::get_data(
                        DataKind::RESENT_DATA,
                        writers,
                        n_bins_,
                        t_from,
                        now_time,
                        StatisticKind::SUM);

                    Stats resent = sum_counter(resent_data);
                    MetricStatus st = classify(resent);

                    if (st == MetricStatus::NO_SAMPLES)
                    {
                        std::cout << "  Resent datas: sin muestras\n";
                    }
                    else if (st == MetricStatus::NAN_ONLY)
                    {
                        std::cout << "  Resent datas: NaN\n";
                    }
                    else if (st == MetricStatus::REAL_ZERO)
                    {
                        std::cout << "  Resent datas: 0 real (sin reenvíos)\n";
                    }
                    else
                    {
                        std::cout << std::fixed << std::setprecision(0)
                                  << "  Resent datas: " << resent.mean
                                  << " packets  (status="
                                  << status_to_string(st) << ")\n";
                    }

                    std::string value = (st == MetricStatus::FINITE ||
                                         st == MetricStatus::MIXED ||
                                         st == MetricStatus::REAL_ZERO)
                        ? std::to_string(static_cast<long long>(resent.mean))
                        : "";
                    write_report_row(
                        "topic", topic_name, "aggregated_writers", "-",
                        "resent_datas_sum", value, "packets",
                        status_to_string(st),
                        "n_writers=" + std::to_string(writers.size()));
                }
                catch (const std::exception& e)
                {
                    std::cout << "  Resent datas: " << e.what() << "\n";
                    write_report_row(
                        "topic", topic_name, "-", "-",
                        "resent_datas_sum", "", "packets", "NaN", e.what());
                }
            }
            else
            {
                std::cout << "  Resent datas: no writers\n";
                write_report_row(
                    "topic", topic_name, "-", "-",
                    "resent_datas_sum", "", "packets", "sin muestras",
                    "no writers visible from this node");
            }

            std::cout << "└─────────────────────────────────────────────────\n";
        }

        // ── optional dump ────────────────────────────────────────────────────
        if (!dump_file_.empty())
        {
            dump_in_file();
        }

        if (reset_)
        {
            clear_inactive_entities();
            std::cout << "  (inactive entities cleared)\n";
        }
    }
}

// ─── Dump / clear ────────────────────────────────────────────────────────────

void Monitor::dump_in_file()
{
    auto t = std::time(nullptr);
    std::tm tm{};

#ifdef _WIN32
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d_%H-%M-%S");
    std::string fname = dump_file_ + "_" + oss.str() + ".json";

    std::cout << "  Dumping DB to " << fname << "\n";
    auto dump = StatisticsBackend::dump_database(reset_);
    std::ofstream file(fname);
    file << std::setw(4) << dump << "\n";
}

void Monitor::clear_inactive_entities()
{
    StatisticsBackend::clear_inactive_entities();
}

// ─── Unused legacy helpers (kept to satisfy monitor.hpp signature) ──────────

std::vector<StatisticsData> Monitor::get_fastdds_latency_mean()
{
    return {};
}

std::vector<StatisticsData> Monitor::get_publication_throughput_mean()
{
    return {};
}

std::string Monitor::timestamp_to_string(const Timestamp timestamp)
{
    auto t = std::chrono::system_clock::to_time_t(timestamp);
    auto msec = duration_cast<milliseconds>(timestamp.time_since_epoch()).count() % 1000;
    std::stringstream ss;

#ifdef _WIN32
    struct tm tm2;
    _localtime64_s(&tm2, &t);
    ss << std::put_time(&tm2, "%F %T");
#else
    ss << std::put_time(localtime(&t), "%F %T");
#endif

    ss << "." << std::setw(3) << std::setfill('0') << msec;
    return ss.str();
}

// ─── Listener callbacks ──────────────────────────────────────────────────────

#define TS() duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count()

void Monitor::Listener::on_participant_discovery(
        EntityId,
        EntityId participant_id,
        const DomainListener::Status& status)
{
    Info info = StatisticsBackend::get_info(participant_id);
    std::string guid = safe_info_to_string(info, GUID_TAG);

    if (status.current_count_change == 1)
    {
        std::cout << "(" << TS() << " ms) Participant discovered: " << guid << "\n";
    }
    else
    {
        std::cout << "(" << TS() << " ms) Participant updated:    " << guid << "\n";
    }
}

void Monitor::Listener::on_datareader_discovery(
        EntityId,
        EntityId datareader_id,
        const DomainListener::Status& status)
{
    Info info = StatisticsBackend::get_info(datareader_id);
    if (info[METATRAFFIC_TAG])
    {
        return;
    }

    std::string guid = safe_info_to_string(info, GUID_TAG);

    if (status.current_count_change == 1)
    {
        std::cout << "(" << TS() << " ms) DataReader  discovered: " << guid << "\n";
    }
    else
    {
        std::cout << "(" << TS() << " ms) DataReader  updated:    " << guid << "\n";
    }
}

void Monitor::Listener::on_datawriter_discovery(
        EntityId,
        EntityId datawriter_id,
        const DomainListener::Status& status)
{
    Info info = StatisticsBackend::get_info(datawriter_id);
    if (info[METATRAFFIC_TAG])
    {
        return;
    }

    std::string guid = safe_info_to_string(info, GUID_TAG);

    if (status.current_count_change == 1)
    {
        std::cout << "(" << TS() << " ms) DataWriter  discovered: " << guid << "\n";
    }
    else
    {
        std::cout << "(" << TS() << " ms) DataWriter  updated:    " << guid << "\n";
    }
}

void Monitor::Listener::on_topic_discovery(
        EntityId,
        EntityId topic_id,
        const DomainListener::Status& status)
{
    Info info = StatisticsBackend::get_info(topic_id);
    if (info[METATRAFFIC_TAG])
    {
        return;
    }

    std::string name = safe_info_to_string(info, NAME_TAG);

    if (status.current_count_change == 1)
    {
        std::cout << "(" << TS() << " ms) Topic       discovered: " << name << "\n";
    }
    else
    {
        std::cout << "(" << TS() << " ms) Topic       updated:    " << name << "\n";
    }
}

void Monitor::Listener::on_host_discovery(
        EntityId host_id,
        const DomainListener::Status& status)
{
    Info info = StatisticsBackend::get_info(host_id);
    std::string name = safe_info_to_string(info, NAME_TAG);

    if (status.current_count_change == 1)
    {
        std::cout << "(" << TS() << " ms) Host        discovered: " << name << "\n";
    }
    else
    {
        std::cout << "(" << TS() << " ms) Host        updated:    " << name << "\n";
    }
}

void Monitor::Listener::on_user_discovery(
        EntityId user_id,
        const DomainListener::Status& status)
{
    Info info = StatisticsBackend::get_info(user_id);
    std::string name = safe_info_to_string(info, NAME_TAG);

    if (status.current_count_change == 1)
    {
        std::cout << "(" << TS() << " ms) User        discovered: " << name << "\n";
    }
    else
    {
        std::cout << "(" << TS() << " ms) User        updated:    " << name << "\n";
    }
}

void Monitor::Listener::on_process_discovery(
        EntityId process_id,
        const DomainListener::Status& status)
{
    Info info = StatisticsBackend::get_info(process_id);
    std::string name = safe_info_to_string(info, NAME_TAG);

    if (status.current_count_change == 1)
    {
        std::cout << "(" << TS() << " ms) Process     discovered: " << name << "\n";
    }
    else
    {
        std::cout << "(" << TS() << " ms) Process     updated:    " << name << "\n";
    }
}

// ─── CLI ─────────────────────────────────────────────────────────────────────

struct Options
{
    uint32_t domain = 0;
    uint16_t bins = 1;
    uint32_t interval_s = 10;
    std::string dump_prefix;
    bool reset = false;
    std::string report_csv = "monitor_report.csv";
    std::string node_id;
};

static void print_usage(const char* prog)
{
    std::cout <<
R"(Usage: )" << prog << R"( [options]

Options:
  -d, --domain   <uint>   DDS domain id              (default: 0)
  -b, --bins     <uint>   Histogram bins             (default: 1)
  -i, --interval <uint>   Report period in seconds   (default: 10)
  -o, --output   <path>   Dump DB to <path>_<ts>.json each cycle
      --reset             Clear inactive entities after each cycle
      --no-reset          Keep inactive entities (default)
      --report-csv <path> Per-cycle metrics CSV      (default: monitor_report.csv)
      --no-report-csv     Disable metrics CSV output
      --node-id <s>       Node identifier            (default: $NODE_ID/$HOSTNAME)
  -h, --help              Show this help

Example:
  )" << prog << R"( --domain 0 --interval 10 --report-csv monitor_report.csv
)";
}

static bool parse_uint(const std::string& s, uint64_t max_val, uint64_t& out)
{
    if (s.empty() || !std::all_of(s.begin(), s.end(), ::isdigit))
    {
        return false;
    }

    try
    {
        unsigned long long v = std::stoull(s);
        if (v > max_val)
        {
            return false;
        }

        out = v;
        return true;
    }
    catch (...)
    {
        return false;
    }
}

static std::optional<Options> parse_args(int argc, char** argv)
{
    Options cfg;

    auto need = [&](int i, const char* name) -> std::optional<std::string>
    {
        if (i + 1 >= argc)
        {
            std::cerr << "Missing value for " << name << "\n";
            return {};
        }
        return std::string(argv[i + 1]);
    };

    for (int i = 1; i < argc; ++i)
    {
        std::string a = argv[i];
        auto pos = a.find('=');
        std::string flag = (pos == std::string::npos) ? a : a.substr(0, pos);
        std::string val = (pos == std::string::npos) ? "" : a.substr(pos + 1);

        if (flag == "-h" || flag == "--help")
        {
            print_usage(argv[0]);
            return {};
        }
        else if (flag == "-d" || flag == "--domain")
        {
            if (val.empty())
            {
                auto v = need(i, "--domain");
                if (!v) return {};
                val = *v;
                ++i;
            }
            uint64_t tmp;
            if (!parse_uint(val, std::numeric_limits<uint32_t>::max(), tmp))
            {
                std::cerr << "Invalid --domain\n";
                return {};
            }
            cfg.domain = static_cast<uint32_t>(tmp);
        }
        else if (flag == "-b" || flag == "--bins")
        {
            if (val.empty())
            {
                auto v = need(i, "--bins");
                if (!v) return {};
                val = *v;
                ++i;
            }
            uint64_t tmp;
            if (!parse_uint(val, std::numeric_limits<uint16_t>::max(), tmp) || tmp == 0)
            {
                std::cerr << "Invalid --bins (must be >= 1)\n";
                return {};
            }
            cfg.bins = static_cast<uint16_t>(tmp);
        }
        else if (flag == "-i" || flag == "--interval")
        {
            if (val.empty())
            {
                auto v = need(i, "--interval");
                if (!v) return {};
                val = *v;
                ++i;
            }
            uint64_t tmp;
            if (!parse_uint(val, std::numeric_limits<uint32_t>::max(), tmp) || tmp == 0)
            {
                std::cerr << "Invalid --interval (must be >= 1)\n";
                return {};
            }
            cfg.interval_s = static_cast<uint32_t>(tmp);
        }
        else if (flag == "-o" || flag == "--output")
        {
            if (val.empty())
            {
                auto v = need(i, "--output");
                if (!v) return {};
                val = *v;
                ++i;
            }
            cfg.dump_prefix = val;
        }
        else if (flag == "--reset")
        {
            cfg.reset = true;
        }
        else if (flag == "--no-reset")
        {
            cfg.reset = false;
        }
        else if (flag == "--report-csv")
        {
            if (val.empty())
            {
                auto v = need(i, "--report-csv");
                if (!v) return {};
                val = *v;
                ++i;
            }
            cfg.report_csv = val;
        }
        else if (flag == "--no-report-csv")
        {
            cfg.report_csv.clear();
        }
        else if (flag == "--node-id")
        {
            if (val.empty())
            {
                auto v = need(i, "--node-id");
                if (!v) return {};
                val = *v;
                ++i;
            }
            cfg.node_id = val;
        }
        else
        {
            std::cerr << "Unknown option: " << a << "\n";
            print_usage(argv[0]);
            return {};
        }
    }

    return cfg;
}

int main(int argc, char** argv)
{
    auto parsed = parse_args(argc, argv);
    if (!parsed)
    {
        return 1;
    }

    const Options cfg = *parsed;

    Monitor monitor;
    if (!monitor.init(
            cfg.domain,
            cfg.bins,
            cfg.interval_s,
            "",
            cfg.dump_prefix,
            cfg.reset,
            cfg.report_csv,
            cfg.node_id))
    {
        std::cerr << "Monitor initialization failed.\n";
        return 1;
    }

    monitor.run();
    return 0;
}
