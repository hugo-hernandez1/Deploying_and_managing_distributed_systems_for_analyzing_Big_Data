#include <fastdds/dds/core/status/PublicationMatchedStatus.hpp>
#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/DataWriterListener.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <tinyxml2.h>
#include "AirSamplePubSubTypes.hpp"

using namespace eprosima::fastdds::dds;

namespace
{

struct AppConfig
{
    std::string csv_path = "data/calair_tiemporeal_ult.csv";
    long magnitud = 12;
    int delay_ms = 250;
    bool use_xml = false;
    std::string xml_path;
    // Identificador lógico del nodo publicador. Si no se pasa, se toma de
    // la variable de entorno NODE_ID o del hostname.
    std::string origin_writer_id;
};

std::vector<std::string> split_semicolon(const std::string& line)
{
    std::vector<std::string> result;
    std::stringstream ss(line);
    std::string item;

    while (std::getline(ss, item, ';'))
    {
        result.push_back(item);
    }

    return result;
}

std::string trim(const std::string& s)
{
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos)
    {
        return "";
    }

    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

bool to_long(const std::string& s, long& value)
{
    try
    {
        std::string t = trim(s);
        size_t idx = 0;
        value = std::stol(t, &idx);
        return idx == t.size();
    }
    catch (...)
    {
        return false;
    }
}

bool to_int(const std::string& s, int& value)
{
    try
    {
        std::string t = trim(s);
        size_t idx = 0;
        value = std::stoi(t, &idx);
        return idx == t.size();
    }
    catch (...)
    {
        return false;
    }
}

bool to_double(const std::string& s, double& value)
{
    try
    {
        std::string t = trim(s);
        if (t.empty())
        {
            return false;
        }

        size_t idx = 0;
        value = std::stod(t, &idx);
        return idx == t.size() && !std::isnan(value);
    }
    catch (...)
    {
        return false;
    }
}

std::string default_origin_id()
{
    if (const char* env = std::getenv("NODE_ID"))
    {
        if (env[0] != '\0')
        {
            return env;
        }
    }
    if (const char* env = std::getenv("HOSTNAME"))
    {
        if (env[0] != '\0')
        {
            return env;
        }
    }
    return "csv_publisher";
}

long long now_ns()
{
    using namespace std::chrono;
    return duration_cast<nanoseconds>(
        system_clock::now().time_since_epoch()).count();
}

void print_usage(const char* progname)
{
    std::cout
        << "Usage:\n"
        << "  " << progname
        << " [csv_path] -c profile.xml [-m magnitud] [-d delay_ms]"
        << " [--origin-id <id>]\n"
        << "  (topic name is always read from the <name> field in topic_profile of the XML)\n";
}

std::string read_topic_name_from_xml(const std::string& xml_path)
{
    tinyxml2::XMLDocument doc;
    if (doc.LoadFile(xml_path.c_str()) != tinyxml2::XML_SUCCESS)
    {
        return "";
    }

    auto* dds = doc.FirstChildElement("dds");
    if (!dds) return "";

    auto* profiles = dds->FirstChildElement("profiles");
    if (!profiles) return "";

    for (auto* topic = profiles->FirstChildElement("topic");
         topic != nullptr;
         topic = topic->NextSiblingElement("topic"))
    {
        const char* profile = topic->Attribute("profile_name");
        if (profile && std::string(profile) == "topic_profile")
        {
            auto* name_el = topic->FirstChildElement("name");
            if (name_el && name_el->GetText())
            {
                return name_el->GetText();
            }
        }
    }

    return "";
}

AppConfig parse_args(int argc, char** argv)
{
    AppConfig cfg;
    std::vector<std::string> positionals;

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
            if (i + 1 >= argc)
            {
                throw std::runtime_error("Missing XML path after -c");
            }

            cfg.use_xml = true;
            cfg.xml_path = argv[++i];
        }
        else if (arg == "-m")
        {
            if (i + 1 >= argc)
            {
                throw std::runtime_error("Missing magnitud value after -m");
            }

            if (!to_long(argv[++i], cfg.magnitud))
            {
                throw std::runtime_error("Invalid magnitud value: " + std::string(argv[i]));
            }
        }
        else if (arg == "-d")
        {
            if (i + 1 >= argc)
            {
                throw std::runtime_error("Missing delay value after -d");
            }

            if (!to_int(argv[++i], cfg.delay_ms))
            {
                throw std::runtime_error("Invalid delay value: " + std::string(argv[i]));
            }
        }
        else if (arg == "--origin-id")
        {
            if (i + 1 >= argc)
            {
                throw std::runtime_error("Missing value after --origin-id");
            }
            cfg.origin_writer_id = argv[++i];
        }
        else
        {
            positionals.push_back(arg);
        }
    }

    if (positionals.size() > 0)
    {
        cfg.csv_path = positionals[0];
    }

    if (positionals.size() > 1)
    {
        throw std::runtime_error("Too many positional arguments");
    }

    if (cfg.origin_writer_id.empty())
    {
        cfg.origin_writer_id = default_origin_id();
    }

    return cfg;
}

class PubListener : public DataWriterListener
{
public:
    std::atomic_int matched_{0};

    void on_publication_matched(
            DataWriter*,
            const PublicationMatchedStatus& info) override
    {
        matched_.store(info.current_count);
    }
};

class CsvPublisher
{
public:
    CsvPublisher()
        : participant_(nullptr)
        , topic_(nullptr)
        , publisher_(nullptr)
        , writer_(nullptr)
    {
    }

    ~CsvPublisher()
    {
        if (publisher_ != nullptr && writer_ != nullptr)
        {
            publisher_->delete_datawriter(writer_);
        }
        if (participant_ != nullptr && publisher_ != nullptr)
        {
            participant_->delete_publisher(publisher_);
        }
        if (participant_ != nullptr && topic_ != nullptr)
        {
            participant_->delete_topic(topic_);
        }
        if (participant_ != nullptr)
        {
            DomainParticipantFactory::get_instance()->delete_participant(participant_);
        }
    }

    bool init(const AppConfig& cfg)
    {
        origin_id_ = cfg.origin_writer_id;

        DomainParticipantFactory* factory = DomainParticipantFactory::get_instance();
        type_.reset(new AirSamplePubSubType());

        if (!cfg.use_xml)
        {
            std::cerr << "This test binary is intended to run with XML profiles only. Use -c <profile.xml>\n";
            return false;
        }

        std::cout << "Loading XML profile: " << cfg.xml_path << std::endl;

        ReturnCode_t rc = factory->load_XML_profiles_file(cfg.xml_path);
        if (rc != RETCODE_OK)
        {
            std::cerr << "Error loading XML: " << cfg.xml_path << std::endl;
            return false;
        }

        participant_ = factory->create_participant_with_profile("publisher_participant_profile");
        if (participant_ == nullptr)
        {
            std::cerr << "Error creating DomainParticipant with XML profile\n";
            return false;
        }

        type_.register_type(participant_);

        std::string topic_name = read_topic_name_from_xml(cfg.xml_path);
        if (topic_name.empty())
        {
            std::cerr << "Could not determine topic name. Add <name> to topic_profile in the XML\n";
            return false;
        }

        std::cout << "Topic name read from XML: " << topic_name << std::endl;
        std::cout << "Origin writer id: " << origin_id_ << std::endl;

        topic_ = participant_->create_topic_with_profile(
            topic_name,
            type_.get_type_name(),
            "topic_profile",
            nullptr,
            StatusMask::all());

        if (topic_ == nullptr)
        {
            std::cerr << "Error creating Topic with XML profile\n";
            return false;
        }

        publisher_ = participant_->create_publisher_with_profile("publisher_profile", nullptr);
        if (publisher_ == nullptr)
        {
            std::cerr << "Error creating Publisher with XML profile\n";
            return false;
        }

        writer_ = publisher_->create_datawriter_with_profile(topic_, "writer_profile", &listener_);
        if (writer_ == nullptr)
        {
            std::cerr << "Error creating DataWriter with XML profile\n";
            return false;
        }

        std::cout << "Publisher initialized on domain "
          << participant_->get_domain_id()
          << std::endl;
        return true;
    }

    void wait_for_match()
    {
        while (listener_.matched_.load() <= 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    bool publish_csv(const std::string& csv_path, long target_magnitud, int delay_ms)
    {
        std::ifstream file(csv_path);
        if (!file.is_open())
        {
            std::cerr << "Could not open CSV: " << csv_path << std::endl;
            return false;
        }

        std::string header_line;
        if (!std::getline(file, header_line))
        {
            std::cerr << "Empty CSV\n";
            return false;
        }

        const auto headers = split_semicolon(header_line);
        std::unordered_map<std::string, size_t> idx;
        for (size_t i = 0; i < headers.size(); ++i)
        {
            idx[trim(headers[i])] = i;
        }

        const std::vector<std::string> required = {
            "MAGNITUD", "ESTACION", "ANO", "MES", "DIA"
        };

        for (const auto& key : required)
        {
            if (idx.find(key) == idx.end())
            {
                std::cerr << "Required column missing: " << key << std::endl;
                return false;
            }
        }

        std::string line;
        size_t published = 0;
        size_t rows_used = 0;

        while (std::getline(file, line))
        {
            if (trim(line).empty())
            {
                continue;
            }

            const auto cols = split_semicolon(line);
            if (cols.size() < headers.size())
            {
                continue;
            }

            long magnitud = 0;
            if (!to_long(cols[idx["MAGNITUD"]], magnitud) || magnitud != target_magnitud)
            {
                continue;
            }

            ++rows_used;

            long year = 0;
            long month = 0;
            long day = 0;
            to_long(cols[idx["ANO"]], year);
            to_long(cols[idx["MES"]], month);
            to_long(cols[idx["DIA"]], day);

            const std::string station = trim(cols[idx["ESTACION"]]);

            for (int h = 1; h <= 24; ++h)
            {
                char hname[4];
                char vname[4];
                std::snprintf(hname, sizeof(hname), "H%02d", h);
                std::snprintf(vname, sizeof(vname), "V%02d", h);

                if (idx.find(hname) == idx.end() || idx.find(vname) == idx.end())
                {
                    continue;
                }

                const std::string validity = trim(cols[idx[vname]]);
                if (validity != "V")
                {
                    continue;
                }

                double value = 0.0;
                if (!to_double(cols[idx[hname]], value))
                {
                    continue;
                }

                // Contador secuencial por magnitud.
                long long seq = ++seq_per_magnitude_[magnitud];

                AirSample sample;
                sample.station(station);
                sample.magnitud(magnitud);
                sample.year(year);
                sample.month(month);
                sample.day(day);
                sample.hour(h);
                sample.value(value);
                sample.validity(validity);

                // Trazabilidad:
                sample.magnitude_id(magnitud);
                sample.sequence_id(seq);
                sample.origin_writer_id(origin_id_);
                sample.publish_ts_ns(now_ns());

                if (writer_->write(&sample) == RETCODE_OK)
                {
                    ++published;
                    std::cout
                        << "Published -> station=" << station
                        << " magnitude_id=" << magnitud
                        << " seq=" << seq
                        << " date=" << year << "-" << month << "-" << day
                        << " hour=" << h
                        << " value=" << value
                        << " validity=" << validity
                        << std::endl;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            }
        }

        std::cout << "\nRows used: " << rows_used << std::endl;
        std::cout << "Samples published: " << published << std::endl;
        return true;
    }

private:
    DomainParticipant* participant_;
    Topic* topic_;
    Publisher* publisher_;
    DataWriter* writer_;
    TypeSupport type_;
    PubListener listener_;

    std::string origin_id_;
    // sequence_id monotónico por magnitude_id.
    std::map<long, long long> seq_per_magnitude_;
};

} // namespace

int main(int argc, char** argv)
{
    try
    {
        AppConfig cfg = parse_args(argc, argv);

        CsvPublisher publisher;
        if (!publisher.init(cfg))
        {
            return 1;
        }

        publisher.wait_for_match();

        if (!publisher.publish_csv(cfg.csv_path, cfg.magnitud, cfg.delay_ms))
        {
            return 1;
        }

        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        print_usage(argv[0]);
        return 1;
    }
}
