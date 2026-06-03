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
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <tinyxml2.h>
#include "AirSamplePubSubTypes.hpp"

using namespace eprosima::fastdds::dds;

namespace
{

struct AppConfig
{
    bool use_xml = false;
    std::string xml_path;
};

std::atomic_bool g_running(true);

void signal_handler(int)
{
    g_running = false;
}

void print_usage(const char* progname)
{
    std::cout
        << "Usage:\n"
        << "  " << progname << " -c profile.xml\n"
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
    std::vector<std::string> extra_args;

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
        else
        {
            extra_args.push_back(arg);
        }
    }

    if (!extra_args.empty())
    {
        throw std::runtime_error("Unrecognized arguments: " + extra_args[0]);
    }

    return cfg;
}

class SampleListener : public DataReaderListener
{
public:
    std::atomic_int matched_{0};

    void on_subscription_matched(
            DataReader*,
            const SubscriptionMatchedStatus& info) override
    {
        matched_.store(info.current_count);

        if (info.current_count_change == 1)
        {
            std::cout << "[MATCH] Publisher matched. total=" << info.current_count << std::endl;
        }
        else if (info.current_count_change == -1)
        {
            std::cout << "[MATCH] Publisher unmatched. total=" << info.current_count << std::endl;
        }
        else
        {
            std::cout << "[MATCH] Subscription status changed. total=" << info.current_count
                      << " change=" << info.current_count_change << std::endl;
        }
    }

    void on_data_available(DataReader* reader) override
    {
        AirSample sample;
        SampleInfo info;

        while (reader->take_next_sample(&sample, &info) == RETCODE_OK)
        {
            if (info.valid_data)
            {
                std::cout
                    << "Received -> station=" << sample.station()
                    << " magnitud=" << sample.magnitud()
                    << " date=" << sample.year() << "-" << sample.month() << "-" << sample.day()
                    << " hour=" << sample.hour()
                    << " value=" << sample.value()
                    << " validity=" << sample.validity()
                    << " matched_publishers=" << matched_.load()
                    << std::endl;
            }
        }
    }
};

class CsvSubscriber
{
public:
    CsvSubscriber()
        : participant_(nullptr)
        , topic_(nullptr)
        , subscriber_(nullptr)
        , reader_(nullptr)
    {
    }

    ~CsvSubscriber()
    {
        if (subscriber_ != nullptr && reader_ != nullptr)
        {
            subscriber_->delete_datareader(reader_);
        }
        if (participant_ != nullptr && subscriber_ != nullptr)
        {
            participant_->delete_subscriber(subscriber_);
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
        if (!cfg.use_xml)
        {
            std::cerr << "This test binary is intended to run with XML profiles only. Use -c <profile.xml>\n";
            return false;
        }

        DomainParticipantFactory* factory = DomainParticipantFactory::get_instance();
        type_.reset(new AirSamplePubSubType());

        std::cout << "Loading XML profile: " << cfg.xml_path << std::endl;

        ReturnCode_t rc = factory->load_XML_profiles_file(cfg.xml_path);
        if (rc != RETCODE_OK)
        {
            std::cerr << "Error loading XML: " << cfg.xml_path << std::endl;
            return false;
        }

        participant_ = factory->create_participant_with_profile("subscriber_participant_profile");
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

        subscriber_ = participant_->create_subscriber_with_profile("subscriber_profile", nullptr);
        if (subscriber_ == nullptr)
        {
            std::cerr << "Error creating Subscriber with XML profile\n";
            return false;
        }

        reader_ = subscriber_->create_datareader_with_profile(topic_, "reader_profile", &listener_);
        if (reader_ == nullptr)
        {
            std::cerr << "Error creating DataReader with XML profile\n";
            return false;
        }

        std::cout << "Subscriber initialized with XML on domain "
          << participant_->get_domain_id()
          << std::endl;
        return true;
    }

    void run()
    {
        std::cout << "Subscriber started. matched_publishers=" << listener_.matched_.load() << std::endl;
        while (g_running)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

private:
    DomainParticipant* participant_;
    Topic* topic_;
    Subscriber* subscriber_;
    DataReader* reader_;
    TypeSupport type_;
    SampleListener listener_;
};

} // namespace

int main(int argc, char** argv)
{
    std::signal(SIGINT, signal_handler);

    try
    {
        AppConfig cfg = parse_args(argc, argv);

        CsvSubscriber subscriber;
        if (!subscriber.init(cfg))
        {
            return 1;
        }

        subscriber.run();
        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        print_usage(argv[0]);
        return 1;
    }
}