#include <boost/test/unit_test.hpp>

#include "../include/fmi.h"

#include <chrono>
#include <future>

namespace {
    const std::string ft_config_path = "../../config/fmi_ft_test.json";
    const std::string missing_direct_config_path = "../../config/fmi_ft_direct_missing.json";

    std::string unique_comm_name() {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
        return "ft-tests-" + std::to_string(nanos);
    }

    bool redis_available(const std::string& comm_name) {
        try {
            FMI::FT::Coordinator coordinator(ft_config_path, comm_name, 2);
            coordinator.clear_job_state();
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }
}

BOOST_AUTO_TEST_SUITE(FaultTolerance);

BOOST_AUTO_TEST_CASE(preferred_backend_requires_direct) {
    BOOST_CHECK_THROW(FMI::Communicator(0, 2, missing_direct_config_path, "ft-missing"), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(session_reconfigures_epoch_after_migration) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(comm_name)) {
        BOOST_TEST_MESSAGE("Skipping FT session migration test because Redis is unavailable");
        return;
    }

    FMI::FT::Coordinator coordinator(ft_config_path, comm_name, 2);
    coordinator.clear_job_state();

    FMI::FT::Session non_target(0, 2, ft_config_path, comm_name, "worker-a");
    FMI::FT::Session target(1, 2, ft_config_path, comm_name, "worker-b");

    coordinator.request_migration(1);

    auto non_target_result = std::async(std::launch::async, [&non_target]() {
        return non_target.safe_point();
    });

    BOOST_CHECK(target.safe_point() == FMI::FT::Event::MigrateSelf);

    FMI::FT::Session replacement(1, 2, ft_config_path, comm_name, "worker-c");
    BOOST_CHECK(replacement.safe_point() == FMI::FT::Event::Reconfigured);
    BOOST_CHECK(non_target_result.get() == FMI::FT::Event::Reconfigured);

    BOOST_CHECK_EQUAL(non_target.epoch(), 1);
    BOOST_CHECK_EQUAL(replacement.epoch(), 1);
    BOOST_CHECK_EQUAL(non_target.comm().get_comm_name(), comm_name + "@epoch=1");
    BOOST_CHECK_EQUAL(replacement.comm().get_comm_name(), comm_name + "@epoch=1");
    BOOST_CHECK(non_target.safe_point() == FMI::FT::Event::None);

    coordinator.clear_job_state();
}

BOOST_AUTO_TEST_SUITE_END();
