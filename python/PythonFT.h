#ifndef FMI_PYTHONFT_H
#define FMI_PYTHONFT_H

#include <ft/Coordinator.h>
#include <ft/Session.h>
#include <memory>
#include <string>

#include "PythonBindingSupport.h"

namespace FMI::Utils {
    class PythonFTSession : private PythonBindingSupport {
    public:
        PythonFTSession(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path, std::string comm_name,
                        std::string worker_id = "", unsigned int faas_memory = 128);

        void send(const boost::python::object& py_obj, FMI::Utils::peer_num dst, FMI::Utils::PythonData type);
        boost::python::object recv(FMI::Utils::peer_num src, FMI::Utils::PythonData type);
        boost::python::object bcast(const boost::python::object& src_data, FMI::Utils::peer_num root, FMI::Utils::PythonData type);
        void barrier();
        boost::python::object gather(const boost::python::object& src_data, FMI::Utils::peer_num root, FMI::Utils::PythonData snd_type);
        boost::python::object scatter(const boost::python::object& src_data, FMI::Utils::peer_num root, FMI::Utils::PythonData snd_type);
        boost::python::object reduce(const boost::python::object& src_data, FMI::Utils::peer_num root, FMI::Utils::PythonFunc f,
                                     FMI::Utils::PythonData type);
        boost::python::object allreduce(const boost::python::object& src_data, FMI::Utils::PythonFunc f, FMI::Utils::PythonData type);
        boost::python::object scan(const boost::python::object& src_data, FMI::Utils::PythonFunc f, FMI::Utils::PythonData type);
        void hint(FMI::Utils::Hint hint);

        FMI::FT::Event safe_point();
        [[nodiscard]] std::uint64_t epoch() const;

    private:
        std::shared_ptr<FMI::FT::Session> session;
        FMI::Utils::peer_num peer_id;
        FMI::Utils::peer_num num_peers;
    };

    class PythonFTCoordinator {
    public:
        PythonFTCoordinator(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers);

        void request_migration(FMI::Utils::peer_num rank);
        void clear_job_state();
        [[nodiscard]] std::uint64_t epoch() const;

    private:
        std::shared_ptr<FMI::FT::Coordinator> coordinator;
    };
}

#endif //FMI_PYTHONFT_H
