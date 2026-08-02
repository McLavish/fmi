#ifndef FMI_PYTHONCOMMUNICATOR_H
#define FMI_PYTHONCOMMUNICATOR_H

#include <Communicator.h>
#include <memory>

#include "PythonBindingSupport.h"

namespace FMI::Utils {
    class PythonCommunicator : private PythonBindingSupport {
    public:
        PythonCommunicator(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path, std::string comm_name,
                           unsigned int faas_memory = 128);

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

    private:
        std::shared_ptr<FMI::Communicator> comm;
        FMI::Utils::peer_num peer_id;
        FMI::Utils::peer_num num_peers;
    };
}

#endif //FMI_PYTHONCOMMUNICATOR_H
