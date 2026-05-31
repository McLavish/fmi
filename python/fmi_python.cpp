#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <boost/python/stl_iterator.hpp>
#include <Communicator.h>
#include <comm/Data.h>
#include "PythonCommunicator.h"
#include "PythonFT.h"

using namespace boost::python;



BOOST_PYTHON_MODULE(fmi)
{
    enum_<FMI::FT::Event>("ft_events")
        .value("none", FMI::FT::Event::None)
        .value("migrate_self", FMI::FT::Event::MigrateSelf)
        .value("reconfigured", FMI::FT::Event::Reconfigured)
    ;

    class_<FMI::Utils::PythonCommunicator>("Communicator", init<FMI::Utils::peer_num, FMI::Utils::peer_num, std::string, std::string, optional<unsigned int> >())
        .def("send", &FMI::Utils::PythonCommunicator::send)
        .def("recv", &FMI::Utils::PythonCommunicator::recv)
        .def("bcast", &FMI::Utils::PythonCommunicator::bcast)
        .def("barrier", &FMI::Utils::PythonCommunicator::barrier)
        .def("gather", &FMI::Utils::PythonCommunicator::gather)
        .def("scatter", &FMI::Utils::PythonCommunicator::scatter)
        .def("reduce", &FMI::Utils::PythonCommunicator::reduce)
        .def("allreduce", &FMI::Utils::PythonCommunicator::allreduce)
        .def("scan", &FMI::Utils::PythonCommunicator::scan)
        .def("hint", &FMI::Utils::PythonCommunicator::hint)
    ;

    class_<FMI::Utils::PythonFTSession>("FTSession", init<FMI::Utils::peer_num, FMI::Utils::peer_num, std::string, std::string,
                                                         optional<std::string, unsigned int, std::string> >())
        .def("send", &FMI::Utils::PythonFTSession::send)
        .def("recv", &FMI::Utils::PythonFTSession::recv)
        .def("bcast", &FMI::Utils::PythonFTSession::bcast)
        .def("barrier", &FMI::Utils::PythonFTSession::barrier)
        .def("gather", &FMI::Utils::PythonFTSession::gather)
        .def("scatter", &FMI::Utils::PythonFTSession::scatter)
        .def("reduce", &FMI::Utils::PythonFTSession::reduce)
        .def("allreduce", &FMI::Utils::PythonFTSession::allreduce)
        .def("scan", &FMI::Utils::PythonFTSession::scan)
        .def("hint", &FMI::Utils::PythonFTSession::hint)
        .def("safe_point", &FMI::Utils::PythonFTSession::safe_point)
        .def("epoch", &FMI::Utils::PythonFTSession::epoch)
    ;

    class_<FMI::Utils::PythonRankDirectoryEntry>("RankDirectoryEntry")
        .def_readonly("rank", &FMI::Utils::PythonRankDirectoryEntry::rank)
        .def_readonly("worker_id", &FMI::Utils::PythonRankDirectoryEntry::worker_id)
        .def_readonly("placement", &FMI::Utils::PythonRankDirectoryEntry::placement)
        .def_readonly("state", &FMI::Utils::PythonRankDirectoryEntry::state)
    ;

    class_<FMI::Utils::PythonFTCoordinator>("FTCoordinator", init<std::string, std::string, FMI::Utils::peer_num>())
        .def("request_migration", &FMI::Utils::PythonFTCoordinator::request_migration)
        .def("clear_job_state", &FMI::Utils::PythonFTCoordinator::clear_job_state)
        .def("epoch", &FMI::Utils::PythonFTCoordinator::epoch)
        .def("placement_for_rank", &FMI::Utils::PythonFTCoordinator::placement_for_rank)
        .def("directory_snapshot", &FMI::Utils::PythonFTCoordinator::directory_snapshot)
    ;

    enum_<FMI::Utils::PythonType>("datatypes")
        .value("int", FMI::Utils::PythonType::INT)
        .value("double", FMI::Utils::PythonType::DOUBLE)
        .value("int_list", FMI::Utils::PythonType::INT_LIST)
        .value("double_list", FMI::Utils::PythonType::DOUBLE_LIST)
    ;

    class_<FMI::Utils::PythonData>("types", init<FMI::Utils::PythonType>())
        .def(init<FMI::Utils::PythonType, unsigned int>())
    ;

    enum_<FMI::Utils::PythonOp>("op")
        .value("sum", FMI::Utils::PythonOp::SUM)
        .value("prod", FMI::Utils::PythonOp::PROD)
        .value("max", FMI::Utils::PythonOp::MAX)
        .value("min", FMI::Utils::PythonOp::MIN)
        .value("custom", FMI::Utils::PythonOp::CUSTOM)
    ;

    class_<FMI::Utils::PythonFunc>("func", init<FMI::Utils::PythonOp>())
        .def(init<FMI::Utils::PythonOp, boost::python::object, bool, bool>())
    ;

    enum_<FMI::Utils::Hint>("hints")
        .value("cheap", FMI::Utils::Hint::cheap)
        .value("fast", FMI::Utils::Hint::fast)
    ;
}
