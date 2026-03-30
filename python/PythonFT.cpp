#include "PythonFT.h"

#include <boost/python/extract.hpp>

FMI::Utils::PythonFTSession::PythonFTSession(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path,
                                             std::string comm_name, std::string worker_id, unsigned int faas_memory) {
    session = std::make_shared<FMI::FT::Session>(peer_id, num_peers, std::move(config_path), std::move(comm_name), std::move(worker_id),
                                                 faas_memory);
    this->peer_id = peer_id;
    this->num_peers = num_peers;
}

void FMI::Utils::PythonFTSession::send(const boost::python::object& py_obj, FMI::Utils::peer_num dst, FMI::Utils::PythonData type) {
    auto& comm = session->comm();
    if (type.type == INT) {
        FMI::Comm::Data<int> data = extract_object<int>(py_obj);
        comm.send<int>(data, dst);
    } else if (type.type == DOUBLE) {
        FMI::Comm::Data<double> data = extract_object<double>(py_obj);
        comm.send<double>(data, dst);
    } else if (type.type == INT_LIST) {
        FMI::Comm::Data<std::vector<int>> data = extract_list<int>(py_obj);
        comm.send<std::vector<int>>(data, dst);
    } else if (type.type == DOUBLE_LIST) {
        FMI::Comm::Data<std::vector<double>> data = extract_list<double>(py_obj);
        comm.send<std::vector<double>>(data, dst);
    } else {
        throw std::runtime_error("Unknown type passed");
    }
}

boost::python::object FMI::Utils::PythonFTSession::recv(FMI::Utils::peer_num src, FMI::Utils::PythonData type) {
    auto& comm = session->comm();
    if (type.type == INT) {
        FMI::Comm::Data<int> data;
        comm.recv<int>(data, src);
        return boost::python::object(data.get());
    }
    if (type.type == DOUBLE) {
        FMI::Comm::Data<double> data;
        comm.recv<double>(data, src);
        return boost::python::object(data.get());
    }
    if (type.type == INT_LIST) {
        FMI::Comm::Data<std::vector<int>> data(type.num_objects);
        comm.recv<std::vector<int>>(data, src);
        return boost::python::object(to_list<int>(data.get()));
    }
    if (type.type == DOUBLE_LIST) {
        FMI::Comm::Data<std::vector<double>> data(type.num_objects);
        comm.recv<std::vector<double>>(data, src);
        return boost::python::object(to_list<double>(data.get()));
    }
    throw std::runtime_error("Unknown type passed");
}

boost::python::object FMI::Utils::PythonFTSession::bcast(const boost::python::object& src_data, FMI::Utils::peer_num root,
                                                         FMI::Utils::PythonData type) {
    auto& comm = session->comm();
    if (type.type == INT) {
        FMI::Comm::Data<int> data;
        if (peer_id == root) {
            data = extract_object<int>(src_data);
        }
        comm.bcast<int>(data, root);
        return boost::python::object(data.get());
    }
    if (type.type == DOUBLE) {
        FMI::Comm::Data<double> data;
        if (peer_id == root) {
            data = extract_object<double>(src_data);
        }
        comm.bcast<double>(data, root);
        return boost::python::object(data.get());
    }
    if (type.type == INT_LIST) {
        FMI::Comm::Data<std::vector<int>> data(type.num_objects);
        if (peer_id == root) {
            data = extract_list<int>(src_data);
        }
        comm.bcast<std::vector<int>>(data, root);
        return boost::python::object(to_list<int>(data.get()));
    }
    if (type.type == DOUBLE_LIST) {
        FMI::Comm::Data<std::vector<double>> data(type.num_objects);
        if (peer_id == root) {
            data = extract_list<double>(src_data);
        }
        comm.bcast<std::vector<double>>(data, root);
        return boost::python::object(to_list<double>(data.get()));
    }
    throw std::runtime_error("Unknown type passed");
}

void FMI::Utils::PythonFTSession::barrier() {
    session->comm().barrier();
}

boost::python::object FMI::Utils::PythonFTSession::gather(const boost::python::object& src_data, FMI::Utils::peer_num root,
                                                          FMI::Utils::PythonData snd_type) {
    auto& comm = session->comm();
    if (snd_type.type == INT || snd_type.type == INT_LIST) {
        std::vector<int> value_list;
        if (snd_type.type == INT) {
            snd_type.num_objects = 1;
            value_list.emplace_back(extract_object<int>(src_data));
        } else {
            value_list = extract_list<int>(src_data);
        }
        FMI::Comm::Data<std::vector<int>> send_data(value_list);
        FMI::Comm::Data<std::vector<int>> recv_data;
        if (peer_id == root) {
            recv_data = FMI::Comm::Data<std::vector<int>>(snd_type.num_objects * num_peers);
        }
        comm.gather<std::vector<int>>(send_data, recv_data, root);
        return boost::python::object(to_list<int>(recv_data.get()));
    }
    if (snd_type.type == DOUBLE || snd_type.type == DOUBLE_LIST) {
        std::vector<double> value_list;
        if (snd_type.type == DOUBLE) {
            snd_type.num_objects = 1;
            value_list.emplace_back(extract_object<double>(src_data));
        } else {
            value_list = extract_list<double>(src_data);
        }
        FMI::Comm::Data<std::vector<double>> send_data(value_list);
        FMI::Comm::Data<std::vector<double>> recv_data;
        if (peer_id == root) {
            recv_data = FMI::Comm::Data<std::vector<double>>(snd_type.num_objects * num_peers);
        }
        comm.gather<std::vector<double>>(send_data, recv_data, root);
        return boost::python::object(to_list<double>(recv_data.get()));
    }
    throw std::runtime_error("Unknown type passed");
}

boost::python::object FMI::Utils::PythonFTSession::scatter(const boost::python::object& src_data, FMI::Utils::peer_num root,
                                                           FMI::Utils::PythonData snd_type) {
    auto& comm = session->comm();
    if (snd_type.num_objects % num_peers != 0) {
        throw std::runtime_error("List length not divisible by number of peers");
    }
    if (snd_type.type == INT || snd_type.type == DOUBLE) {
        throw std::runtime_error("Cannot scatter atomic types");
    }
    if (snd_type.type == INT_LIST) {
        FMI::Comm::Data<std::vector<int>> recv_data(snd_type.num_objects / num_peers);
        FMI::Comm::Data<std::vector<int>> send_data;
        if (peer_id == root) {
            send_data = extract_list<int>(src_data);
        }
        comm.scatter<std::vector<int>>(send_data, recv_data, root);
        return boost::python::object(to_list<int>(recv_data.get()));
    }
    if (snd_type.type == DOUBLE_LIST) {
        FMI::Comm::Data<std::vector<double>> recv_data(snd_type.num_objects / num_peers);
        FMI::Comm::Data<std::vector<double>> send_data;
        if (peer_id == root) {
            send_data = extract_list<double>(src_data);
        }
        comm.scatter<std::vector<double>>(send_data, recv_data, root);
        return boost::python::object(to_list<double>(recv_data.get()));
    }
    throw std::runtime_error("Unknown type passed");
}

boost::python::object FMI::Utils::PythonFTSession::reduce(const boost::python::object& src_data, FMI::Utils::peer_num root,
                                                          FMI::Utils::PythonFunc f, FMI::Utils::PythonData type) {
    auto& comm = session->comm();
    if (type.type == INT) {
        FMI::Comm::Data<int> send_buf(extract_object<int>(src_data));
        FMI::Comm::Data<int> recv_buf;
        comm.reduce(send_buf, recv_buf, root, get_function<int>(f));
        return boost::python::object(recv_buf.get());
    }
    if (type.type == DOUBLE) {
        FMI::Comm::Data<double> send_buf(extract_object<double>(src_data));
        FMI::Comm::Data<double> recv_buf;
        comm.reduce(send_buf, recv_buf, root, get_function<double>(f));
        return boost::python::object(recv_buf.get());
    }
    if (type.type == INT_LIST) {
        FMI::Comm::Data<std::vector<int>> send_buf(extract_list<int>(src_data));
        FMI::Comm::Data<std::vector<int>> recv_buf(type.num_objects);
        comm.reduce(send_buf, recv_buf, root, get_vec_function<int>(f));
        return boost::python::object(to_list<int>(recv_buf.get()));
    }
    if (type.type == DOUBLE_LIST) {
        FMI::Comm::Data<std::vector<double>> send_buf(extract_list<double>(src_data));
        FMI::Comm::Data<std::vector<double>> recv_buf(type.num_objects);
        comm.reduce(send_buf, recv_buf, root, get_vec_function<double>(f));
        return boost::python::object(to_list<double>(recv_buf.get()));
    }
    throw std::runtime_error("Unknown type passed");
}

boost::python::object FMI::Utils::PythonFTSession::allreduce(const boost::python::object& src_data, FMI::Utils::PythonFunc f,
                                                             FMI::Utils::PythonData type) {
    auto& comm = session->comm();
    if (type.type == INT) {
        FMI::Comm::Data<int> send_buf(extract_object<int>(src_data));
        FMI::Comm::Data<int> recv_buf;
        comm.allreduce(send_buf, recv_buf, get_function<int>(f));
        return boost::python::object(recv_buf.get());
    }
    if (type.type == DOUBLE) {
        FMI::Comm::Data<double> send_buf(extract_object<double>(src_data));
        FMI::Comm::Data<double> recv_buf;
        comm.allreduce(send_buf, recv_buf, get_function<double>(f));
        return boost::python::object(recv_buf.get());
    }
    if (type.type == INT_LIST) {
        FMI::Comm::Data<std::vector<int>> send_buf(extract_list<int>(src_data));
        FMI::Comm::Data<std::vector<int>> recv_buf(type.num_objects);
        comm.allreduce(send_buf, recv_buf, get_vec_function<int>(f));
        return boost::python::object(to_list<int>(recv_buf.get()));
    }
    if (type.type == DOUBLE_LIST) {
        FMI::Comm::Data<std::vector<double>> send_buf(extract_list<double>(src_data));
        FMI::Comm::Data<std::vector<double>> recv_buf(type.num_objects);
        comm.allreduce(send_buf, recv_buf, get_vec_function<double>(f));
        return boost::python::object(to_list<double>(recv_buf.get()));
    }
    throw std::runtime_error("Unknown type passed");
}

boost::python::object FMI::Utils::PythonFTSession::scan(const boost::python::object& src_data, FMI::Utils::PythonFunc f,
                                                        FMI::Utils::PythonData type) {
    auto& comm = session->comm();
    if (type.type == INT) {
        FMI::Comm::Data<int> send_buf(extract_object<int>(src_data));
        FMI::Comm::Data<int> recv_buf;
        comm.scan(send_buf, recv_buf, get_function<int>(f));
        return boost::python::object(recv_buf.get());
    }
    if (type.type == DOUBLE) {
        FMI::Comm::Data<double> send_buf(extract_object<double>(src_data));
        FMI::Comm::Data<double> recv_buf;
        comm.scan(send_buf, recv_buf, get_function<double>(f));
        return boost::python::object(recv_buf.get());
    }
    if (type.type == INT_LIST) {
        FMI::Comm::Data<std::vector<int>> send_buf(extract_list<int>(src_data));
        FMI::Comm::Data<std::vector<int>> recv_buf(type.num_objects);
        comm.scan(send_buf, recv_buf, get_vec_function<int>(f));
        return boost::python::object(to_list<int>(recv_buf.get()));
    }
    if (type.type == DOUBLE_LIST) {
        FMI::Comm::Data<std::vector<double>> send_buf(extract_list<double>(src_data));
        FMI::Comm::Data<std::vector<double>> recv_buf(type.num_objects);
        comm.scan(send_buf, recv_buf, get_vec_function<double>(f));
        return boost::python::object(to_list<double>(recv_buf.get()));
    }
    throw std::runtime_error("Unknown type passed");
}

void FMI::Utils::PythonFTSession::hint(FMI::Utils::Hint hint) {
    session->comm().hint(hint);
}

FMI::FT::Event FMI::Utils::PythonFTSession::safe_point() {
    return session->safe_point();
}

std::uint64_t FMI::Utils::PythonFTSession::epoch() const {
    return session->epoch();
}

FMI::Utils::PythonFTCoordinator::PythonFTCoordinator(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers) {
    coordinator = std::make_shared<FMI::FT::Coordinator>(std::move(config_path), std::move(comm_name), num_peers);
}

void FMI::Utils::PythonFTCoordinator::request_migration(FMI::Utils::peer_num rank) {
    coordinator->request_migration(rank);
}

void FMI::Utils::PythonFTCoordinator::clear_job_state() {
    coordinator->clear_job_state();
}

std::uint64_t FMI::Utils::PythonFTCoordinator::epoch() const {
    return coordinator->epoch();
}
