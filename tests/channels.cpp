#include <boost/test/unit_test.hpp>

#include "forked_rank_guard.h"

#include "../include/comm/Channel.h"
#if FMI_ENABLE_TCPUNCH
#include <tcpunch.h>
#endif
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <new>
#include <numeric>
#include <thread>
#include <vector>
#include <ctime>
#include <omp.h>
#include <sys/mman.h>
#include <sys/wait.h>

BOOST_AUTO_TEST_SUITE(Channels);

std::map<std::string, std::string> s3_test_params = {
        {"bucket_name", "romanboe-uploadtest"},
        {"s3_region", "eu-central-1"},
        {"timeout", "100"},
        {"max_timeout", "1000"}
};

std::map<std::string, std::string> s3_test_model_params = {
        {"bandwidth", "50.0"},
        {"overhead", "40.4"},
        {"transfer_price", "0.0"},
        {"download_price", "0.00000043"},
        {"upload_price", "0.0000054"}
};

std::map<std::string, std::string> redis_test_params = {
        {"host", "127.0.0.1"},
        {"port", "6379"},
        {"timeout", "1"},
        {"max_timeout", "1000"}
};

std::map<std::string, std::string> redis_test_model_params = {
        {"bandwidth_single", "100.0"},
        {"bandwidth_multiple", "400.0"},
        {"overhead", "5.2"},
        {"transfer_price", "0.0"},
        {"instance_price", "0.0038"},
        {"requests_per_hour", "1000"},
        {"include_infrastructure_costs", "true"}
};

std::map<std::string, std::string> direct_test_params = {
        {"host", "127.0.0.1"},
        {"port", "10000"},
        {"max_timeout", "1000"}
};

std::map<std::string, std::string> direct_test_model_params = {
        {"bandwidth", "250.0"},
        {"overhead", "0.34"},
        {"transfer_price", "0.0"},
        {"vm_price", "0.0134"},
        {"requests_per_hour", "1000"},
        {"include_infrastructure_costs", "true"}
};

std::map< std::string, std::pair< std::map<std::string, std::string>, std::map<std::string, std::string> > > backends = {
        //{"S3", {s3_test_params, s3_test_model_params}},
       // {"Redis", {redis_test_params, redis_test_model_params}},
#if FMI_ENABLE_TCPUNCH
        {"Direct", {direct_test_params, direct_test_model_params}},
#endif
};

std::string comm_name = std::to_string(std::time(nullptr)) + "Tests";

BOOST_AUTO_TEST_CASE(sending_receiving) {
    for (auto const & backend_data : backends) {
        // Using C++ 17 [key, val] : map syntax here does not compile (on some clang versions) in combination with the omp section because of a clang bug:
        // https://stackoverflow.com/questions/65819317/openmp-clang-sometimes-fail-with-a-variable-declared-from-structured-binding
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;

        int val = 42;
        int recv;
        #pragma omp parallel num_threads(2)
        {
            int tid = omp_get_thread_num();
            auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
            ch->set_peer_id(tid);
            ch->set_num_peers(2);
            ch->set_comm_name(comm_name);
            if (tid == 0) {
                channel_data buf {reinterpret_cast<char*>(&val), sizeof(val)};
                ch->send(buf, 1);
            } else if (tid == 1) {
                channel_data recv_buf {reinterpret_cast<char*>(&recv), sizeof(recv)};
                ch->recv(recv_buf, 0);
            }
            ch->finalize();
        }
        BOOST_CHECK_EQUAL(val, recv);
    }
}

BOOST_AUTO_TEST_CASE(sending_receiving_mult_times) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;

        int val1 = 42;
        int val2 = 4242;
        int recv1, recv2;
        #pragma omp parallel num_threads(2)
        {
            int tid = omp_get_thread_num();
            auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
            ch->set_peer_id(tid);
            ch->set_num_peers(2);
            ch->set_comm_name(comm_name);
            if (tid == 0) {
                ch->send({reinterpret_cast<char*>(&val1), sizeof(val1)}, 1);
                ch->send({reinterpret_cast<char*>(&val2), sizeof(val2)}, 1);
            } else if (tid == 1) {
                ch->recv({reinterpret_cast<char*>(&recv1), sizeof(recv1)}, 0);
                ch->recv({reinterpret_cast<char*>(&recv2), sizeof(recv2)}, 0);
            }
            ch->finalize();
        }
        BOOST_CHECK_EQUAL(val1, recv1);
        BOOST_CHECK_EQUAL(val2, recv2);
    }
}

BOOST_AUTO_TEST_CASE(bcast) {
    for (auto const & backend_data : backends) {
        // Using many threads leads to race conditions (in the AWS SDK, raw sockets, hiredis, ...), therefore processes are used for these tests
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        FMI::Utils::peer_num root = 14;
        constexpr int num_peers = 32;
        int* vals = static_cast<int*>(mmap(nullptr, num_peers * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        vals[root] = 42;
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        ch->bcast({reinterpret_cast<char*>(&vals[peer_id]), sizeof(vals[peer_id])}, root);
        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            for (int i = 0; i < num_peers; i++) {
                BOOST_CHECK_EQUAL(vals[i], 42);
            }
        } else {
            exit(0);
        }

    }
}

BOOST_AUTO_TEST_CASE(barrier_unsucc) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        constexpr int num_peers = 4;
        bool* caught = static_cast<bool*>(mmap(nullptr, num_peers * sizeof(bool), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        std::chrono::steady_clock::time_point bef = std::chrono::steady_clock::now();
        if (peer_id != 1) {
            try {
                ch->barrier();
            } catch (FMI::Utils::Timeout) {
                caught[peer_id] = true;
            }

        }
        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            for (int i = 0; i < num_peers; i++) {
                if (i != 1) {
                    BOOST_CHECK_EQUAL(caught[i], true);
                }
            }
        } else {
            exit(0);
        }
    }
}

BOOST_AUTO_TEST_CASE(barrier_succ) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        constexpr int num_peers = 2;
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        std::chrono::steady_clock::time_point bef = std::chrono::steady_clock::now();
        ch->barrier();
        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(after - bef).count();
            BOOST_TEST(elapsed_ms < std::stoi(test_params["max_timeout"]));
        } else {
            exit(0);
        }
    }

}

BOOST_AUTO_TEST_CASE(gather_one) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        constexpr int num_peers = 2;
        std::vector<int> vals {1,2,3,4};
        FMI::Utils::peer_num root = 1;

        int* rcv_vals = static_cast<int*>(mmap(nullptr, num_peers * 2 * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        if (peer_id == root) {
            ch->gather({reinterpret_cast<char*>(vals.data() + 2 * peer_id), sizeof(vals[0]) * 2},
                        {reinterpret_cast<char*>(rcv_vals), sizeof(int) * num_peers * 2}, root);
        } else {
            ch->gather({reinterpret_cast<char*>(vals.data() + 2 * peer_id), sizeof(vals[0]) * 2}, {}, root);
        }
        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            for (int i = 0; i < num_peers; i++) {
                BOOST_CHECK_EQUAL(rcv_vals[i], i + 1);
            }
        } else {
            exit(0);
        }
    }
}

BOOST_AUTO_TEST_CASE(gather_multiple) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        constexpr int num_peers = 14;
        std::vector<int> vals(2 * num_peers);
        for (int i = 0; i < vals.size(); i++) {
            vals[i] = i + 1;
        }
        FMI::Utils::peer_num root = 0;

        int* rcv_vals = static_cast<int*>(mmap(nullptr, num_peers * 2 * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        if (peer_id == root) {
            ch->gather({reinterpret_cast<char*>(vals.data() + 2 * peer_id), sizeof(vals[0]) * 2},
                       {reinterpret_cast<char*>(rcv_vals), sizeof(int) * num_peers * 2}, root);
        } else {
            ch->gather({reinterpret_cast<char*>(vals.data() + 2 * peer_id), sizeof(vals[0]) * 2}, {}, root);
        }
        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            for (int i = 0; i < num_peers; i++) {
                BOOST_CHECK_EQUAL(rcv_vals[i], i + 1);
            }
        } else {
            exit(0);
        }
    }
}

BOOST_AUTO_TEST_CASE(scatter_one) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        constexpr int num_peers = 2;
        std::vector<int> root_vals {1,2,3,4};
        FMI::Utils::peer_num root = 0;

        int* rcv_vals = static_cast<int*>(mmap(nullptr, num_peers * 2 * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        if (peer_id == root) {
            ch->scatter({reinterpret_cast<char*>(root_vals.data()), sizeof(root_vals[0]) * root_vals.size()},
                             {reinterpret_cast<char*>(rcv_vals + peer_id * 2), sizeof(int) * 2}, root);
        } else {
            ch->scatter({}, {reinterpret_cast<char*>(rcv_vals + peer_id * 2), sizeof(int) * 2}, root);
        }
        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            for (int i = 0; i < num_peers; i++) {
                BOOST_CHECK_EQUAL(rcv_vals[i], i + 1);
            }
        } else {
            exit(0);
        }
    }
}

BOOST_AUTO_TEST_CASE(scatter_multiple) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        constexpr int num_peers = 14;
        std::vector<int> root_vals(2 * num_peers);
        for (int i = 0; i < root_vals.size(); i++) {
            root_vals[i] = i + 1;
        }
        FMI::Utils::peer_num root = 3;

        int* rcv_vals = static_cast<int*>(mmap(nullptr, num_peers * 2 * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        if (peer_id == root) {
            ch->scatter({reinterpret_cast<char*>(root_vals.data()), sizeof(root_vals[0]) * root_vals.size()},
                        {reinterpret_cast<char*>(rcv_vals + peer_id * 2), sizeof(int) * 2}, root);
        } else {
            ch->scatter({}, {reinterpret_cast<char*>(rcv_vals + peer_id * 2), sizeof(int) * 2}, root);
        }
        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            for (int i = 0; i < num_peers; i++) {
                BOOST_CHECK_EQUAL(rcv_vals[i], i + 1);
            }
        } else {
            exit(0);
        }
    }
}

BOOST_AUTO_TEST_CASE(reduce_multiple) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        FMI::Utils::peer_num root = 5;
        constexpr int num_peers = 13;
        int* res = static_cast<int*>(mmap(nullptr, sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto f = [] (char* a, char* b) {
            int* dest = reinterpret_cast<int*>(a);
            *dest = *((int*) a) * *((int*) b);
        };
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        int val = peer_id + 1;
        if (peer_id == root) {
            ch->reduce({reinterpret_cast<char*>(&val), sizeof(int)}, {reinterpret_cast<char*>(res), sizeof(int)}, root, {f, true, true});
        } else {
            ch->reduce({reinterpret_cast<char*>(&val), sizeof(int)}, {}, root, {f, true, true});
        }

        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            int expected = 1;
            for (int i = 1; i < num_peers; i++) {
                expected *= (i + 1);
            }
            BOOST_CHECK_EQUAL(expected, *res);
        } else {
            exit(0);
        }

    }
}

BOOST_AUTO_TEST_CASE(reduce_multiple_ltr) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        FMI::Utils::peer_num root = 0;
        constexpr int num_peers = 8;
        int* res = static_cast<int*>(mmap(nullptr, sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto f = [] (char* a, char* b) {
            int* dest = reinterpret_cast<int*>(a);
            *dest = *((int*) a) - *((int*) b);
        };
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        int val = peer_id + 1;
        if (peer_id == root) {
            ch->reduce({reinterpret_cast<char*>(&val), sizeof(int)}, {reinterpret_cast<char*>(res), sizeof(int)}, root, {f, false, false});
        } else {
            ch->reduce({reinterpret_cast<char*>(&val), sizeof(int)}, {}, root, {f, false, false});
        }

        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            int expected = 1;
            for (int i = 1; i < num_peers; i++) {
                expected -= (i + 1);
            }
            BOOST_CHECK_EQUAL(expected, *res);
        } else {
            exit(0);
        }

    }
}

BOOST_AUTO_TEST_CASE(allreduce_multiple) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        constexpr int num_peers = 8;
        int* res = static_cast<int*>(mmap(nullptr, num_peers * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto f = [] (char* a, char* b) {
            int* dest = reinterpret_cast<int*>(a);
            *dest = *((int*) a) + *((int*) b);
        };
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        int val = peer_id + 1;
        ch->allreduce({reinterpret_cast<char*>(&val), sizeof(int)}, {reinterpret_cast<char*>(res + peer_id), sizeof(int)}, {f, true, true});

        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            int expected = 1;
            for (int i = 1; i < num_peers; i++) {
                expected += (i + 1);
            }
            for (int i = 0; i < num_peers; i++) {
                BOOST_CHECK_EQUAL(expected, res[i]);
            }
        } else {
            exit(0);
        }

    }
}

BOOST_AUTO_TEST_CASE(allreduce_multiple_ltr) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        FMI::Utils::peer_num root = 0;
        constexpr int num_peers = 8;
        int* res = static_cast<int*>(mmap(nullptr, num_peers * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto f = [] (char* a, char* b) {
            int* dest = reinterpret_cast<int*>(a);
            *dest = *((int*) a) - *((int*) b);
        };
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        int val = peer_id + 1;
        ch->allreduce({reinterpret_cast<char*>(&val), sizeof(int)}, {reinterpret_cast<char*>(res + peer_id), sizeof(int)}, {f, false, false});

        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            int expected = 1;
            for (int i = 1; i < num_peers; i++) {
                expected -= (i + 1);
            }
            for (int i = 0; i < num_peers; i++) {
                BOOST_CHECK_EQUAL(expected, res[i]);
            }
        } else {
            exit(0);
        }

    }
}

BOOST_AUTO_TEST_CASE(scan) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;
        
        constexpr int num_peers = 32;
        int* res = static_cast<int*>(mmap(nullptr, sizeof(int) * num_peers, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto f = [] (char* a, char* b) {
            int* dest = reinterpret_cast<int*>(a);
            *dest = *((int*) a) + *((int*) b);
        };
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        int val = peer_id + 1;
        ch->scan({reinterpret_cast<char*>(&val), sizeof(int)}, {reinterpret_cast<char*>(res + peer_id), sizeof(int)}, {f, true, true});
        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            int prefix_sum = 0;
            for (int i = 0; i < num_peers; i++) {
                prefix_sum += (i + 1);
                BOOST_CHECK_EQUAL(prefix_sum, res[i]);
            }
        } else {
            exit(0);
        }


    }
}

BOOST_AUTO_TEST_CASE(scan_ltr) {
    for (auto const & backend_data : backends) {
        auto channel_name = backend_data.first;
        auto test_params = backend_data.second.first;
        auto model_params = backend_data.second.second;

        constexpr int num_peers = 8;
        int* res = static_cast<int*>(mmap(nullptr, sizeof(int) * num_peers, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        for (int i = 1; i < num_peers; i ++) {
            int pid = fork();
            if (pid == 0) {
                peer_id = i;
                break;
            }
        }
        auto f = [] (char* a, char* b) {
            int* dest = reinterpret_cast<int*>(a);
            *dest = *((int*) a) - *((int*) b);
        };
        auto ch = FMI::Comm::Channel::get_channel(channel_name, test_params, model_params);
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(comm_name);
        int val = peer_id;
        ch->scan({reinterpret_cast<char*>(&val), sizeof(int)}, {reinterpret_cast<char*>(res + peer_id), sizeof(int)}, {f, false, false});
        ch->finalize();
        if (peer_id == 0) {
            int status = 0;
            while (wait(&status) > 0);
            int result = 0;
            for (int i = 0; i < num_peers; i++) {
                result = result - i;
                BOOST_CHECK_EQUAL(result, res[i]);
            }
        } else {
            exit(0);
        }


    }
}

#if FMI_ENABLE_TCPUNCH
namespace {
    // These cases talk to the rendezvous server directly rather than through a Channel, so they
    // skip rather than fail when it is absent -- the rest of the suite already fails loudly in
    // that situation and there is no point in adding more noise.
    bool rendezvous_reachable() {
        int probe = socket(AF_INET, SOCK_STREAM, 0);
        if (probe < 0) {
            return false;
        }
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(10000);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        const bool ok = connect(probe, (struct sockaddr*)&addr, sizeof(addr)) == 0;
        close(probe);
        return ok;
    }

    // pair() reports failure as Timeout, but error_exit() in the TCPunch common header throws a
    // bare std::string, so both have to be caught to tell "did not pair" from a crash.
    bool pair_fails(const std::string& name, int timeout_ms) {
        try {
            int fd = pair(name, "127.0.0.1", 10000, timeout_ms);
            if (fd >= 0) {
                close(fd);
            }
            return false;
        } catch (const Timeout&) {
            return true;
        } catch (const std::string&) {
            return true;
        }
    }
}

// One pairing that never finds a partner must not disturb the pairings running beside it, and
// the name it abandoned must stay usable afterwards. Before the registry was swept, an entry
// was reclaimed only when a second client registered under the same name, so an abandoned
// registration survived for the lifetime of the server and the next pairing under that name was
// handed a peer address belonging to a client that had already gone away.
//
// This also covers pair() itself being callable concurrently: it kept its listener state in two
// file-scope atomics, so two simultaneous calls in one process overwrote each other's result.
BOOST_AUTO_TEST_CASE(direct_concurrent_pairings_one_timeout) {
    if (!rendezvous_reachable()) {
        BOOST_TEST_MESSAGE("no rendezvous server on 127.0.0.1:10000, skipping");
        return;
    }
    const std::string base = "concurrent-" + std::to_string(getpid());
    const std::string abandoned = base + "-abandoned";

    std::atomic<bool> orphan_timed_out{false};
    std::thread orphan([&] { orphan_timed_out = pair_fails(abandoned, 500); });

    std::atomic<int> paired{0};
    std::vector<std::thread> peers;
    for (int k = 0; k < 3; k++) {
        for (int side = 0; side < 2; side++) {
            peers.emplace_back([&, k] {
                if (!pair_fails(base + "-" + std::to_string(k), 20000)) {
                    paired++;
                }
            });
        }
    }
    for (auto& t : peers) {
        t.join();
    }
    orphan.join();

    BOOST_CHECK(orphan_timed_out.load());
    BOOST_CHECK_EQUAL(paired.load(), 6);

    // The abandoned name must be usable again by a real pair of peers.
    std::atomic<int> repaired{0};
    std::vector<std::thread> retry;
    for (int side = 0; side < 2; side++) {
        retry.emplace_back([&] {
            if (!pair_fails(abandoned, 20000)) {
                repaired++;
            }
        });
    }
    for (auto& t : retry) {
        t.join();
    }
    BOOST_CHECK_EQUAL(repaired.load(), 2);
}
#endif // FMI_ENABLE_TCPUNCH

#if FMI_ENABLE_REDIS
// The `backends` map above has Redis and S3 commented out, so every case in this suite runs
// against Direct only — i.e. against PeerToPeer. The whole ClientServer family (Redis, S3) is
// otherwise untested here, which is how an inclusive-scan ordering bug survived in it.
//
// This case pins the ClientServer implementation directly. It uses subtraction (neither
// commutative nor associative, so left_to_right is on) and values starting at 1: the existing
// scan_ltr case gives rank 0 the value 0, which is subtraction's identity and happens to mask
// a wrong fold order. Inclusive scan at rank k must be v0 - v1 - ... - vk, with the rank's own
// value applied LAST; folding it first instead yields vk - v0 - ... - v(k-1).
BOOST_AUTO_TEST_CASE(scan_ltr_client_server_ordering) {
    constexpr int num_peers = 4;
    int* res = static_cast<int*>(mmap(nullptr, sizeof(int) * num_peers, PROT_READ | PROT_WRITE,
                                      MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    BOOST_REQUIRE(res != MAP_FAILED);
    // Shared-memory rendezvous, deliberately not ClientServer::barrier(). upload() records every
    // name it writes in created_objects -- barrier markers included -- and finalize() deletes all
    // of them, so the first rank out of the barrier removes its own marker and the ranks still
    // polling can never reach num_arrived >= num_peers. Using the channel's own barrier here to
    // order scan against finalize() therefore deadlocks about 7 runs in 10.
    auto* arrived = static_cast<std::atomic<int>*>(mmap(nullptr, sizeof(std::atomic<int>),
                                                        PROT_READ | PROT_WRITE,
                                                        MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    BOOST_REQUIRE(arrived != MAP_FAILED);
    new (arrived) std::atomic<int>(0);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    for (int i = 1; i < num_peers; i++) {
        int pid = fork();
        if (pid == 0) {
            peer_id = i;
            break;
        }
    }

    auto f = [] (char* a, char* b) {
        *reinterpret_cast<int*>(a) = *reinterpret_cast<int*>(a) - *reinterpret_cast<int*>(b);
    };

    // redis_test_params caps max_timeout at 1000 ms, which a loaded machine can exceed on a
    // legitimately slow download; this case should fail on a wrong fold order, not on timing.
    std::map<std::string, std::string> params = redis_test_params;
    params["max_timeout"] = "30000";
    auto ch = FMI::Comm::Channel::get_channel("Redis", params, redis_test_model_params);
    ch->set_peer_id(peer_id);
    ch->set_num_peers(num_peers);
    ch->set_comm_name(comm_name + "_cs_scan");
    int val = peer_id + 1;
    ch->scan({reinterpret_cast<char*>(&val), sizeof(int)},
             {reinterpret_cast<char*>(res + peer_id), sizeof(int)}, {f, false, false});
    // No rank may finalize() until every rank has finished scan: finalize() deletes this rank's
    // uploaded objects, and rank 0 completes immediately (it folds only its own value), so
    // without this it deletes the object ranks 1..n-1 still have to download.
    arrived->fetch_add(1, std::memory_order_acq_rel);
    while (arrived->load(std::memory_order_acquire) < num_peers) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ch->finalize();

    if (peer_id == 0) {
        int status = 0;
        while (wait(&status) > 0);
        int expected = 1;                       // v0
        BOOST_CHECK_EQUAL(expected, res[0]);
        for (int i = 1; i < num_peers; i++) {
            expected = expected - (i + 1);      // ... - vi
            BOOST_CHECK_EQUAL(expected, res[i]);
        }
    } else {
        exit(0);
    }
}
#endif

BOOST_AUTO_TEST_SUITE_END();
