mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))

build-python311:
	docker run --rm --mount type=bind,source=$(mkfile_dir)/../../../,target=/opt/fmi/ fmi-build-python311:latest bash -lc "rm -rf /opt/fmi/python/build/* && mkdir -p /opt/fmi/python/build/runtime-lib/ && cd /opt/fmi/python/build/ && cmake .. && make && for lib in \$$(ldd /opt/fmi/python/build/fmi.so /usr/lib64/libcurl.so.4 | awk '/=> \// {print \$$3} /^\// {print \$$1}' | sort -u); do case \"\$$lib\" in /lib64/ld-linux-*|/lib64/libc.so.*|/lib64/libm.so.*|/lib64/libdl.so.*|/lib64/librt.so.*|/lib64/libpthread.so.*|/lib64/libutil.so.*|/usr/lib64/libgcc_s.so.*|/usr/lib64/libstdc++.so.*) ;; *) cp -L \"\$$lib\" /opt/fmi/python/build/runtime-lib/ ;; esac; done"
	mkdir -p $(ARTIFACTS_DIR)/python/libfmi/extern/TCPunch/client/
	mkdir -p $(ARTIFACTS_DIR)/lib/
	cp $(mkfile_dir)/../../build/libfmi/extern/TCPunch/client/libtcpunch.a "$(ARTIFACTS_DIR)/python/libfmi/extern/TCPunch/client/"
	cp $(mkfile_dir)/../../build/fmi.so "$(ARTIFACTS_DIR)/python/"
	cp $(mkfile_dir)/../../build/libfmi/libFMI.a "$(ARTIFACTS_DIR)/python/libfmi/"
	cp $(mkfile_dir)/../../build/runtime-lib/* "$(ARTIFACTS_DIR)/lib/"
