#!/usr/bin/env bash
set -euo pipefail

RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$RUNBOOK_DIR/../.." && pwd)"
BUILD_DIR="$REPO_ROOT/python/build-localstack-redis-gcc10"
OUT_DIR="$RUNBOOK_DIR/build/bundle"

export CC=/usr/bin/gcc10-gcc
export CXX=/usr/bin/gcc10-g++

rm -rf "$BUILD_DIR" "$OUT_DIR"
mkdir -p "$BUILD_DIR" "$OUT_DIR/lib"

cmake -S "$REPO_ROOT/python" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=/usr/bin/gcc10-gcc \
  -DCMAKE_CXX_COMPILER=/usr/bin/gcc10-g++ \
  -DPython3_EXECUTABLE=/var/lang/bin/python3.11 \
  -DFMI_ENABLE_S3=OFF \
  -DFMI_ENABLE_REDIS=ON \
  -DFMI_USE_STATIC_BOOST=ON \
  -DBOOST_ROOT=/usr/local \
  -DBoost_USE_STATIC_LIBS=ON \
  -DFMI_BOOST_PYTHON_LIBRARY=/usr/local/lib/libboost_python311.a

cmake --build "$BUILD_DIR" -j"$(nproc)"

cp "$BUILD_DIR/fmi.so" "$OUT_DIR/"

for lib in $(ldd "$BUILD_DIR/fmi.so" | awk '/=> \// {print $3} /^\// {print $1}' | sort -u); do
  case "$lib" in
    /lib64/ld-linux-*|/lib64/libc.so.*|/lib64/libm.so.*|/lib64/libdl.so.*|/lib64/librt.so.*|/lib64/libpthread.so.*|/lib64/libutil.so.*|/usr/lib64/libgcc_s.so.*|/usr/lib64/libstdc++.so.*)
      ;;
    *)
      cp -L "$lib" "$OUT_DIR/lib/"
      ;;
  esac
done

echo "Built $OUT_DIR/fmi.so and runtime libs:"
find "$OUT_DIR" -maxdepth 2 -type f -printf '  %P\n' | sort
