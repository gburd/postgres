#!/bin/bash
mkdir -p /mnt/work/out
exec >/mnt/work/out/BUILD 2>&1
set -x
echo "=== NVMe ==="
if ! mountpoint -q /mnt/nvme; then
  sudo mkfs.xfs -f /dev/nvme1n1 || true; sudo mkdir -p /mnt/nvme
  sudo mount /dev/nvme1n1 /mnt/nvme || true; sudo chown ec2-user /mnt/nvme
fi
mountpoint /mnt/nvme && echo "NVMe mounted"
echo "=== libxtc (debugoptimized: xtc-stranded/xtc-rings/xtc_tail need -g) ==="
cd /mnt/work && { find xtc -mindepth 1 -delete 2>/dev/null; rmdir xtc 2>/dev/null; }; mkdir -p xtc && cd xtc && tar xzf /tmp/libxtc.tar.gz
meson setup build -Dtls=openssl -Dshared=true -Dbuildtype=debugoptimized -Dio-backend=uring && ninja -C build && sudo ninja -C build install || { echo LIBXTC_FAIL; exit 1; }
echo "/usr/local/lib64" | sudo tee /etc/ld.so.conf.d/usrlocal.conf >/dev/null; sudo ldconfig
grep -c XTC_TAIL_LOOP_POLL src/evt/loop.c && echo "libxtc OK (LOOP_POLL present)"
echo "=== PG ==="
cd /mnt/work && { find pg -mindepth 1 -delete 2>/dev/null; rmdir pg 2>/dev/null; }; mkdir -p pg && cd pg && tar xzf /tmp/pgxtc.tar.gz
export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig
meson setup build --buildtype=debugoptimized -Dcassert=false -Dxtc=enabled -Dprefix=/mnt/work/inst/usr/local/pgsql -Dc_args="-fno-omit-frame-pointer -g" && ninja -C build && ninja -C build install || { echo PG_FAIL; exit 1; }
echo "PG ok"
