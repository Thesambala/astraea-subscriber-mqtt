# Aturan Wajib Proyek ASTRAEA (berlaku untuk SEMUA AI & SEMUA sesi)

MQTT subscriber: ESP32/CAM_YOLO → DynamoDB + S3 + Telegram/Email.

1. COMMIT + PUSH SETIAP PERUBAHAN: setiap file yang diubah/ditambah (kode, config, docs)
   WAJIB di-`git commit` dan `git push` ke branch yang sama sebelum sesi/pekerjaan selesai.
   Dilarang menumpuk perubahan tanpa push.
2. SECRET & ENV: semua repo kini PRIVATE — file `.env` WAJIB ikut di-commit+push
   agar config tidak hilang (pengecualian dari aturan umum, berlaku selama repo private).
   DILARANG menjadikan repo ini public. Jangan paste secret di issue/komentar.
3. FILE PRODUKSI: `subscriber_aws.py` adalah file yang jalan di server via systemd
   `traffic-aws-subscriber` (`/home/ubuntu/traffic-aws-subscriber`, EC2 `astraea-web-mqtt`).
   File `subscriber_aws2..5.py` adalah arsip/percobaan — jangan hapus tanpa konfirmasi.
4. LOKASI DEPLOY: `/home/ubuntu/traffic-aws-subscriber` di EC2 `astraea-web-mqtt`
   (ap-southeast-2). Setelah ubah kode: copy ke server, `systemctl restart traffic-aws-subscriber`,
   verifikasi via `journalctl -u traffic-aws-subscriber` + publish pesan uji.
5. AUTO-PUSH: server ini punya deploy key SSH (`~/.ssh/astraea-sub`, remote `gh-sub`).
   Selesai mengubah file di `~/workspace/astraea-subscriber-mqtt`: commit lalu
   `git push` langsung (tanpa token). Lalu sync ke deploy:
   copy `subscriber_*.py` ke `/home/ubuntu/traffic-aws-subscriber`,
   `systemctl restart traffic-aws-subscriber`, verifikasi via `journalctl`.
