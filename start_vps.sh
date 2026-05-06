#!/bin/bash
# Matikan paksa jika ada bot yang masih jalan (baik V1 maupun V2)
pkill -9 -f mexc_xaut_bot.py
pkill -9 -f mexc_xaut_botV2.py
sleep 2

# Hapus log lama agar bersih
rm -f xaut_bot.log

# Jalankan bot V2 dengan flag --live untuk mode LIVE TRADING
nohup ./venv/bin/python mexc_xaut_botV2.py --dashboard --live > xaut_bot.log 2>&1 &

echo "Bot XAUT Pro V2 started in background (LIVE MODE)"
ps -ef | grep mexc_xaut_botV2.py
