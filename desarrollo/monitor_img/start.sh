#!/bin/bash

# Arrancar display virtual a resolución completa
Xvfb :0 -screen 0 1920x1080x24 &
export DISPLAY=:0

# Esperar a que Xvfb esté listo
sleep 2

# Arrancar x11vnc
x11vnc -display :0 -rfbport 5900 -passwd tfm -forever -shared -cursor most &

# Arrancar la aplicación FastDDS Monitor
source /root/entrypoint.bash
fastdds_monitor