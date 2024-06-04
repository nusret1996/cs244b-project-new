#!/usr/bin/bash
DEBIAN_FRONTEND=noninteractive sudo apt-get -yq update
DEBIAN_FRONTEND=noninteractive sudo apt-get -yq upgrade
DEBIAN_FRONTEND=noninteractive sudo apt-get -yq install build-essential
DEBIAN_FRONTEND=noninteractive sudo apt-get -yq install libgrpc++-dev
DEBIAN_FRONTEND=noninteractive sudo apt-get -yq install libprotobuf-dev
DEBIAN_FRONTEND=noninteractive sudo apt-get -yq install protobuf-compiler-grpc
