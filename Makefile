.PHONY: v1 protos

GRPC_CFLAGS := $(shell pkg-config --cflags grpc++_unsecure)
GRPC_LIBS := $(shell pkg-config --libs grpc++_unsecure)
PROTOBUF_CFLAGS := $(shell pkg-config --cflags protobuf)
PROTOBUF_LIBS := $(shell pkg-config --libs protobuf)

override CXXFLAGS += -std=c++14 $(GRPC_CFLAGS) $(PROTOBUF_CFLAGS)

v1: StreamletNodeV1.o NetworkInterposer.o CryptoManager.o utils.o streamlet.pb.o streamlet.grpc.pb.o KeyValueStateMachine.o
	$(CXX) -o StreamletNodeV1 $(CXXFLAGS) $(GRPC_LIBS) $(PROTOBUF_LIBS) $^

notarization_test: notarization_test.o streamlet.pb.o
	$(CXX) -o notarization_test $(CXXFLAGS) $(PROTOBUF_LIBS) $^

protos: streamlet.pb.o streamlet.grpc.pb.o

streamlet.pb.cc: streamlet.proto
	protoc --cpp_out=. streamlet.proto

streamlet.grpc.pb.cc: streamlet.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc=$(shell which grpc_cpp_plugin) streamlet.proto

streamlet.pb.o: streamlet.pb.cc
	$(CXX) $(CXXFLAGS) -c streamlet.pb.cc

streamlet.grpc.pb.o: streamlet.grpc.pb.cc
	$(CXX) $(CXXFLAGS) -c streamlet.grpc.pb.cc