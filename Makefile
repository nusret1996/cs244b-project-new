.PHONY: protos

override CXXFLAGS += -std=c++14

protos: streamlet.pb.o streamlet.grpc.pb.o

streamlet.pb.cc: streamlet.proto
	protoc --cpp_out=. streamlet.proto

streamlet.grpc.pb.cc: streamlet.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc=$(shell which grpc_cpp_plugin) streamlet.proto

streamlet.pb.o: streamlet.pb.cc
	$(CXX) $(CXXFLAGS) -c streamlet.pb.cc

streamlet.grpc.pb.o: streamlet.grpc.pb.cc
	$(CXX) $(CXXFLAGS) -c streamlet.grpc.pb.cc