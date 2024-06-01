GRPC_CFLAGS := $(shell pkg-config --cflags grpc++_unsecure)
GRPC_LIBS := $(shell pkg-config --libs grpc++_unsecure)
PROTOBUF_CFLAGS := $(shell pkg-config --cflags protobuf)
PROTOBUF_LIBS := $(shell pkg-config --libs protobuf)
OPENSSL_CFLAGS := $(shell pkg-config --cflags libcrypto)
OPENSSL_LIBS := $(shell pkg-config --libs libcrypto)

override CXXFLAGS += -std=c++14 $(GRPC_CFLAGS) $(PROTOBUF_CFLAGS)

.PHONY: protos

gst: streamlet.pb.o streamlet.grpc.pb.o StreamletNodeGST.o NetworkInterposer.o CryptoManager.o utils.o KeyValueStateMachine.o ThroughputLossStateMachine.o
	$(CXX) -o StreamletNodeGST $(CXXFLAGS) $(GRPC_LIBS) $(PROTOBUF_LIBS) $^

strict: streamlet.pb.o streamlet.grpc.pb.o StreamletNodeStrict.o NetworkInterposer.o CryptoManager.o utils.o KeyValueStateMachine.o ThroughputLossStateMachine.o
	$(CXX) -o StreamletNodeStrict $(CXXFLAGS) $(GRPC_LIBS) $(PROTOBUF_LIBS) $^

notarization_test: streamlet.pb.o notarization_test.o
	$(CXX) -o notarization_test $(CXXFLAGS) $(PROTOBUF_LIBS) $^

hash_sign_test: utils.o hash_sign_test.o
	$(CXX) -o hash_sign_test $(CXXFLAGS) $(OPENSSL_CLFAGS) $(PROTOBUF_LIBS) $(GRPC_LIBS) $(OPENSSL_LIBS) $^

protos: streamlet.pb.o streamlet.grpc.pb.o

streamlet.pb.cc: streamlet.proto
	protoc --cpp_out=. streamlet.proto

streamlet.grpc.pb.cc: streamlet.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc="$(shell which grpc_cpp_plugin)" streamlet.proto

streamlet.pb.o: streamlet.pb.cc
	$(CXX) $(CXXFLAGS) -c streamlet.pb.cc

streamlet.grpc.pb.o: streamlet.grpc.pb.cc
	$(CXX) $(CXXFLAGS) -c streamlet.grpc.pb.cc