CC = g++
VERSION = -std=c++11
FLAGS = -O2 -Wall
INC=-I../Include
SRC=../Source

all: SPMCTest MPSCTestConsumer MPSCTestProducer

SPMCTest: SPMCTest.cc
	$(CC) $(VERSION) $(FLAGS) $(INC) SPMCTest.cc $(SRC)/SPMC.cc $(SRC)/Profiler.cc -o SPMCTest -l pthread

MPSCTestConsumer: MPSCTestConsumer.cc
	$(CC) $(VERSION) $(FLAGS) $(INC) MPSCTestConsumer.cc $(SRC)/MPSC.cc $(SRC)/Profiler.cc -o MPSCTestConsumer -l pthread

MPSCTestProducer: MPSCTestProducer.cc
	$(CC) $(VERSION) $(FLAGS) $(INC) MPSCTestProducer.cc $(SRC)/MPSC.cc $(SRC)/Profiler.cc -o MPSCTestProducer -l pthread

clean:
	rm SPMCTest MPSCTestConsumer MPSCTestProducer
