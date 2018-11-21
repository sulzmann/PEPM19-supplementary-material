#!/bin/bash
# Install STM with "cabal install stm" first.

ghc -O2 -threaded --make FutureSystematic

if [ "$?" -ne 0 ]; then
	echo "Compilation error, goodbye!"
	exit 1
fi

for test in $(seq 1 4); do
	FILE_NAME="test$test.txt"
	rm "$FILE_NAME"
	for cores in 1 2 4 8; do
		echo "Cores: $cores:" >> "$FILE_NAME"
		./FutureSystematic "$test" +RTS -N"$cores" >> "$FILE_NAME"
	done
done