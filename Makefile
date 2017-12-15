sqlitestore: *.lisp *.asd zstd/*.lisp
	sbcl --disable-debugger --load build.lisp

#sqlitestore: *.lisp *.asd zstd/*.lisp
#	ccl -b -l build.lisp

#sqlitestore: *.lisp *.asd zstd/*.lisp
#	clisp -i build.lisp

random.data:
	head -c $$((10*1024*1024)) /dev/urandom > random.data

double.data: random.data
	cat random.data random.data > double.data

bench: sqlitestore random.data double.data
	rm -f bench.sqs
	date >> benchmark.out
	./sqlitestore create bench.sqs
	{ time ./sqlitestore add bench.sqs random.data <random.data; } 2>> benchmark.out
	{ time ./sqlitestore add bench.sqs double.data <double.data; } 2>> benchmark.out
	sh -c '{ time ./sqlitestore restore bench.sqs random.data ; } 2>> benchmark.out 1> tmp.data'
	diff random.data tmp.data 2>> benchmark.out
	sh -c '{ time ./sqlitestore restore bench.sqs double.data ; } 2>> benchmark.out 1>tmp.data'
	diff double.data tmp.data 2>> benchmark.out
	rm tmp.data
