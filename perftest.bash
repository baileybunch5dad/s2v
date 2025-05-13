echo "Starting performance test varying transport layer security and concurrency levels"
for opts in tls notls
do
	for threads in 6 5 4 3 2 1
	do
		outfile=test_${opts}_${threads}_threads
		# echo $outfile
		SECONDS=0
		echo Starting at $(date) execution of ./krm_capp --$opts --nthreads=$threads
		./krm_capp --$opts --nthreads=$threads > $outfile
		duration=$SECONDS
		echo Completed ./krm_capp --$opts --nthreads=$threads in $duration seconds
	done
done
