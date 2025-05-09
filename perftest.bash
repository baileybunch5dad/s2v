for opts in tls notls
do
	for threads in 1 2 3 4 5 6
	do
		outfile=test_${opts}_${threads}_threads
		echo $outfile
		SECONDS=0
		echo ./krm_capp --$opts --nthreads=$threads
		./krm_capp --$opts --nthreads=$threads > $outfile
		duration=$SECONDS
		echo ./krm_capp --$opts --nthreads=$threads took $duration seconds
	done
done
