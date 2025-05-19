pyprocesses=$(ps -u chris f | grep 3.11 | awk '{print $1}')


# for pid in "${pyprocesses[@]}"; 
for pid in ${pyprocesses}
do
  echo py-spy top --pid $pid --subprocesses
  py-spy top --pid $pid --subprocesses
  break
done


