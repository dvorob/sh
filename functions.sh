#!/bin/bash
#logger. write_log pid process_name message log stream_type
write_log(){
	pid=$1
	process_name=$2
	message=$3
	logpath=$4

	if [ $5 ]
		then
		stream_type=$5
		echo -e [$(date +%H:%M:%S" "%d.%m.%Y)] [$pid]'\t'$process_name'\t'$stream_type:'\t'$message
		echo -e [$(date +%H:%M:%S" "%d.%m.%Y)] [$pid]'\t'$process_name'\t'$stream_type:'\t'$message >> $logpath
	else
		echo -e [$(date +%H:%M:%S" "%d.%m.%Y)] [$pid]'\t'$process_name: $message
		echo -e [$(date +%H:%M:%S" "%d.%m.%Y)] [$pid]'\t'$process_name: $message >> $logpath
	fi
}

show_help_db_load(){
	echo "Usage: db_load.sh -c /u01/cem/etc/streams.cfg -t voice_msk"
  echo "  -c - path to cfg file, overrides default ($cfg_file)"
  echo "  --config-file=PATH - the same as -c"
  echo "  -t - type of stream, values: voice_msk, data_msk, sms1v, sms2v, sms3v, voice_cnt, data_cnt,yota_claims"
  echo "  --type=TYPE - the same as -t"
  echo ""
  echo "-t is mandatory parameter"
}

check_stream_type(){
	stream_type=$1
	script_name=$2
	if [[ ! ($stream_type =~ voice_msk|data_msk|sms|vlr|voice_cnt|data_cnt|yota_claims) ]]
	then
    echo "invalid stream: $stream_type (${script_name} -h for help)" 1>&2
    exit 3
  fi
}

check_file(){
	file=$1
	if [[ ! (-f $file) ]]
		then
	  echo "file doesn't exist: $file" 1>&2
	  return 5
	else
		return 0
	fi
}

check_process(){
  local process=$1
  local workpath=$2
  local logpath=$3
  local pid=$4
  local stream_type=$5
  local cfg=$6

  check_file $pid
  [ $? -ne 0 ] && exit 2

  by_pid=$(ps -p $(cat $pid) -o command= | wc -l)
  if [[ $by_pid -eq 1  ]]
  then
    write_log $$ $this "$process is ok" $logpath $stream_type
    return 0
  fi
  by_name=$(pgrep -f "$process.*$stream_type"| wc -l)
  if [[ $by_name -eq 1 ]]; then
    echo $(pgrep -f "$process.*$stream_type") > $pid 
    write_log $$ $this "$process is ok. pid changed to $(cat $pid)" $logpath $stream_type
    return 0
  elif [[ $by_name -gt 1 ]]; then
    echo $(pgrep -f "$process.*$stream_type") | xargs kill
    write_log $$ $this "$process is gt than 1. processes killed" $logpath $stream_type
  fi

  $workpath/bin/$process.sh -c $cfg -t $stream_type > /dev/null 2>>$workpath/log/error/$process.error & echo $! > $pid   

  if [ $? -eq 0 ]; then
    write_log $$ $this "$process successfully restarted" $logpath $stream_type
    return 0
  else
    write_log $$ $this "$process cannot be restarted" $logpath $stream_type
    return 1
  fi
}

clean_files(){
  path=$1
  regexp=$2
  days=$3

  write_log $$ $this "Clean ${path}${regexp}" $monitor_log
  files_before=`find $path -regextype posix-awk -regex $regexp -type f -print | sort -d | wc -l`
  for file in `find $path -regextype posix-awk -regex $regexp -type f -print | sort -d`
  do
    fmod=`stat -c %y $file | sed -n 's/\([0-9]\{4\}\)-\([0-9]\{2\}\)-\([0-9]\{2\}\) \([0-9]\{2\}\):\([0-9]\{2\}\):.*/\1\2\3 \4\5/p'`
    if [[ `date -d "$fmod +$days day" +%Y%m%d%H%M` -lt `date +%Y%m%d%H%M` ]]; then
      rm -f $file
    fi
  done
  files_after=`find $path -regextype posix-awk -regex $regexp -type f -print | sort -d | wc -l`
  deleted_files=$(echo $files_before - $files_after | bc)
  if [[ $deleted_files -le 0 ]]; then
    write_log $$ $this "no files earlier, than $(date -d "$fmod +$days day" +%d.%m.%Y' '%H:%M)" $monitor_log
  else
    write_log $$ $this "Done. deleted $deleted_files files" $monitor_log
  fi
}
