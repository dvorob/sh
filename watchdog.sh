#!/bin/bash
[ $# -eq 0 ] && { echo "Missing arguments ("`basename $0 .sh` -h" for help)"; exit 1; }

cfg_file="/u01/minicem/etc/mon.cfg"
#source "/u01/minicem/etc/functions.sh"

write_log(){
	pid=$1
	process_name=$2
	message=$3
	logpath=$4

	if [ $5 ]
		then
		stream_type=$5
		echo -e [$(date +%H:%M:%S" "%d.%m.%Y)] [$pid]'\t'$process_name'\t'$stream_type: $message
		echo -e [$(date +%H:%M:%S" "%d.%m.%Y)] [$pid]'\t'$process_name'\t'$stream_type: $message >> $logpath
	else
		echo -e [$(date +%H:%M:%S" "%d.%m.%Y)] [$pid]'\t'$process_name: $message
		echo -e [$(date +%H:%M:%S" "%d.%m.%Y)] [$pid]'\t'$process_name: $message >> $logpath
	fi
}

show_help_move_files(){
	echo "Usage: move_files.sh -c /u01/minicem/etc/streams.cfg -t voice -s raw"
  echo "  -c - path to cfg file, overrides default ($cfg_file)"
  echo "  --config-file=PATH - the same as -c"
  echo "  -t - type of stream, values: voice, voice_cnt, data, data_cnt, data_newcnt, sms1v, sms2v, sms3v, yota_claims"
  echo "  --type=TYPE - the same as -t"
  echo "  -s - step. values: raw, parsed"
  echo "  --step=STEP - the same as -s"
  echo ""
  echo "-s and -t are mandatory parameters"
}

check_stream_type(){
	stream_type=$1
	script_name=$2
	if [[ ! ($1 =~ "voice|voice_cnt|data|data_cnt|data_newcnt|sms1v|sms2v|sms3v|vlr|yota_claims") ]]
		then
    echo "invalid stream: $stream_type (${script_name}.sh -h for help)" 1>&2
    exit 3
  fi
}

check_step(){
	step=$1
	script_name=$2
	if [[ ! ($step =~ "raw|parsed") ]]
		then
    echo "invalid step: $step ("`basename $script_name .sh` -h" for help)" 1>&2
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

move_to_remote_host(){
	filepath=$1
	remote_path=$2
	remote_host=$3

	check_file $filepath

	infile=`basename $filepath`

sftp $remote_host <<EOF
cd ${remote_path}
put ${filepath}
quit
EOF

	if [ $? -ne 0 ]
	then
	  echo "Couldn't move $infile to ${remote_path} on $remote_host"
	  return 1
	fi
	ssh $remote_host "mv ${remote_path}/$infile ${remote_path}/buf/${infile}"
	if [ $? -ne 0 ]
	then
	  echo "Couldn't move $infile to ${remote_path}"buf" on $remote_host"
	  return 2
	fi
	echo "$infile was successfuly moved to ${remote_path}"buf" on $remote_host"
	return 0
}

show_help_streams(){
	echo "Usage: streams.cfg -t voice -s raw"
	echo "  -t - type of stream, values: voice, voice_cnt, data, data_cnt, data_newcnt, sms1v, sms2v, sms3v"
	echo "  -s - step, values: raw, parsed"
}

cat_parsed_files(){
	stream_type=$1
	fname=$2
	to_dir=$3
	infile=$4
	if [[ $stream_type =~ "sms" ]]
	  then
	  sed 's/.*/'"${fname}"',&/' ${to_dir}/${fname} >> ${to_dir}/${infile}
	  res=$?
	else
	  cat ${to_dir}/${fname} >> ${to_dir}/${infile}
	  res=$?
	fi
	return $res
}

move_check(){
	process=$1
	workpath=$2
	serv_log=$3
	stream_type=$4
	fj_log=$5

	if [[ ! -f $serv_log ]]
	then
		write_log $$ $this "No server log $serv_log" $monitor_log $stream_type
		rval=$(echo $rval+1|bc)
	elif [[ ! -f $fj_log ]]
		then
		write_log $$ $this "No server log $serv_log" $monitor_log $stream_type
		rval=$(echo $rval+1|bc)
	fi
	last_move_log_serv=$(tac $serv_log | grep -m1 "$process.* were piled to")
	last_move_log_fj=$(tac $fj_log | grep -m1 $process)

	last_move_date_serv=$(echo $last_move_log_serv | sed -n 's/\[\(.*\)\].*'$process'.*'$stream_type'.*/\1/p')
	last_move_date_fj=$(echo $last_move_log_fj | sed -n 's/\[\(.*\)\].*'$process'.*'$stream_type'.*/\1/p')
	
	last_move_date_serv=$(echo $last_move_date_serv | sed -n 's/\([0-9]\{2\}\):\([0-9]\{2\}\):[0-9]\{2\} *\([0-9]\{2\}\)\.\([0-9]\{2\}\).\([0-9]\{4\}\).*/\5\4\3 \1\2/p')
	last_move_date_fj=$(echo $last_move_date_fj | sed -n 's/\([0-9]\{2\}\):\([0-9]\{2\}\):[0-9]\{2\} *\([0-9]\{2\}\)\.\([0-9]\{2\}\).\([0-9]\{4\}\).*/\5\4\3 \1\2/p')

	if [[ $(date -d "$last_move_date_serv" +%Y%m%d%H%M) -gt $(date -d "$last_move_date_fj" +%Y%m%d%H%M) ]]
		then
		last_move_date=$last_move_date_serv
		last_move_data=`echo $last_move_log_serv | sed -n 's/.*'$process'.*'$stream_type':.*were piled to \([^ ]*\).*/\1/p'`
	else
		last_move_date=$last_move_date_fj
		last_move_data=`echo $last_move_log_fj | sed -n 's/.*'$process'.*'$stream_type':.*\(in[^ ]*\).*/\1/p'`
	fi
	[[ -f ${workpath}/${last_move_data}.bz2 ]] && full_path_to_data=${workpath}/${last_move_data}.bz2 || full_path_to_data=${workpath}/${last_move_data}
	if [[ -f $full_path_to_data ]]
		then
		if [[ (`wc -l $full_path_to_data | cut -f1 -d " "` > 0) ]]
			then
			if [[ `date -d "$(echo $last_move_date | sed -n 's/\([0-9]\{2\}\):\([0-9]\{2\}\):[0-9]\{2\} *\([0-9]\{2\}\)\.\([0-9]\{2\}\).\([0-9]\{4\}\).*/\5\4\3 \1\2/p') +$delta min" +%Y%m%d%H%M` -gt $(date +%Y%m%d%H%M) ]]
			then
				write_log $$ $this "Ok [$last_move_data $last_move_date]" $monitor_log $stream_type
			else
				write_log $$ $this "Ok but late [$last_move_data $last_move_date]" $monitor_log $stream_type
				rval=$(echo $rval+1|bc)
			fi

		else
			write_log $$ $this "Moved empty file! [$last_move_data $last_move_date]" $monitor_log $stream_type
			rval=$(echo $rval+1|bc)
		fi
	else
		write_log $$ $this "No file $full_path_to_data" $monitor_log $stream_type
	fi
}


check_process(){
	#movers call function like this:
	
	local process=$1  #move_files
	local workpath=$2 #/u01/minicem
	local logpath=$3  #/u01/minicem/log/monitor.log
	local pid=$4      #/u01/minicem/log/move_files_raw.pid
	#echo $process $workpath $logpath $pid
	
	if [ $5 ]; then
		local stream_type=$5 #voice
		local cfg=$6  #/u01/minicem/etc/streams.cfg
		local step=$7 #raw
	fi

	by_pid=$(ps -p $(cat $pid) -o command= | wc -l)
	
  if [[ $by_pid -eq 1  ]]

	then
		[ $stream_type ] && write_log $$ $this "$step: $process is ok!" $logpath $stream_type || write_log $$ $this "$process is ok" $logpath
		return 0
	fi
	if [[ $process =~ "parser" ]]; then
		by_name=$(ps -ef | grep Djava.*$process | grep -v grep | wc -l)
		if [[ $by_name -eq 1 ]]; then 
			echo $(pgrep -f "Djava.*$process") > $pid
			[ $stream_type ] && write_log $$ $this "$step: $process is ok2. pid changed to $(cat $pid)" $logpath $stream_type || write_log $$ $this "$process is ok3. pid changed to $(cat $pid)" $logpath
			return 0
		elif [[ $by_name -ge 1 ]]; then
			echo $(pgrep -f "Djava.*$process") | xargs kill
			[ $? -eq 0 ] && write_log $$ $this "$process is Not ok. process killed" $logpath $stream_type && { write_log $$ $this "$process is Not ok. process couldn't be killed"; return 1; }
		fi
	else
		by_name1=$(pgrep -f "$process.*$step.*$stream_type"| wc -l)
		by_name2=$(pgrep -f "$process.*$stream_type -s $step.*$"| wc -l)
		if [[ $(echo $by_name1 + $by_name2 | bc) -eq 1 ]]; then
			[ $by_name1 -eq 1 ] && echo $(pgrep -f "$process.*$step.*$stream_type") > $pid || echo $(pgrep -f "$process.*$stream_type -s $step.*$") > $pid
			write_log $$ $this "$step: $process is ok. pid changed to $(cat $pid)" $logpath $stream_type
			return 0
		elif [[ $(echo $by_name1 + $by_name2 | bc) -gt 1 ]]; then
			[ by_name1 -eq 1 ] && { echo $(pgrep -f "$process.*$step.*$stream_type") | xargs kill; } || { echo $(pgrep -f "$process.*$stream_type -s $step.*$") | xargs kill; }
			[ $? -eq 0 ] && write_log $$ $this "$step: $process is Not ok. process killed" $logpath $stream_type && { write_log $$ $this "$step: $process is Not ok. process couldn't be killed"; return 1; }
		fi
	fi	
	echo $pid
	if [ $process = "msk_parser_cdr" ]; then
		java -Djava.library.path=$lib -jar $workpath/for_scripts/$process.jar $path_to_src_cdr $path_to_par_cdr $path_to_msc >> $workpath/log/$process.log 2>&1 & echo $! > $pid
	elif [ $process = "msk_parser_scdr" ]; then
		java -Djava.library.path=$lib -jar $workpath/for_scripts/$process.jar $path_to_src_scdr $path_to_par_scdr $path_to_done $path_to_log $path_to_grammar >> /u01/datacem/log/$process.log 2>&1 & echo $! > $pid
	elif [ $process = "cnt_parser_cdr" ]; then
		java -Djava.library.path=/u01/minicem/etc/ -jar /u01/minicem/for_scripts/cnt_parser_cdr.jar /u01/minicem/for_cdr_src/src_cnt/ /u01/minicem/for_cdr_src/par/cnt/ /u01/smscem/sms_load/msc/cnt >> $workpath/log/cnt/cnt_parser_cdr.log 2>&1 & echo $! > $pid
	elif [ $process = "cnt_parser_scdr" ]; then
		java -Djava.library.path=/u01/minicem/etc/ -jar /u01/minicem/for_scripts/cnt_parser_scdr.jar /u01/datacem/for_cdr_src/src_cnt/ /u01/datacem/for_cdr_src/par/cnt/ /u01/datacem/for_cdr_src/done/cnt/ /u01/datacem/for_cdr_src/log/ /u01/minicem/etc/grammar_r8_v2 >> /u01/datacem/log/cnt/cnt_parser_scdr.log 2>&1 & echo $! > $pid
	elif [ $process = "cntnew_parser_scdr" ]; then
		java -Djava.library.path=/u01/minicem/etc/ -jar /u01/minicem/for_scripts/cntnew_parser_scdr.jar /u01/datacem/for_cdr_src/src_cnt_new/ /u01/datacem/for_cdr_src/par/cnt/ /u01/datacem/for_cdr_src/done/cnt/ /u01/datacem/for_cdr_src/log/ /u01/minicem/etc/grammar >> /u01/datacem/log/cnt/cntnew_parser_scdr.log 2>&1 & echo $! > $pid
	else
		/bin/su - oracle -c "$workpath/for_scripts/$process.sh -c $cfg -t $stream_type -s $step > /dev/null 2>>$workpath/log/$process.error & echo $! > $pid"		
	fi
	if [ $? -eq 0 ]
	then
		[ $stream_type ] && write_log $$ $this "$step: $process successfully restarted" $logpath $stream_type || write_log $$ $this "$process successfully restarted" $logpath
		return 0
	else
		[ $stream_type ] && write_log $$ $this "$step: $process cannot be restarted" $logpath $stream_type || write_log $$ $this "$process cannot be restarted" $logpath
		return 1
	fi
}

clean_log(){
	log=$1
	log_date=$(date +%Y%m%d -d "$2 weeks ago")
	lines_before=$(wc -l $log | cut -f1 -d " ")

	write_log $$ $this "Clean $log" $monitor_log
	startline=$(grep -nm1 "$log_date" $log | sed -n 's/^\([0-9]\+\).*$/\1/p')
	if [ $startline ]
	then
		sed -i -n "$startline,\$p" $log
		lines_after=$(wc -l $log | cut -f1 -d " ")
		deleted_lines=$(echo $lines_before - $lines_after | bc)
		if [[ $deleted_files -le 0 ]]
			then
			write_log $$ $this "Done. deleted 0 lines" $monitor_log
		else
			write_log $$ $this "Done. deleted $deleted_lines lines" $monitor_log
		fi
		
	else
		write_log $$ $this "$log_date not found" $monitor_log
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
		if [[ `date -d "$fmod +$days day" +%Y%m%d%H%M` -lt `date +%Y%m%d%H%M` ]]
		then
			rm -f $file
		fi
	done
	files_after=`find $path -regextype posix-awk -regex $regexp -type f -print | sort -d | wc -l`
	deleted_files=$(echo $files_before - $files_after | bc)
	if [[ $deleted_files -le 0 ]]
		then
		write_log $$ $this "no files earlier, than $(date -d "$fmod +$days day" +%d.%m.%Y' '%H:%M)" $monitor_log
	else
		write_log $$ $this "Done. deleted $deleted_files files" $monitor_log
	fi
}

while [ true ]; do 
	if [ "${1#--config-file=}" != "$1" ]; then 
		cfg_file="${1#--config-file=}" 
	elif [ "$1" = "-c" ]; then 
		shift
		cfg_file="$1" 
	elif [ -z "$1" ]; then 
		break # Ключи кончились 
	else
  	[ $1 != "-h" ] && echo "invalid argument: $1"
      echo "Usage: $0 -c $cfg_file"
      echo "  -c - path to cfg file, overrides default ($cfg_file)"
      echo "  --config-file=PATH - the same as -c"
      exit 2
 	fi 
 	shift 
done 
this=`basename $0 .sh`
check_file $cfg_file
[ $? -eq 0 ] && source $cfg_file -s $this || exit 2
num_processes=${#aim_process[@]}
for (( i=0; i<${num_processes}; i++ ))
do
	echo ${pid[$i]}
	if [[ ${streams[$i]} ]]; then
		check_process ${aim_process[$i]} $workpath $monitor_log ${pid[$i]} ${streams[$i]} $cfg ${step[$i]}
	else
		check_process ${aim_process[$i]} $workpath $monitor_log ${pid[$i]}
	fi
done
