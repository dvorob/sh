#!/bin/bash
#default value
[ $# -eq 0 ] && { echo "Missing arguments ("`basename $0 .sh` -h" for help)"; exit 1; }
cfg_file="/u01/minicem/etc/streams.cfg"

source "/u01/minicem/etc/functions.sh"

while [ true ]
do
	if [ "${1#--type=}" != "$1" ]
		then
		stream_type="${1#--type=}"
	elif [ "$1" = "-t" ]
		then
		shift
		stream_type="$1"
	elif [ "${1#--step=}" != "$1" ]
		then
		step="${1#--step=}"
	elif [ "$1" = "-s" ]
		then
		shift
		step="$1"
	elif [ "${1#--config-file=}" != "$1" ]
		then
		cfg_file="${1#--config-file=}"
	elif [ "$1" = "-c" ]
		then
		shift
		cfg_file="$1"
	elif [ -z "$1" ]
		then
		break # Ключи кончились
	else
  	[ $1 != "-h" ] && echo "invalid argument: $1" 1>&2
    show_help_move_files
    exit 2
 	fi
 	shift
done
#check
check_stream_type $stream_type $0
check_step $step `basename $script_name .sh`
check_file $cfg_file
[ $? -eq 0 ] && source $cfg_file -t $stream_type -s $step || exit 5

#init values
load_index=0 # index of beginning/stopping loading of files
count_files=0
temp_dir=""
still_timer=0
totalsize=0

write_log $$ $step "started" $serv_log $stream_type
trap "write_log $$ $step 'stopped' $serv_log $stream_type; exit 0" SIGTERM
#logic starts here
if [ "$(ls -A $from_dir | grep -P $file_template)" ] # if files exist - move them
	then
	chsum1="1"
	chsum2="2"
else
	chsum1=$(ls -cl $from_dir | grep -P $file_template | md5sum | cut -f1 -d " ")
	chsum2=$chsum1
fi

while [ true ]
	do
	if [ $chsum1 = $chsum2 ]
		then
		chsum2=$(ls -cl $from_dir | grep -P $file_template | md5sum | cut -f1 -d " ")
		still_timer=$(echo $still_timer+1|bc)
		if [ $load_index -eq 1 ]
			then
			write_log $$ $step "$count_files files were processed" $serv_log $stream_type
			write_log $$ $step "runtime: $(($(date +%s)-$start_time)) sec" $serv_log $stream_type
			[ $step = "parsed" ] && echo >> $serv_log
			load_index=0
			count_files=0
			totalsize=0
			source $cfg_file -t $stream_type -s $step
		fi
		if [ $still_timer -ge $still_limit ]
			then
			write_log $$ $step "no files in $from_dir during $still_timer secs" $serv_log $stream_type
			still_timer=0
		fi
		#no changes
	else
		still_timer=0
		while [ $chsum1 != $chsum2 ]
			do
		    chsum1=$chsum2
		    sleep $delta
				[ $(ls $from_dir | grep -P $file_template | wc -l) > $max_files ] && break
		    chsum2=$(ls -cl $from_dir | grep -P $file_template | md5sum | cut -f1 -d " ")
		done
		start_time=$(date +%s)
		if [ $step = "parsed" ]
			then
			infile=${prefix}$(date +%Y%m%d%H%M%S)
			while [ -f ${datapath}/$infile ]
				do
				write_log $$ $step "$infile exists in $datapath add _1" $serv_log $stream_type
				infile=${infile}_1
			done
			temp_dir=${cemhome}${infile}_tmp
			#check if dir exists
			if [ -d "$temp_dir" ]
				then
				write_log $$ $step "$temp_dir is not empty" $serv_log $stream_type
				rm -rf $temp_dir
				continue
			fi
			mkdir $temp_dir
			to_dir=$temp_dir
			temp_dir=""
		fi
		for filepath in `find $from_dir -maxdepth 1 -regextype posix-awk -regex $file_template`
      do
    	if [ $step = "parsed" ]
    		then
    		filesize=$(echo `du -b $filepath | awk '{print $1}'` | bc)
	      totalsize=$(echo ${totalsize}+${filesize} | bc)
	      if [ $totalsize -ge $critsize ]
	      	then
	      	write_log $$ $step "Reached max total session size ($(echo $critsize/1024/1024 | bc) MB). Stopped at $filepath" $serv_log $stream_type
	      	totalsize=0
	        break
	      fi
	    fi
      fname=`basename $filepath`
      if [[ `grep $fname $files_log` ]]
      then
        write_log $$ $step "$fname already in $files_journey_log - should be already moved from $from_dir to $to_dir" $serv_log $stream_type
        rm -f $filepath
        [ $? -eq 0 ] && write_log $$ $step "$fname removed" $serv_log $stream_type || write_log $$ $step "$fname Couldn't be removed" $serv_log $stream_type
      else
        mv -bf $filepath ${to_dir}/${fname}${postfix}
        if [ $? -eq 0 ]
        then
          write_log $$ $step "$fname moved from $from_dir to $to_dir" $files_journey_log $stream_type
          echo $fname >> $files_log
          if [ $step = "parsed" ]
          	then
          	cat_parsed_files $stream_type $fname $to_dir $infile
            [ $? -ne 0 ] && write_log $$ $step "Couldn't cat $fname to $infile. $?" $serv_log $stream_type
          fi
          count_files=$(echo $count_files+1|bc)
          if [ $load_index -eq 0 ]
          then
            load_index=1
            [ $step = "parsed" ] || echo >> $serv_log
            write_log $$ $step "started moving files from $from_dir to $to_dir" $serv_log $stream_type
          fi
        else
          write_log $$ $step "Couldn't move $fname from $from_dir to $to_dir" $serv_log $stream_type
        fi
      fi
    done
    if [ $step = "parsed" ]
    	then
    	mv ${to_dir}/${infile} ${datapath}/${infile}
    	if [ $? -eq 0 ]
	    then
	      write_log $$ $step "`wc -l ${datapath}/${infile}`" $files_journey_log $stream_type
	      filepath=${datapath}/${infile}
	      chmod 666 $filepath
	      num_hosts=${#aim_host[@]}
	      for (( i=0; i<${num_hosts}; i++ ))
	      do
	        write_log $$ $step "started moving $infile to ${aim_host[$i]}:${aim_path[$i]}" $serv_log $stream_type
	        move_to_remote_host $filepath ${aim_path[$i]} ${aim_host[$i]}
	        res=$?
	        if [ $res -eq 0 ]
	          then
	          write_log $$ $step "$infile was successfuly moved to ${aim_path[$i]}"buf" on ${aim_host[$i]}" $serv_log $stream_type
	        elif [ $res -eq 1 ]
	          then
	          write_log $$ $step "Couldn't move $infile to ${aim_path[$i]} on ${aim_host[$i]}" $serv_log $stream_type
	          break
	        else
	          write_log $$ $step "Couldn't move $infile to ${aim_path[$i]}"buf" on ${aim_host[$i]}" $serv_log $stream_type
	          break
	        fi
	      done
	      mv ${datapath}/${infile} ${arhpath}/${infile}
	      rm -rf ${to_dir}
	      chsum1=$(ls -cl $from_dir | grep -P $file_template | md5sum | cut -f1 -d " ")
      	chsum2=$chsum1
	    else
	      write_log $$ $step "Couldn't move ${to_dir}/${infile} to ${datapath}/${infile}" $serv_log $stream_type
	    fi
    fi
	fi
	sleep 1
done
