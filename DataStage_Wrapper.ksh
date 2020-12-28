#!/bin/ksh
#
################################################################################
# sss_jjj_expnd.ksh -e environment -f filepattern ProjName Jobname 
# Input parameters:
#
#  -e environment - Application's environment: valid values are DEV or PRD.  This
#     effects the setting of some environment variables.
#   -m mode - RESTART or NEW
#  -h - help: show these comments
#  -f - File Validation flag. Use this flag if file validation is required. Supply the file pattern without the regular expression as the argument
#
#  Usage limitations:
#  	-e is required
# It is mandatory for the job being called to have the $APT_CONFIG_FILE parameter
#
#  Examples:
#  	sss_jjj_expnd.ksh -e dev -f testfile projectname test
#
# Modification History
#
# Date:      Name:		Desc:
# ---------- ---------- -------------------------------------------------------
# 10/14/2014 Jerome		Created
################################################################################
#
#

#############################################################################################################################
# Arguments - NA
# Return Values - NA
# Description - Function defining script usage
#############################################################################################################################
Usage()
{
	echo "Usage: $1 -e environment -m mode [-f file_pattern] [-h] DSProjectName DSJobName"
	exit
}

#############################################################################################################################
# Arguments - $1: Job name
# Return Values - NA
# Description - Function to clean-up all logs created by the job older than 2 weeks and data files older than 30 days
#############################################################################################################################
FileCLNUP()
{
	# remove files and logs > 30 days old
	echo `date` "Removing these files:"
	find ${log_dir}  -name "$1*" -mtime +14 -exec ls -lt {} \;
	find ${log_dir}  -name "$1*" -mtime +14 -exec rm {} \;
	#find ${data_dir} -name "*" -type f -mtime +30 -exec ls -lt {} \;
	#find ${data_dir} -name "*" -type f -mtime +30 -exec rm {} \;
	#echo "Data files older than 30 days and archives older than 2 weeks removed"
}

#############################################################################################################################
# Arguments - $1 : File pattern
# Return Values - NA
# Description - Function to validate the sssjjj file structure
#############################################################################################################################
Validatesssjjj()
{
file_pattern=$1
echo "Changing directory to ${ftp_dir}"
cd ${ftp_dir}
#Convert DOS files to UNIX format
dos2unix ${file_pattern}* 2>/dev/null
#############################
mkdir -p BADFILES
for filename in `ls ${file_pattern}*`;
do
ErrString=`awk -F "|" -v filename="${filename}" '
BEGIN { issue = "NO" }
#Record Validation
END{ print issue }' ${filename};`

S10Flag=`awk -F "|" '
BEGIN{ flag=0 }
{CNT++} /^S10/ {($2==CNT?"":flag=1); CNT=0;}
END{ print flag }' ${filename};`

if [ "$ErrString" == "YES" ]; then
#Enter logic to handle errors
fi

done
}

#############################################################################################################################
# Arguments - $1 : File pattern
# Return Values - 0 for success and 1 for failure
# Description - Function to check existence of files and validate whether or not they contain data
#############################################################################################################################
CheckFile()
{
echo "Changing directory to ${ftp_dir}"
cd ${ftp_dir}
return_val=1
echo "File pattern read by CheckFile() function : $1"
file_pattern=$1
file_cnt=`ls -lrt ${file_pattern}* 2>/dev/null | wc -l`
echo "Number of files with ${file_pattern} is ${file_cnt}"
if [ $file_cnt -eq 0 ]; then
echo "No files with pattern ${file_pattern}"
SendDelayMail
return 1
else
for ftp_file in `ls -tr ${file_pattern}*`
do
	if [[ -s ${ftp_file} ]]; then
	return_val=0
	else
	echo "File ${ftp_file} is empty"
	fi
done
fi

#Function used to validate sssjjj Source data. Remove this if not required
Validatesssjjj ${file_pattern}
return ${return_val}
}


#############################################################################################################################
# Arguments - $1: Project Name, $2: Job Name, $3: Reset flag (1/0)
# Return Values - 0 for success and 1 for failure
# Description - Function to check status of job. If job is not runnable, a reset is issued. If the job is unable to be reset, then returns 1 else 0
#############################################################################################################################
CheckJobStatus()
{
echo "Checking Job Status of "
echo -e "Project Name: $1 \n Job Name: $2 \n Reset Flag: $3"
DSPROJNAME=$1 
DSJOBNAME=$2
RFlag=$3
MFlag=$4
#BinFilsssrectory=`cat /.dshome`/bin 
#echo "Setting BinFilsssrectory = $BinFilsssrectory"
################################################################################ 
# Check job status here 
################################################################################ 

JOB_STATUS=`dsjob -jobinfo $DSPROJNAME $DSJOBNAME | head -1 | cut -d"(" -f2 | cut -d")" -f1` 
echo "JOB_STATUS=$JOB_STATUS" 
case ${JOB_STATUS} in 
################################################################################ 
# 0 "Running" 
################################################################################ 
0) 
echo "Job $DSJOBNAME already running.Job run Failed." 
exit 999 
;; 
################################################################################ 
# Runnable Job Status (do something) 
# 1 "Finished" 
# 2 "Finished with Warning (see log)" 
# 9 "Has been reset" 
# 11 "Validated OK" 
# 12 "Validated (see log)" 
# 21 "Has been reset" 
# 99 "Compiled" 
################################################################################ 
1|2|7|9|11|12|21|99) 
echo "Job is in runnable state"
return 0
;; 
################################################################################ 
# NOT Runnable Job Status (reset job) 
# 0 "Running" 
# 3 "Aborted" 
# 8 "Failed validation" 
# 13 "Failed validation" 
# 96 "Aborted" 
# 97 "Stopped" 
# 98 "Not Compiled" 
################################################################################ 
*) 
if [ $RFlag -eq 1 ]; then
	echo "Reset flag has been set to 1. Job will be issued reset command"
	#echo "${BinFilsssrectory}/dsjob -server :31539 -run -mode RESET -wait $DSPROJNAME $DSJOBNAME"
	dsjob -run -mode RESET -wait $DSPROJNAME $DSJOBNAME 
	CheckJobStatus $DSPROJNAME $DSJOBNAME 0
	RETURN_VALUE=$?
		if [ ${RETURN_VALUE} -ne 0 ] 
		then 
		echo "Unable to reset job $DSJOBNAME already running..Job run Failed."
		return 1 
		fi 
else echo "Reset flag has been set to 0. No reset will be performed"
		if [ $MFlag = "NEW" ]; then
		return 1
		else
		return 0
		fi
fi
esac 
}


#############################################################################################################################
# Arguments - $1: Project Name, $2: Job Name
# Return Values - 0 for success and 1 for failure
# Description - Function to run the DataStage job passed as parameter
#############################################################################################################################
RunDSJob()
{
echo "Running job with parameters"
echo -e "Project Name: $1 \n Job Name: $2 "
DSPROJNAME=$1
DSJOBNAME=$2
FILEPATTERN=$4
FILEFLAG=$3
CONFIG_FILE=`cat /.dshome`/../Configurations/default.apt
#BinFilsssrectory=`cat /.dshome`/bin 
#echo "BinFilsssrectory set to $BinFilsssrectory"
if [ $FILEFLAG -eq 1 ]; then
echo "File pattern passed to job - $FILEPATTERN"
dsjob -run -mode NORMAL -wait -warn 0 -param '$APT_CONFIG_FILE'=${CONFIG_FILE} -param 'pattern='${FILEPATTERN} -param parameterset=valuefile $DSPROJNAME $DSJOBNAME
temprc=$?
else
dsjob -run -mode NORMAL -wait -warn 0 -param '$APT_CONFIG_FILE'=${CONFIG_FILE} -param parameterset=valuefile $DSPROJNAME $DSJOBNAME
temprc=$?
fi
echo ${temprc}
if [ ${temprc} -ne 0 ]; then
return 1
else
CheckJobStatus $DSPROJNAME $DSJOBNAME 0
rc=$?
return $rc
fi
}

SendDelayMail()
{
echo $DSLOGGILE
echo $DSJOBNAME
status="File Not Found. ${tm} run delayed"  && mail_list=${dly_mail} && SendMail "$status" $DSLOGFILE $DSJOBNAME
exit 0
}

#############################################################################################################################
# Arguments - $1: Status, $2: Log file name, $3: Job name
# Return Values - NA
# Description - Function to send status mail
#############################################################################################################################
SendMail()
{
       export mail_list="emailids"     # for testing
	   echo "Sending mail with subject ${env}:  sss jjj status: $1"
        mailx -s "${env}: $3 status: $1" "${mail_list}" < $2
}

################################################################################
#
# Begin Here:
#  - process cmdline flags
#  - set env vars
#  - cleanup old files
#
################################################################################

#  process cmdline flags

while getopts e:m:f:h val
do
   case $val in
        e)      eflag=1;
                export env=`echo ${OPTARG} | tr "[a-z]" "[A-Z]"`;;
		m)		mflag=1;
				runmode=${OPTARG};; #Mode of run - either RESTART or NEW
		f)		fflag=1;
				file_pattern=${OPTARG};; #Source Files to be read and validated
        h)      hflag=1;;       # help
        *)      Usage $0;;
   esac
done

if [ "$hflag" ]; then
   head -$(($(grep -n "Modification History" $0 | sed 2,\$d | \
   cut -f1 -d:)-1)) $0
   Usage $0
fi

[[ -z "$eflag" ]] && printf "Option -e must be specified\n" && Usage $0
[[ -z "$mflag" ]] && printf "Option -m must be specified\n" && Usage $0
[[ "${runmode}" != "RESTART" && "${runmode}" != "NEW" ]] && printf "Mode option argument must either be NEW or RESTART\n" && Usage $0
[[ -z "$fflag" ]] && fflag=0



#  set datastage env vars
EnvFilePath=#set environment file path
. $EnvFilePath                                 
. `cat /.dshome`/dsenv

#  set insight env vars
. .envfile

#set -o xtrace
export dt=`date '+%Y%m%d'`
export tm=`date '+%H%M'`
export dttm=`date '+%Y%m%d%H%M%S'`
export log_dir=#set log dir path
export ftp_dir=#set source data dir path
export data_dir=##
export dt_ctl='sss_jjj.dt'
export status='Success'
export mail_list=${inf_mail}
export script_dir=#set path for script
#Inf Upg Prema
export dly_mail=${sssjjj_delay_mail}

export lst_run_dt=`cat ${ctl_dir}/${dt_ctl} | cut -c1-8`
export run_cnt=`cat ${ctl_dir}/${dt_ctl} | cut -c16`

shift $(($OPTIND -1)) #This command shifts the CLA to capture the arguments

#Setting the project name and the job name
export DSPROJNAME=$1 
export DSJOBNAME=$2
export DSLOGFILE=${log_dir}/${DSJOBNAME}_${dttm}.log

echo "Setting log name - ${DSLOGFILE}"
exec > ${DSLOGFILE} 2>&1
echo " "
echo "Executing in ${env} environment"
echo " "
#Validate parameters
[[ -z ${DSPROJNAME} ]] && echo -e "Please pass the project name" && Usage $0
[[ -z ${DSJOBNAME} ]] && echo -e "Please pass the Job name" && Usage $0

# remove old archived data and log files
FileCLNUP $DSJOBNAME


# initialise the run count in the first run of the day
if [[ ${lst_run_dt} -ne ${dt} ]]; then
   run_cnt=0
fi

# Set the the ftp dir as the current dir
cd ${ftp_dir}

if [ $runmode = "NEW" ]; then 
CheckJobStatus $DSPROJNAME $DSJOBNAME 1 $runmode
else  
CheckJobStatus $DSPROJNAME $DSJOBNAME 0 $runmode
fi

rc=$?
echo "Status = " $rc
if [ $rc -eq 0 ]; then
	if [ $fflag -eq 1 ]; then
	CheckFile ${file_pattern}
	rc=$?
		if [ $rc -eq 0 ]; then
		RunDSJob $DSPROJNAME $DSJOBNAME $fflag ${file_pattern}
		fi
	else
		RunDSJob $DSPROJNAME $DSJOBNAME $fflag
	fi
elif [ $rc -eq 999 ]; then
exit 0
fi

rc=$?
if [ $rc -eq 0 ]; then
runmode="NEW"
CheckJobStatus $DSPROJNAME $DSJOBNAME 0 $runmode
RETURN_CODE=$?
else
RETURN_CODE=1
fi

if [ ${RETURN_CODE} -ne 0 ]; then
echo "Job run failed. Please check director and script logs"
Status="Failed"
SendMail $Status $DSLOGFILE $DSJOBNAME
else 
echo "Job Run Successful. Please check director for any warnings"
Status="Success"
SendMail $Status $DSLOGFILE $DSJOBNAME
fi

exit 0
