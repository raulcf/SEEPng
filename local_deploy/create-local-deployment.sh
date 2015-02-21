#!/bin/bash
###############
## Script to create local SEEPng deployments
## Options:
## seeppath (sp): path to seep install directory
## numworkers (nw): num workers required
## sessionname (sn): tmux session name
## logpath (lp): path to a writable directory to dump all logs
## help (h): displays help
###############

# Variables
SEEP_PATH="" # mandatory parameter
NUM_WORKERS="" # mandatory parameter
TMUX_SESSION_NAME="SEEPng"
LOG_PATH=""
DISPLAY_HELP=""

HELP_MESSAGE="Help:
-sp | --seeppath    (mandatory): path to the 'install' directory of SEEPng
-nw | --numworkers  (mandatory): desired num of workers
-sn | --sessionname (optional):  Tmux session name (used to attach later)
-lp | --logpath     (optional):  writable directory to dump all SEEP worker and master logs
-h  | --help        (optional):  displays this message

"

# Parse input parameters
if [[ $# -lt 4 ]] 
	then
	echo "Need to specify more parameters"
	DISPLAY_HELP=1
fi

while [[ $# > 1 && $DISPLAY_HELP -ne 1 ]]
do
key="$1"

case $key in
	-sp|--seeppath)
    SEEP_PATH="$2"
    shift
    ;;
    -nw|--numworkers)
    NUM_WORKERS="$2"
    shift
    ;;
    -sn|--sessionname)
    TMUX_SESSION_NAME="$2"
    shift
    ;;
    -lp|--logpath)
    LOG_PATH="$2"
    shift
    ;;
    -h|--help)
    DISPLAY_HELP=1
    shift
    ;;
    *)
	echo "Unrecognized option"
    DISPLAY_HELP=1 # unknown option
    ;;
esac
shift
done

# Check for mandatory parameters
if [[ $SEEP_PATH == "" || $NUM_WORKERS -eq "" ]]
	then
	echo "Need to specify mandatory parameters"
	DISPLAY_HELP=1
fi

# Check whether something failed, show help and exit
if [[ $DISPLAY_HELP -eq 1 ]]
	then
	echo "$HELP_MESSAGE"
	exit
fi

echo " " 
echo "###############"
echo "## Input parameters provided: "
echo "## Seep path: $SEEP_PATH"
echo "## Num workers: $NUM_WORKERS"
echo "## Tmux session name: $TMUX_SESSION_NAME"
echo "## Log path: $LOG_PATH"
echo "###############"
echo " "

##############
## Prepare TMUX environment
##############

let num_windows_req=$NUM_WORKERS/4
let num_panes_last_window=$NUM_WORKERS-$num_windows_req*4

echo "TMUX session with: $num_windows_req windows aprox"

# One full window for master
tmux new-session -d -s $TMUX_SESSION_NAME -c $SEEP_PATH -n 'Master'

# One window with max 4 panes for workers (create as many windows as necessary)
let current_window=0
let worker=0
while [[ $current_window < $num_windows_req ]] 
do
	let worker_range=$worker+3
#	let w_name="Worker_$worker_$worker_range"
	let w_name=$(printf 'Worker-%s-%s' "worker" "worker_range")
	tmux new-window -c $SEEP_PATH -n "Worker-$w_name"
	# Create 4 panels and lay them out nicely
	tmux split-window -c $SEEP_PATH
	tmux split-window -c $SEEP_PATH
	tmux split-window -c $SEEP_PATH
	tmux select-layout tiled # Lay panes nicely in the screen
	let current_window=$current_window+1
	let worker=$worker+4
done

# Check whether it's necessary to create panes for a last window
if [[ $num_panes_last_window > 0 ]]
	then
	let worker_range="$worker+$num_panes_last_window"
	echo "worker_range: $worker_range"
#	let w_name="Worker $worker - $worker_range"
	let w_name=$(printf "Worker-%s-%s" "worker" "worker_range")
	tmux new-window -c $SEEP_PATH -n "Worker-$w_name"
	tmux new-window -c $SEEP_PATH -n "Worker-$w_name"
	while [[ $worker < $num_panes_last_window ]]
	do
		tmux split-window -c $SEEP_PATH
		let worker=$worker+1
	done
	tmux select-layout tiled
fi

echo " "
echo " "
echo "####################################"
echo "## TMUX session created!"
echo "## Execute the following command to connect to the running session:"
echo "## tmux attach -t $TMUX_SESSION_NAME"
echo "####################################"
echo " "
echo " "
exit
