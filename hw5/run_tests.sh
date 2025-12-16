#!/usr/bin/env bash
# echo "Actual bash running this script: $BASH"
# echo "Bash version: $BASH_VERSION"

# Contributors: Jessica Card, Anonymous, Pranav Mantri
# Special thanks to Jessica Card for original script and colors
# Special thanks to Pranav Mantri for parallelization
# Thanks to Ryan Sherby, Brian Paick, and Anonymous for contributions
# Author: Wei Alexander Xin
# Last edited: Nov 17, 2025
# Version: 1.07
# https://github.com/eggrollofchaos/go_batch_test_script

# MAJOR CHANGES IN VERSION 1.07
# Fixed test failure message capture logic.
#   After finding '--- FAIL:', look for failure message in preceding line(s), not next line.

# MAJOR CHANGES IN VERSION 1.06
# Added pre-flight build check: checks to ensure source code is able to build before running tests.
#   Build errors are captured and output to terminal.

# INTRODUCTION
# This is fairly robust batch test script for COMS 4113 Golang programming assignments using the <go test> command.
# Supports SERIAL or PARALLEL testing modes.
# For SERIAL testing, progress is displayed for each run, good for immediate feedback.
# For PARALLEL testing, progress is displayed every PROG_INT tests.
# All tests are run individually in their own process regardless.
#
# Usage:
#   See `help` section below (around line 600).
#
# Note re: logging --
# The verbosity levels in this script requires adding logging via `slog` to your `*_test.go` file, otherwise will fail.
# See README for more information on how to implement.
# If logging isn't enabled, just omit any verbosity flags (-v) from command line.
#
# Note re: timing --
# The SLOW_TIME threshold is for tracking test runs that are slow to complete, defaulting to 1m.
# Runs that exceed threshold are marked SLOW but will be allowed to complete if within the hard TIMEOUT deadline.
# The hard TIMEOUT deadline for each run is set by the -timeout flag, defaulting to 2m.
# Hitting the <go test> TIMEOUT deadline causes a panic with exit status 2 in the log output, and are marked FAILED.
#
# Note re: homework assignments --
# The setup is such that if you do not specify a test suite (omit -z) and do not use positional test names,
# The `./...` will find all tests in all subdirectories, i.e. pkg/paxos, pkg/pingpong.

##########################################################
# ================ COMS 4113 TEST SUITES =============== #
##########################################################

A3A=("TestBasic" "TestDeaf" "TestForget" "TestManyForget" "TestForgetMem" "TestRPCCount" "TestMany" "TestOld" "TestManyUnreliable" "TestPartition" "TestLots")
A3B=("TestBasic" "TestDone" "TestPartition" "TestUnreliable" "TestHole" "TestManyPartition")

A4A=("TestBasic" "TestUnreliable" "TestFreshQuery") # /shardmaster/test_test.go
A4B=("TestBasic" "TestMove" "TestLimp" "TestConcurrent" "TestConcurrentUnreliable") # /shardkv/test_test.go

A5A_State=("TestHashAndEqual" "TestStateInherit" "TestNextStates" "TestPartition") # pkg/pingpong/state_test.go
A5A_Search=("TestBasic" "TestBfsFind" "TestBfsFindAll1" "TestBfsFindAll2" "TestBfsFindAll3" "TestRandomWalkFindAll" "TestRandomWalkFind") # pkg/pingpong/search_test.go
A5A_PP=("TestServerEqual" "TestClientEqual" "TestMessageEqual") # pkg/pingpong/pingpong_test.go
A5A_All=( "${A5A_State[@]}" "${A5A_Search[@]}" "${A5A_PP[@]}" )

A5B=("TestUnit") # pkg/paxos/paxos_test.go")

# Note, TestFailChecks is not graded
A5C_Basic=("TestBasic" "TestBasic2" "TestBfs1" "TestBfs2" "TestBfs3" "TestInvariant" "TestPartition1" "TestPartition2" "TestFailChecks") # pkg/paxos/scenario_test.go
A5C_Pred=("TestCase5Failures" "TestNotTerminate" "TestConcurrentProposer") # pkg/paxos/scenario_test.go
A5C_All=( "${A5C_Basic[@]}" "${A5C_Pred[@]}" )

##########################################################
# ========= SET CONFIGURATION, DEFAULTS, STYLE ========= #
##########################################################

# To kill all children upon Ctrl-C
trap 'echo; printf "\nAborted by user; killing all child processes.\n\n"; kill 0; exit 130' SIGINT

# DEFAULTS
##########################################################
# Test type, number of sets (loops)
BATCH_TYPE=SERIAL                   # Serial or Parallel processing
TEST_STR="s"                        # default infix is 's' for Serial, otherwise 'p' for Parallel
TOTAL_SETS=100                      # defaults to 100 for Serial, or 500 or Parallel

# Test suite
IS_SUITE=false                      # track whether test suite is selected, using -z
TESTSUITE=(${A4A[@]})               # default is Assignment 4 Part A
SUITE_NAME="N/A"                    # default is no test suite
SELECTED_TESTS=()                   # default is empty array

# Total batch size
NUM_TESTS=2                         # based on test in TESTSUITE, or individually defined tests
TOTAL_TEST_EX=200                   # total test runs to execute: TOTAL_SETS x NUM_TESTS

# Parallel testing config
PROG_INT=10                         # interval of tests between progress reports
NUM_PROCS=2                         # per-test # of processes
                                    # WARNING: if you select 4 tests, with num_procs=2 this script will spawn 8 processes,
                                    # which is okay for most personal laptops these days, but may starve threads on a 2 core VM
CHUNK_SIZE=50                       # chunk size: TOTAL_SETS / NUM_PROCS
TOTAL_PROCS=4                       # total parallel processes: NUM_TESTS x NUM_PROCS

# Logging, time thresholds
VERBOSE=0                           # debug logging verbosity level: 0=None, 1=Error, 2=Warn, 3=Info, 4=Debug/Trace
VERBOSE_STR="None"
VERBOSE_CMD=""
SLOW_TIME=1m                        # SLOW threshold: set lower to be vigilant
SLOW_TIME_SEC=60
TIMEOUT=2m                          # hard deadline for each run, enforced by <go test> -timeout flag
TIMEOUT_SEC=120

# Test runs (executions) result counters
NOT_RUN=0                           # runs that were NOT RUN due to test having invalid name, i.e. <go test> returns 'no tests to run'
PASSED=0                            # runs that PASSED (within SLOW time threshold)
SLOW=0                              # runs that PASSED but exceeded SLOW time threshold
FAILED=0                            # runs that FAILED due to error or exceeding hard TIMEOUT
SKIPPED=0                           # (FAIL-FAST) after failure, runs SKIPPED for that test

# COLORS AND STYLE
##########################################################
# These ANSI codes have the form "\033[ + xxx + m":
#   \033[XXXm
# Usage:
#   echo -e "${ESC}[$MODIFIERS...]${END}[text]${RESET_ALL}"
# See: https://stackoverflow.com/questions/4842424/list-of-ansi-color-escape-sequences
ESC="\033["
RED="31"
GREEN="32"
YELLOW="33"
GREY="38;5;7"
BRIGHT_MAGENTA="38;5;13"
BRIGHT_BLUE="38;5;39"
BRIGHT_CYAN="96"
BOLD="1"
UNDERLINE="4"
SLOW_BLINK="5"
CANARY="38;5;11"
ON_BLACK="40"
ON_GREY="48;5;255"
RESET="0"
END="m"
RESET_ALL=$ESC$RESET$END

# Predefined colors and styles
CANARY_BOLD_ON_BLACK="$ESC$CANARY;$BOLD;$ON_BLACK$END"
BOLD_UNDERLINE="$ESC;$BOLD;$UNDERLINE$END"
BRIGHT_MAGENTA_BOLD_ON_BLACK="$ESC$BRIGHT_MAGENTA;$BOLD;$ON_BLACK$END"
BRIGHT_BLUE_BOLD_ON_BLACK="$ESC$BRIGHT_BLUE;$BOLD;$ON_BLACK$END"
BRIGHT_CYAN_ON_BLACK="$ESC$BRIGHT_CYAN;$ON_BLACK$END"
BRIGHT_CYAN_BOLD_ON_BLACK="$ESC$BRIGHT_CYAN;$BOLD;$ON_BLACK$END"
BRIGHT_CYAN_SLOW_BLINK_ON_BLACK="$ESC$BRIGHT_CYAN;$ON_BLACK;$SLOW_BLINK$END"
GREY_ON_BLACK="$ESC$GREY;$ON_BLACK$END"
GREY_BOLD_ON_BLACK="$ESC$GREY;$BOLD;$ON_BLACK$END"
GREEN_BOLD_ON_BLACK="$ESC$GREEN;$BOLD;$ON_BLACK$END"
YELLOW_BOLD_ON_BLACK="$ESC$YELLOW;$BOLD;$ON_BLACK$END"
RED_ON_GREY="$ESC$RED;$ON_GREY$END"
RED_BOLD_ON_GREY="$ESC$RED;$BOLD;$ON_GREY$END"
RED_BOLD_SLOW_BLINK_ON_GREY="$ESC$RED;$BOLD;$ON_GREY;$SLOW_BLINK$END"

# Welcome
printf "${CANARY_BOLD_ON_BLACK}\n\n"
printf "Batch test script for COMS 4113 Golang programming assignments using <go test> command.\n${RESET_ALL}\n"
echo

##########################################################
# =============== BASIC HELPER FUNCTIONS =============== #
##########################################################

# Help function to display error requiring nonnegative input
input_error_positive() {
  local arg_name=$1
  local flag=$2
  local value=$3
  local flag_str=" (-$flag)"

  if [[ -z ${flag} ]]; then
    flag_str=""
  fi
  echo "Input error --"
  echo "${arg_name}${flag_str} must be at least 1, got ${value}"
  echo
}

# Helper function to parse times in seconds from a string (like 23m or 1s)
extract_time_from_string() {
  local time_str=$1
  local time_sec=0

  if [[ $time_str =~ ^([0-9]+)([ms])$ ]]; then
    # Get time in seconds
    time_num=${BASH_REMATCH[1]}
    time_unit=${BASH_REMATCH[2]}
    if [[ $time_unit == "m" ]]; then
      time_sec=$((time_num * 60))
    else
      time_sec=$time_num
    fi
  else                                            # not formatted properly
    time_str="error"
  fi

  echo "$time_str|$time_sec"
}

# Helper function to check that source code is able to build successfully
pre_flight_build_check() {
  # printf "${BRIGHT_CYAN_ON_BLACK}Running pre-flight build check...${RESET_ALL}\n"
  # Capture both stdout and stderr from the build command
  build_output=$(go build ./... 2>&1)
  build_status=$?

  # Build failed
  if (( build_status != 0 )); then
    printf "${RED_BOLD_ON_GREY}BUILD FAILED${RESET_ALL}\n"
    printf "Batch test job cannot begin because source code does not compile.\n"
    printf "Please fix the compile errors below:\n\n"
    # Print the captured build error
    echo "$build_output"
    echo
    exit 1
  # else
    # printf "${GREEN_BOLD_ON_BLACK}Build check passed.${RESET_ALL}\n\n"
  fi
}
# Helper function to parse through positional arguments for test names
discover_tests() {
  if (( "${#SELECTED_TESTS[@]}" == 0 )); then     # no test names specified in command line
    # Find tests with go test -list=.
    mapfile -t tests_raw < <(go test -list=. | grep '^Test')
    SELECTED_TESTS=("${tests_raw[@]}")
    if (( ${#SELECTED_TESTS[@]} == 0 )); then
      echo "Error: No tests discovered with 'go test -list=.'"
      echo
      exit 1
    fi
    TEST_STR="${TEST_STR}_all"                    # `all` ~ run all tests

  else # (( "${#SELECTED_TESTS[@]}" > 0 ))        # tests were specified in command line
    declare -A seen_tests                         # for checking dupes
    for test_name in "${SELECTED_TESTS[@]}"; do
      # Check if name starts with "Test"
      if [[ ! $test_name =~ ^Test ]]; then
        echo "Input error --"
        echo "Invalid test name(s) specified; in Go, test functions must start with 'Test'"
        echo
        exit 1
      fi

      # Check for duplicates
      if [[ -v seen_tests[$test_name] ]]; then
        echo "Input error --"
        echo "Duplicate test name specified: '$test_name'"
        echo
        exit 1
      fi

      # Mark as seen
      seen_tests[$test_name]=1
    done
    TEST_STR="${TEST_STR}_spec"                   # `spec`` ~ run specific tests
  fi
}

# Helper function to clean older log and output files
cleanup_old_test_files() {
  rm -f "all_selected_tests_${TEST_STR}.txt"
  rm -f "unique_tests_not_run_${TEST_STR}.txt"
  rm -f "unique_full_pass_tests_${TEST_STR}.txt"
  rm -f "unique_slow_tests_${TEST_STR}.txt"
  rm -f "unique_failed_tests_${TEST_STR}.txt"
  rm -f "problematic_tests_${TEST_STR}.txt"
  rm -f progress_${TEST_STR}_*.txt
  rm -f output_${TEST_STR}_*.log
  rm -f output_${TEST_STR}_*_summary.txt          # only used for Parallel
}

# Helper function to format test names with color/style based on result (at end of batch run)
# Also adds hanging indent, good for Configuration display as well
format_test_names() {
  # Try to get actual terminal width, fallback to 120
  # Invisible ANSI codes take up space, so adding 30 as buffer
  # For the results, they are all color-coded, adding 14*NUM_TESTS
  local config_results=$1
  local buffer=30
  if [[ $config_results == "results" ]]; then
    buffer=$((10 * NUM_TESTS))
  fi

  local max_width=$(( $(tput cols 2>/dev/null || echo 120) + buffer ))
  if (( max_width > ( 160 + buffer ) )); then     # cap at 160 + 30
    max_width=$(( 160 + buffer ))
  elif (( max_width < ( 80 + buffer ) )); then    # minimum 80 + 30
    max_width=$(( 80 + buffer ))
  fi
  # echo "max_width=$max_width"

  local prefix_len=28                   # 28 spaces align with `  Tests to run: ...` prefix
  local indent=$(printf "%${prefix_len}s" '')     # generated as padding
  local current_line=""
  local styled_tests=""                 # final output line(s)

  # Read all unique_* files once
  local not_run_tests=$(cat "unique_tests_not_run_${TEST_STR}.txt" 2>/dev/null)
  local failed_tests=$(cat "unique_failed_tests_${TEST_STR}.txt" 2>/dev/null)
  local slow_tests=$(cat "unique_slow_tests_${TEST_STR}.txt" 2>/dev/null)
  local full_pass_tests=$(cat "unique_full_pass_tests_${TEST_STR}.txt" 2>/dev/null)
  
  # Iterate over all selected tests
  for test_name in "${SELECTED_TESTS[@]}"; do
    local test_style=""                 # default: plain

    # Check if test is in NOT_RUN, SLOW, FAILED, or FULL_PASS files
    [[ $not_run_tests == *"$test_name"* ]] && test_style=$GREY_BOLD_ON_BLACK
    [[ $slow_tests == *"$test_name:"* ]] && test_style=$YELLOW_BOLD_ON_BLACK
    [[ $failed_tests == *"$test_name:"* ]] && test_style=$RED_BOLD_ON_GREY
    [[ $full_pass_tests == *"$test_name"* ]] && test_style=$GREEN_BOLD_ON_BLACK
    
    # Build test name, stylized based on result
    local styled_test="${test_style}${test_name}${RESET_ALL}"

    # local test_with_space="$plain_test"
    local current_plain=$(echo "$current_line" | sed 's/\x1b\[[0-9;]*m//g')     # strip ANSI codes
    local projected_len=$((prefix_len + ${#current_plain} + ${#test_name}))

    # Check if appending this test would exceed max width
    if (( projected_len > max_width )) && [[ -n "$current_line" ]]; then
      # Output current line and start a new one
      styled_tests+="$current_line\n${indent}"
      current_line="$styled_test "
    else  # Will be within the max width
      # Append this test
      current_line+="$styled_test "
    fi
  done

  # Add final line
  if [[ -n "$current_line" ]]; then
    styled_tests+="$current_line"
  fi
  
  printf "%b\n" "$styled_tests"
}

# Helper function to track elapsed time over the entire batch job
format_elapsed_time() {
  local total_seconds=$1
  local hours=$((total_seconds / 3600))
  local minutes=$(( (total_seconds % 3600) / 60 ))
  local seconds=$((total_seconds % 60))
  
  local time_str=""
  if (( hours > 0 )); then
    time_str+="${hours}h"
  fi
  if (( minutes > 0 )); then
    time_str+="${minutes}m"
  fi
  time_str+="${seconds}s"
  
  echo "$time_str"
}

# Helper function to pretty-print progress report using colors & styles, Parallel mode
print_progress_report_aligned() {
  local test_name=$1
  local runs=$5
  local slow=$3
  local slow_str=$4
  local failed=$2
  local total=$6
  local prog_pct=$((100 * runs / total))
  local prog_fmt=$(printf '%3d' "$prog_pct")      # default to a 2 digit number, therefore pad 1 space

  # Define styles
  local passed_style="$GREEN_BOLD_ON_BLACK"
  local slow_style="$YELLOW_BOLD_ON_BLACK"
  local failed_style="$RED_BOLD_ON_GREY"

  # Define default prefix, progress, and reporting strings
  local prefix="${failed_style}[Progress: ${prog_fmt}%%]${RESET_ALL} ${test_name}"
  local progress_info="${runs} run(s) completed"
  local some_slow="${slow} test(s) ${slow_style}SLOW (${slow_str})${RESET_ALL}"
  local some_failed="${failed} test(s) ${failed_style}FAILED${RESET_ALL}"
  local conj1=", "
  local affix1=""
  local conj2=""
  local affix2=""
  local chunk_done=""

  # Logic for what to report
  if (( failed == 0 && slow == 0 )); then         # all PASSED
    prefix="${passed_style}[Progress: ${prog_fmt}%%]${RESET_ALL} ${test_name}"
    progress_info="${runs} run(s) completed in this worker"
    conj1=""
  elif (( failed == 0 && slow > 0 )); then        # some SLOW
    prefix="${slow_style}[Progress: ${prog_fmt}%%]${RESET_ALL} ${test_name}"
    affix1="$some_slow"
  elif (( failed > 0 && slow == 0 )); then        # some FAILED
    affix1="$some_failed"
  else    # some SLOW & some FAILED, however based on logic this is impossible: FAILED takes precedence
    affix1="$some_slow"
    conj2=", "
    affix2="$some_failed"
  fi

  if (( prog_pct == 100 )); then
    chunk_done=" - chunk done"
  fi

  # Print with proper alignment and style
  printf "${prefix}: ${progress_info}${conj1}${affix1}${conj2}${affix2}${chunk_done}\n"
}

# Helper function to compare time in seconds
# If the time is similar enough, output "yes", otherwise "no"
time_similar_enough() {
  local x=$1                  # reference time
  local y=$2                  # time to compare

  local min=1
  local max=0

  if (( x < 4 )); then
    max=$((2 * x + 1))
  elif (( x >= 4 && x <= 10 )); then
    min=$((3 * x / 4))
    max=$((x + 5))
  else # x > 10
    min=$((3 * x / 4))
    max=$((3*x/2))
  fi

  # Do comparison
  if (( y >= min && y <= max )); then
    echo "yes"
  else
    echo "no"
  fi
}

# Helper function for output unknown errors to file
output_unknown_error() {
  local test_name=$1
  local fail_status=$2
  local fail_msg=$3
  local outfile=$4

  echo "unknowns"
  printf '[%s] --\n' "$(date)" >> "$outfile"
  ps -p $$ -o args= >> "$outfile"
  printf '%-27s %s - %s\n\n' "$test_name:" "$fail_status" "$fail_msg" >> "$outfile"
}

# Helper function to produce final test result counts, Parallel mode
# Aggregate test counts from summary files
aggregate_counts_from_summary() {
  # NOT_RUN=0; PASSED=0; SLOW=0; FAILED=0; SKIPPED=0
  shopt -s nullglob
  for sum_file in output_${TEST_STR}_*_summary.txt; do
    if [[ -s $sum_file ]]; then
      NOT_RUN=$(( NOT_RUN + $(grep NOT_RUN "$sum_file" | awk '{print $2}') ))
      PASSED=$(( PASSED + $(grep PASSED "$sum_file" | awk '{print $2}') ))
      SLOW=$(( SLOW + $(grep SLOW "$sum_file" | awk '{print $2}') ))
      FAILED=$(( FAILED + $(grep FAILED "$sum_file" | awk '{print $2}') ))
      SKIPPED=$(( SKIPPED + $(grep SKIPPED "$sum_file" | awk '{print $2}') ))
    fi
  done
  shopt -u nullglob
}

# Helper function to calculate which tests Fully Passed (no FAILED, no SLOW)
calculate_fully_passing_tests() {
  local all_fully_passed=$2       # false or true
  
  # Determine Fully Passing tests from unique_* files
  printf '%s\n' "${SELECTED_TESTS[@]}" | sort > "all_selected_tests_${TEST_STR}.txt"
  
  if [[ $all_fully_passed == true ]]; then
    printf '%s\n' "${SELECTED_TESTS[@]}" >> "unique_full_pass_tests_${TEST_STR}.txt"
  else
    (cat "unique_failed_tests_${TEST_STR}.txt" 2>/dev/null | awk '{print $1}' | sed 's/:$//'; \
     cat "unique_slow_tests_${TEST_STR}.txt" 2>/dev/null | awk '{print $1}' | sed 's/:$//'; \
     cat "unique_tests_not_run_${TEST_STR}.txt" 2>/dev/null) | sort -u > "problematic_tests_${TEST_STR}.txt"

    comm -23 "all_selected_tests_${TEST_STR}.txt" "problematic_tests_${TEST_STR}.txt" > "unique_full_pass_tests_${TEST_STR}.txt"
  fi
}

# Helper function to count # of lines in a file for tallying results
count_file_lines() {
  local file=$1
  local count=0
  
  if [[ -f $file ]]; then
    count=$(wc -l < "$file" 2>/dev/null | tr -d ' ')
  fi
  
  echo "$count"
}

# Helper function to set colors and styles based on results
set_color_styles() {
  not_run_style=""
  passed_style=""
  slow_style=""
  failed_style=""
  pass_or_slow_rate_style=""
  full_pass_rate_style=""
  
  (( NOT_RUN > 0 )) && not_run_style=$GREY_BOLD_ON_BLACK
  (( PASSED > 0 )) && passed_style=$GREEN_BOLD_ON_BLACK
  (( PASSED == TOTAL_TEST_EX )) && full_pass_rate_style=$passed_style
  (( SLOW > 0 )) && slow_style=$YELLOW_BOLD_ON_BLACK && pass_or_slow_rate_style=$slow_style && full_pass_rate_style=$slow_style
  (( FAILED > 0 )) && failed_style=$RED_BOLD_ON_GREY && full_pass_rate_style=$failed_style
}

##########################################################
# ===== PARSE COMMAND LINE ARGUMENTS, PRINT CONFIG ===== #
##########################################################

# Parse options
##########################################################
while getopts "pn:g:z:vs:t:h" opt; do
  case ${opt} in
  p )                                       # set to choose Parallel mode
    BATCH_TYPE="PARALLEL"
    TOTAL_SETS=500                          # update default
    TEST_STR="p"
    ;;
  n )                                       # set to specify number of processes per test
    NUM_PROCS="$OPTARG"
    if (( NUM_PROCS < 1 )); then
      input_error_positive "NUM_PROCS" "n" "$OPTARG"
      exit 1
    fi
    ;;
  g )                                       # set to specify number of tests between progress reports
    PROG_INT="$OPTARG"
    if (( PROG_INT < 1 )); then
      input_error_positive "PROG_INT" "g" "$OPTARG"
      exit 1
    fi
    ;;
  z )                                       # set to specify a predefined test suite
    SUITE_NAME="$OPTARG"
    if declare -p "$SUITE_NAME" &>/dev/null; then
      # indirect expansion into TESTSUITE array
      eval "TESTSUITE=(\"\${${SUITE_NAME}[@]}\")"
    else
      echo "Input error --"
      echo "Test suite '${SUITE_NAME}' not defined. Run $0 -h to see all available suites."
      echo
      exit 1
    fi
    IS_SUITE=true
    ;;
  v )                                       # set verbosity level
    ((VERBOSE++))                           # each additional v increases verbosity
    case $VERBOSE in
    1)
      VERBOSE_STR="Error"
      VERBOSE_CMD="-loglevel 1"
      ;;
    2)
      VERBOSE_STR="Warn"
      VERBOSE_CMD="-loglevel 2"
      ;;
    3) 
      VERBOSE_STR="Info"
      VERBOSE_CMD="-loglevel 3"
      ;;
    4) 
      VERBOSE_STR="Debug"
      VERBOSE_CMD="-loglevel 4"
      ;;
    esac

    ;;
  s )                                       # set to specify SLOW time threshold
    slow_arg=$(extract_time_from_string $OPTARG)
    SLOW_TIME=${slow_arg%%|*}
    SLOW_TIME_SEC=${slow_arg#*|}

    if [[ $SLOW_TIME == "error" ]]; then
      echo "Input error --"
      echo "Invalid slow time threshold format: '$OPTARG'. Use ^[0-9][ms] (e.g., 30s, 2m)."
      echo
      exit 1
    fi
    if (( SLOW_TIME_SEC < 1 )); then
      input_error_positive "SLOW_TIME_SEC" "s" "$OPTARG"
      exit 1
    fi
    ;;
  t )                                       # set to specify hard TIMEOUT threshold
    timeout_arg=$(extract_time_from_string $OPTARG)
    TIMEOUT=${timeout_arg%%|*}
    TIMEOUT_SEC=${timeout_arg#*|}

    if [[ $TIMEOUT == "error" ]]; then
      echo "Input error --"
      echo "Invalid hard timeout format: '$OPTARG'. Use ^[0-9][ms] (e.g., 30s, 2m)."
      echo
      exit 1
    fi
    if (( TIMEOUT_SEC < 1 )); then
      input_error_positive "TIMEOUT_SEC" "s" "$OPTARG"
      exit 1
    fi
    ;;
  h )                                       # set to show this Help
    echo "Usage: $0 [options] [TOTAL_SETS] [TestName1] [TestName 2 ...]"
    echo
    echo "Options:"
    echo "  -p             Run in PARALLEL mode; if omitted, will run in SERIAL mode."
    echo "  -n NUM_PROCS   Set number of parallel processes per test (default: 2)."
    echo "  -g PROG_INT    Set progress report interval (default: 10)."
    echo "  -z TESTSUITE   For PARALLEL mode, set test suite (supported: A4A, A4B, A5A_State, A5A_Search, A5A_PP, A5A_All, A5B, A5C_Basic, A5C_Pred, 5C_All). If omitted, will default to all in directory."
    echo "  -v[v][v][v]    Set debug logging verbosity (-v Error, -vv Warning, -vvv Info, -vvvv Trace). Default is none, however system level logs from <go test> will always print."
    echo "  -s SLOW_TIME   Set slow threshold per test run, e.g. 2m, 1m, 30s (default: 1m). Runs exceeding this threshold are marked SLOW."
    echo "  -t TIMEOUT     Set hard timeout deadline per test run (default: 2m). Runs exceeding this threshold are marked FAILED."
    echo "  -h             Show this Help."
    echo
    echo "Positional Arguments:"
    echo "  TOTAL_SETS     Number of sets of tests to run (default: 100)."
    echo "  TestName1...   Names of tests to run per set (default: all)."
    echo
    echo "Examples:"
    echo "  $0                                   # (Serial) run all tests 100 times, no debug logging, 1m/2m slow/timeout."
    echo "  $0 -v 50 TestUnreliable -s 45s       # (Serial) run TestUnreliable 50 times, loglevel Error, 45s/2m slow/timeout."
    echo "  $0 -pvv -n 3 -g 25 -s 30s -t 1m      # (Parallel) run all tests in directory 500 times, 2 processes per test,"
    echo "                                           progress report every 25 tests, loglevel Warn, 30s/1m slow/timeout."
    echo "  $0 -pvvv -n 20 -s 30s TestBasic      # (Parallel) run TestBasic 500 times, 20 processes per test,"
    echo "                                           progress report every 10 tests, loglevel Info, 30s/2m slow/timeout."
    echo "  $0 -pvvvv -g 50 -z A4A -t 45s 1000   # (Parallel) run test suite A4A 1000 times, 2 processes per test,"
    echo "                                           progress report every 50 tests, loglevel Trace, 1m/45s slow/timeout."
    echo
    exit 0
    ;;
  \? )
    echo "Invalid option: -$OPTARG" >&2
    echo
    exit 1
    ;;
  : )
    echo "Option -$OPTARG requires an argument" >&2
    echo
    exit 1
    ;;
  esac
done
shift $((OPTIND -1))

# Warn if TIMEOUT <= SLOW_TIME
if (( TIMEOUT_SEC <= SLOW_TIME_SEC )); then
  echo "Note, TIMEOUT is set less than or equal to SLOW_TIME; under these conditions tests will never return SLOW, they will all be FAILED upon reaching TIMEOUT."
  echo
fi

# Parse position arguments
##########################################################
# If $1 is a number, it's TOTAL_SETS; otherwise interpret as a test name
if [[ "$1" =~ ^[0-9]+$ ]]; then
  TOTAL_SETS=$1
  if (( TOTAL_SETS < 1 )); then
    input_error_positive "TOTAL_SETS" "" "$1"
    exit 1
  fi
  shift
fi

# Validate that positional arguments are either numbers or valid test names
for arg in "$@"; do
  # Check if there appear to be an option flag(s) remaining (likely in error)
  if [[ $arg =~ ^- ]]; then
    echo "Input error --"
    echo "Additional arguments present after positional arguments: '$arg'"
    echo "Run $0 -h to see help."
    echo
    exit 1
  fi
done

# Don't allow both -z TESTSUITE and positional test names
if [[ $IS_SUITE == true ]] && (( $# > 0 )); then
  echo "Error: Cannot specify both -z TESTSUITE and explicit test names."
  echo
  exit 1
fi

# Ensure that code actually builds successfully
pre_flight_build_check

# If used -z TESTSUITE
if [[ $IS_SUITE == true ]]; then
  SELECTED_TESTS=("${TESTSUITE[@]}")
  TEST_STR="${TEST_STR}_${SUITE_NAME}"
# If used positional test names
else 
  SELECTED_TESTS=("$@")
  discover_tests                                  # process test names and set TEST_STR
fi
NUM_TESTS=${#SELECTED_TESTS[@]}
TOTAL_TEST_EX=$((TOTAL_SETS * NUM_TESTS))

# Validate parallel testing params and prepare environment
##########################################################
if (( NUM_PROCS > TOTAL_SETS )); then             # NUM_PROCS ≤ TOTAL_SETS
  NUM_PROCS=$TOTAL_SETS
fi
TOTAL_PROCS=$((NUM_PROCS * NUM_TESTS))          # TOTAL_PROCS ≤ TOTAL_TEST_EX

CHUNK_SIZE=$((TOTAL_SETS / NUM_PROCS))          # CHUNK_SIZE ≥ 1
if (( PROG_INT > CHUNK_SIZE / 2)); then          # PROG_INT ≤ MAX(CHUNK_SIZE/2, 1)
  PROG_INT=$((CHUNK_SIZE / 2))
  if (( PROG_INT == 0 )); then
    PROG_INT=1
  fi
fi

# Sanity check; though based on constraints, the below can never happen
if (( TOTAL_SETS < PROG_INT )); then
  PROG_INT=$TOTAL_SETS
fi

# Prepare logging environment
cleanup_old_test_files                  # delete older files to prepare for batch run

# Print configuration
##########################################################
printf "${BOLD_UNDERLINE}BATCH CONFIGURATION${RESET_ALL}\n"
if [[ $BATCH_TYPE == "PARALLEL" ]]; then
  batch_style=$BRIGHT_BLUE_BOLD_ON_BLACK
else
  batch_style=$BRIGHT_MAGENTA_BOLD_ON_BLACK
fi
printf "  Batch test type:          ${batch_style}${BATCH_TYPE}${RESET_ALL}\n"
echo "  Test suite:               ${SUITE_NAME}"
printf "  Tests to run:             $(format_test_names "config")\n"
echo "    # Tests:                ${NUM_TESTS}"
printf "  Sets to run:              ${BRIGHT_CYAN_BOLD_ON_BLACK}${TOTAL_SETS}${RESET_ALL}\n"
printf "    # Tests to execute:     ${BRIGHT_CYAN_ON_BLACK}${TOTAL_TEST_EX}${RESET_ALL}\n"
if [[ $BATCH_TYPE == "PARALLEL" ]]; then
  echo "  Processes per test:       ${NUM_PROCS}"
  echo "    Total processes:        ${TOTAL_PROCS}"
  echo "    Chunk size:             ${CHUNK_SIZE}"
  echo "  Progress interval:        ${PROG_INT}"
fi
echo "  Slow threshold:           ${SLOW_TIME}"
echo "  Hard timeout:             ${TIMEOUT}"
echo "  Log verbosity:            ${VERBOSE} (${VERBOSE_STR})"
echo
printf "${BRIGHT_CYAN_BOLD_ON_BLACK}=== Running test loop ===${RESET_ALL}\n"
echo

##########################################################
# ===================== SUBROUTINES ==================== #
##########################################################

# Function for extracting and formatting failure category (status) and reasons (messages)
extract_failure_detail() {
  local logfile=$1
  local duration=$2
  local timeout_sec=$3
  local fail_msg=""
  local search_lines=250
  local fail_status="UNK_ERROR"
  local error_line

  # 1) Check for panic timeout line
  error_line=$(grep -m1 -E '^panic: test timed out after' "$logfile" || true)
  # Add duration comparison as sanity check
  if [[ -n "$error_line" ]] && (( duration >= timeout_sec )); then
    fail_status="TIMEOUT"
    fail_msg="$error_line"
    echo "$fail_status|$fail_msg"
    return
  fi

  # 2) Search for failure block with '--- FAIL:' and capture error message on an earlier line
  # Look through last 250 lines (in case there are a bunch of stack traces or logging at the end)
  # Need to look at last occurrence in case multiple runs (and errors) are captured in 250 lines
  local tail_lines=$(tail -n $search_lines "$logfile")

  # Grep with line numbers, find last occurrence of FAIL line, get line number
  local fail_line_num=$(echo "$tail_lines" | grep -n '^--- FAIL:' | tail -1 | cut -d: -f1)
  if [[ -n "$fail_line_num" ]]; then
    fail_status="ERROR"
    # Within this block of (up to) 250 lines, get everything up to the FAIL line
    local lines_before_fail=$(echo "$tail_lines" | head -n "$((fail_line_num - 1))")

    # Search for last line containing '_test.go' (the actual error message)
    error_line=$(echo "$lines_before_fail" | grep '_test\.go:' | tail -1)

    # Strip leading/trailing whitespace
    fail_msg=$(echo "$error_line" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
    echo "$fail_status|$fail_msg"
    return
  fi

  # 3) Default fallback for unknown failure but non-zero exit
  fail_status="UNK_ERROR"
  # Output the failure line itself
  error_line=$(echo "$tail_lines" | head -n "$((fail_line_num - 1))")
  fail_msg="$error_line - unable to find failure message in log file"
  echo "$fail_status|$fail_msg"
}

# Function for checking and appending SLOW result to file
# Uses a fuzzy match on duration, in case finish times for SLOW tests differ significantly
check_and_append_slow() {
  local test_name=$1
  local duration=$2
  local outfile=$3
  
  local similar_time="no"

  mapfile -t found_lines < <(grep "^$test_name" "$outfile" 2>/dev/null)

  # If test not found in SLOW file at all, add
  if (( ${#found_lines[@]} == 0 )); then
    printf '%-27s SLOW (~%ss)\n' "$test_name:" "$duration" >> "$outfile"

  # If test found in SLOW file, check times and compare duration; append if time differs materially
  else
    local similar_time_tmp="yes"

    # Loop over existing lines for this test, usually just 1 or 2
    for test_line in "${found_lines[@]}"; do

      # Look for the actual duration(s) recorded previously and compare; e.g. ...SLOW (~10s)
      if [[ "$test_line" =~ SLOW\ \(~([0-9]+[ms])\) ]]; then
        time_str="${BASH_REMATCH[1]}"
      else
        similar_time_tmp="no"
      fi

      # Extract time and compare durations
      ref_time=$(extract_time_from_string "$time_str")
      ref_time_sec=${ref_time#*|}
      similar_time_tmp=$(time_similar_enough "$ref_time_sec" "$duration")

      # If new time is similar to at least one existing line, no need to add
      if [[ $similar_time_tmp == "yes" ]]; then
        similar_time="yes"
        break
      fi

    done

    # Done with loop; if new time not similar to any previous times, append to SLOW file
    if [[ $similar_time == "no" ]]; then
      printf '%-27s SLOW (~%ss)\n' "$test_name:" "$duration" >> "unique_slow_tests_${TEST_STR}.txt"
    fi
  fi
}

# Function for pretty-printing FAILED run results using colors & styles, Parallel mode
print_failure_aligned() {
  local test_name=$1
  local run_num=$2
  local duration=$3
  local fail_status=$4
  local fail_msg=$5
  local to_skip=$6
  local style=$7
  # local prefix_fail="[FAILURE]  "
  local prefix="[FAIL-SLOW]"            # changed for clarity and alignment
  local fail_info="$fail_status - $fail_msg"
  local skip_str=""

  # FAIL-FAST logic - only skip remaining for TIMEOUT failures
  if [[ $fail_status == "TIMEOUT" ]]; then
    prefix="[FAIL-FAST]"
    skip_str=$(printf '%*s%s' 61 '' "- skipping remainder of chunk (${to_skip} runs)\n")  # padding 61 spaces
  fi

  # Build prefix with colors / styles
  local prefix_styled="${style}${prefix}${RESET_ALL} ${test_name} failed on run #${run_num} (${duration}s):"
  local prefix_plain="${prefix} ${test_name} failed on run #${run_num} (${duration}s):"

  # Calculate length of plain text for padding
  local prefix_len=${#prefix_plain}
  
  # Calculate spaces needed to pad to 59 characters
  local pad_width=$((59 - prefix_len))
  if (( pad_width < 1 )); then
    pad_width=1
  fi
  
  # Build padding
  local padding=$(printf '%*s' "$pad_width" '')
  
  # Print with proper alignment and style
  printf "${prefix_styled}${padding}${fail_info}\n${skip_str}"
}

# Serial test loop function
# Run one test up to N=TOTAL_SET times
# Stop on failure, skip remaining runs for this test only if TIMEOUT
run_serial_test_loop() {
  local test_name=$1
  local iter=$2

  local outfile="output_${TEST_STR}_${test_name}_${iter}.log"

  local start=$(date +%s)
  go test -run="^$test_name\$" -v -count=1 -timeout="$TIMEOUT" $VERBOSE_CMD ./... > $outfile 2>&1
  local result=$?
  local end=$(date +%s)
  local duration=$((end - start))
  local to_skip=0

  # Check `no tests to run` warning, very pesky
  if grep -q 'testing: warning: no tests to run' "$outfile"; then
    to_skip=$((TOTAL_SETS))
    printf "${GREY_BOLD_ON_BLACK}NOT RUN${RESET_ALL} - No tests to run for ${test_name} - check test names or build\n"
    # echo "                                          - skipping remaining runs for this test"
    echo "$test_name" >> "unique_tests_not_run_${TEST_STR}.txt"
    printf '%*s%s\n' 42 '' "- skipping all ${to_skip} runs for this test"           # padding 42 spaces

    ((NOT_RUN += TOTAL_SETS))                     # increment NOT_RUN counter by TOTAL_SETS
    skip_no_run_map["$test_name"]=1               # skip remaining runs for this test
    mv "$outfile" "${outfile%.log}_not_run.log"   # add "not_run" suffix to log filename
    return
  fi

  # Test FAILED
  if (( result != 0 )); then

    failure_info=$(extract_failure_detail "$outfile" "$duration" "$TIMEOUT_SEC")
    fail_status=${failure_info%%|*}               # extract everything before the first |
    fail_msg=${failure_info#*|}                   # extract everything after the first |

    printf "${RED_BOLD_ON_GREY}FAILED${RESET_ALL} (${duration}s) - ${fail_status} - ${fail_msg}\n"
    ((FAILED++))

    # Only FAIL-FAST skip for TIMEOUT failures
    if [[ "$fail_status" == "TIMEOUT" ]]; then
      to_skip=$((TOTAL_SETS - iter))
      # echo "                                          - skipping remaining runs for this test"
      printf '%*s%s\n' 42 '' "- skipping remaining ${to_skip} runs for this test"   # padding 42 spaces
      ((SKIPPED += to_skip))                      # increment SKIPPED counter based on current run index
      skip_failed_map["$test_name"]=1             # skip remaining runs for this test
    fi

    # Write failure status and message to file for concise reporting
    printf '%-27s %s - %s\n' "$test_name:" "$fail_status" "$fail_msg" >> "unique_failed_tests_${TEST_STR}.txt"
    mv "$outfile" "${outfile%.log}_failed.log"    # add "failed" suffix to log filename

    # Write command and failure info for any unknown errors    
    if [[ ${fail_status} == "UNK_ERROR" ]]; then
      output_unknown_error "$test_name" "$fail_status" "$fail_msg" "unknown_errors_${TEST_STR}.txt"
    fi

  else
    # Exceeded SLOW threshold
    if (( duration > SLOW_TIME_SEC )); then
      printf "${YELLOW_BOLD_ON_BLACK}SLOW${RESET_ALL} (${duration}s)\n"
      # Only record test_name once per test
      check_and_append_slow "$test_name" "$duration" "unique_slow_tests_${TEST_STR}.txt"
      ((SLOW++))
      mv "$outfile" "${outfile%.log}_slow.log"    # add "slow" suffix to log filename
    # PASSED without reservation
    else
      printf "${GREEN_BOLD_ON_BLACK}PASSED${RESET_ALL} (${duration}s)\n"
      ((PASSED++))
      rm "$outfile"                               # delete log file if this test run passes
    fi
  fi
}

# Parallel test loop function
# Run one test up to N=TOTAL_SET times, divided roughly evenly into chunks of size=CHUNK_SIZE
# Stop on failure, skip remaining runs in this loop only if TIMEOUT
# Each loop is a separate background process
run_parallel_test_loop() {
  local test_name=$1
  local n_start=$2
  local n_end=$3
  local outfile="output_${TEST_STR}_${test_name}_${n_start}_${n_end}.log"
  local progress_file="progress_${TEST_STR}_${test_name}_${n_start}_${n_end}.txt"
  local chunk_size=$((n_end - n_start + 1))

  local not_run=0
  local passed=0
  local slow=0
  local failed=0
  local skipped=0
  local to_skip=0

  local start=0
  local end=0
  local duration=0
  local slow_min=0
  local slow_max=0
  local slow_str=""

  for ((i=n_start; i<=n_end; i++)); do
    echo "Run #$i for $test_name" >> "$outfile"

    start=$(date +%s)
    go test -run="^$test_name\$" -v -count=1 -timeout="$TIMEOUT" $VERBOSE_CMD ./... >> "$outfile" 2>&1
    result=$?
    end=$(date +%s)
    duration=$((end - start))

    echo "----------------------------------------" >> "$outfile"

    # Check `no tests to run` warning, very pesky
    if grep -q 'testing: warning: no tests to run' "$outfile"; then
      not_run=$((n_end - i + 1))                # set not_run counter = entire chunk size
      break                                       # skip remaining tests in this chunk
    fi

    # Test FAILED
    if (( result != 0 )); then

      failure_info=$(extract_failure_detail "$outfile" "$duration" "$TIMEOUT_SEC")
      fail_status=${failure_info%%|*}
      fail_msg=${failure_info#*|}

      # pretty-print failure line
      to_skip=$((n_end - i))
      print_failure_aligned "$test_name" "$i" "$duration" "$fail_status" "$fail_msg" "$to_skip" "$RED_BOLD_ON_GREY"
      failed=1

      # Only FAIL-FAST skip for TIMEOUT failures
      if [[ "$fail_status" == "TIMEOUT" ]]; then
        skipped=$to_skip                          # set skipped counter = remaining tests in this chunk
        break
      fi

    # Test PASSED
    else
      # Exceeded SLOW threshold
      if (( duration > SLOW_TIME_SEC )); then
        if (( slow_min == 0 || duration < slow_min)); then  # first time or new slow_min time
          slow_min=$duration
        fi
        if (( duration > slow_max )); then        # new slow_max time
          slow_max=$duration
        fi
        ((slow++))
      # Passed without reservation
      else
        ((passed++))
      fi
    fi

    # Print progress report once every PROG_INT runs
    if (( (i - n_start + 1) % PROG_INT == 0 )); then
      # Compose duration string for SLOW jobs
      if (( slow_min == slow_max )); then
        slow_str="~${slow_max}s"
      else
        slow_str="${slow_min}s ~ ${slow_max}s"
      fi

      runs_completed=$((i - n_start + 1))
      total_in_chunk=$((n_end - n_start + 1))
      print_progress_report_aligned "$test_name" "$failed" "$slow" "$slow_str" "$runs_completed" "$total_in_chunk"
    fi

    # After each test completes, atomically update progress_file
    (               # Uses a lock mechanism not dissimilar to what we learned in class
      flock 200
      echo "$((i - n_start + 1))" > "$progress_file"
    ) 200>"${progress_file}.lock"
  done

  # After all tests in this process is done, atomically write unique test categorization
  # Also do final update to progress_file
  (                 # Uses a lock mechanism not dissimilar to what we learned in class
    flock 200
    # NOT RUN
    if (( not_run > 0 )); then
      if ! grep -q "^$test_name$" "unique_tests_not_run_${TEST_STR}.txt" 2>/dev/null; then
        printf "${GREY_BOLD_ON_BLACK}[NOT RUN]${RESET_ALL}   No tests to run for ${test_name} - check test names or build\n"
        printf '%*s%s\n' 61 '' "- skipping chunk (${chunk_size} runs)"     # padding 61 spaces
        echo "$test_name" >> "unique_tests_not_run_${TEST_STR}.txt"
      fi
      mv "$outfile" "${outfile%.log}_not_run.log"     # add "not_run" suffix to log filename
    
    else
      # FAILED
      if (( failed == 1 )); then
        # Write failure status and message to file for concise reporting
        if ! grep -q "^$test_name:" "unique_failed_tests_${TEST_STR}.txt" 2>/dev/null; then
          printf '%-27s %s - %s\n' "$test_name:" "$fail_status" "$fail_msg" >> "unique_failed_tests_${TEST_STR}.txt"
        fi
        mv "$outfile" "${outfile%.log}_failed.log"    # add "failed" suffix to log filename

        # Write command and failure info for any unknown errors
        if [[ ${fail_status} == "UNK_ERROR" ]]; then
          output_unknown_error "$test_name" "$fail_status" "$fail_msg" "unknown_errors_${TEST_STR}.txt"
        fi
      
      # SLOW
      # If a test chunk has a mix of SLOW vs FAILED results, FAILED will take precedence
      elif (( slow > 0 )); then
        # Look for existing SLOW entries, append if none or if duration is sufficiently different
        check_and_append_slow "$test_name" "$slow_max" "unique_slow_tests_${TEST_STR}.txt"
        mv "$outfile" "${outfile%.log}_slow.log"      # add "slow" suffix to log filename for this chunk
      else          # all test runs in chunk PASSED
        # Delete log file -- to save space! especially when doing TRACE logging
        rm "$outfile"
      fi
    fi

    # Update progress file to mark chunk complete
    echo "$chunk_size" > "$progress_file"

  ) 200>"unique_tests.lock"

  # Write individual test run counters
  echo "NOT_RUN $not_run" > "${outfile%.log}_summary.txt"
  echo "PASSED $passed" >> "${outfile%.log}_summary.txt"
  echo "SLOW $slow" >> "${outfile%.log}_summary.txt"
  echo "FAILED $failed" >> "${outfile%.log}_summary.txt"
  echo "SKIPPED $skipped" >> "${outfile%.log}_summary.txt"
}

# Monitoring function for tracking aggregate progress, print progress at 10% intervals and every 10 seconds
# Will be run as background process 
monitor_overall_progress () {
  local start_time=$1                   # Unix timestamp at beginning of entire batch job
  local pids_to_monitor=("${@:2}")      # array of all child PIDs generated
  local current_time=0
  local elapsed=0
  local elapsed_str=""
  local polling_interval=10

  local milestone_interval=$((TOTAL_TEST_EX / 10))
  local total_completed=0
  local current_milestone=0
  local pct_done=0
  local prev_milestone=0
  local all_done=1
  local prev_report=0
  local reports_skipped=0

  printf "${BRIGHT_CYAN_SLOW_BLINK_ON_BLACK}Monitoring overall progress...${RESET_ALL}\n\n"

  while true; do
    # Check if any monitored processes are still running
    all_done=1
    for pid in "${pids_to_monitor[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        all_done=0
        break
      fi
    done

    # Aggregate progress from all worker files
    total_completed=0
    
    shopt -s nullglob
    for prog_file in progress_${TEST_STR}_*.txt; do
      # Extract n_start and n_end from filename
      # e.g. progress_p_all_TestBasic_1_50.txt -> extract 1 and 50
      if [[ $prog_file =~ ([0-9]+)_([0-9]+)\.txt ]]; then
        local n_start=${BASH_REMATCH[1]}
        local n_end=${BASH_REMATCH[2]}
        local expected_runs=$((n_end - n_start + 1))

        # Read current progress from file
        local current_progress=$(cat "$prog_file" 2>/dev/null || echo 0)

        # Sum up tallies of completed test executions
        total_completed=$((total_completed + current_progress))
      fi
    done
    shopt -u nullglob

    # If all processes are done AND we've completed all tests, exit
    if (( all_done == 1 && total_completed >= TOTAL_TEST_EX )); then
      printf "\n${BRIGHT_CYAN_BOLD_ON_BLACK}[Overall:  100%%] All %s test executions completed in %s.${RESET_ALL} \n" \
        "$TOTAL_TEST_EX" "$elapsed_str"
      break
    fi
    if (( total_completed == 0 )); then           # skip progress report at 0
      continue
    fi
    # If progress hasn't moved, skip this poll
    # Skip up to 2 times, i.e. 30 seconds max time between polling if default interval
    if (( total_completed == prev_report )); then
      ((reports_skipped++))
      if (( reports_skipped<=2 )); then
        sleep $polling_interval                   # sleep until next poll
        continue                                
      else
        reports_skipped=0                         # skipped twice, print a report
      fi
    else
      reports_skipped=0                           # total_completed has increased, reset the skip counter
    fi

    # Otherwise continue to calculate overall progress and report
    current_milestone=$((total_completed / milestone_interval))

    # Calculate elapsed time
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    elapsed_str=$(format_elapsed_time $elapsed)
    
    # Check if we've crossed a 10% milestone, if so display prominently
    if (( current_milestone > prev_milestone && current_milestone <= 10 )); then
      pct_done=$((current_milestone * 10))
      printf "\n${BRIGHT_CYAN_BOLD_ON_BLACK}[Overall: %4d%%] %d/%d test executions completed (%s)${RESET_ALL}\n\n" \
        "$pct_done" "$total_completed" "$TOTAL_TEST_EX" "$elapsed_str"
      prev_milestone=$current_milestone
    # Otherwise print currently progress less prominently
    else
      pct_done=$((100 * total_completed / TOTAL_TEST_EX))
      printf "${BRIGHT_CYAN_ON_BLACK}[Overall: %4d%%]${RESET_ALL} %d/%d test executions completed (%s)\n" \
        "$pct_done" "$total_completed" "$TOTAL_TEST_EX" "$elapsed_str"
    fi
    prev_report=$total_completed
    
    sleep $polling_interval
  done
}

##########################################################
# ================= MAIN TEST LOGIC ==================== #
##########################################################

# Serial processing
## All tests run sequentially, then each set is repeated
##########################################################
if [[ $BATCH_TYPE == "SERIAL" ]]; then

  declare -A skip_no_run_map
  declare -A skip_failed_map

  test_run=""
  run_info=""

  # Primary loop
  for i in $(seq 1 $TOTAL_SETS); do
    for test_name in "${SELECTED_TESTS[@]}"; do

      test_run="Test run $i/$TOTAL_SETS"
      run_info="${test_run}: ${test_name}: "  

      if [[ ${skip_no_run_map[$test_name]:-0} -eq 1 ]]; then
        continue                                  # Skip test because not valid
      fi
      if [[ ${skip_failed_map[$test_name]:-0} -eq 1 ]]; then
        continue                                  # Skip test due to failure
      fi

      printf '%-39s ' "$run_info"                 # print current iteration + test + padding to 39 char
      run_serial_test_loop "$test_name" "$i"

    done
  done

# Parallel processing
## Multiple processes spawned per test, divided into chunks
##########################################################
else

  # For proper locking for check-and-write
  rm -f unique_tests.lock
  rm -f progress_${TEST_STR}_*.lock
  touch unique_tests.lock

  declare -a worker_pids                          # track worker PIDs
  batch_job_start=$(date +%s)                     # track test loop start time

  # Primary loop
  for test_name in "${SELECTED_TESTS[@]}"; do
    for ((p=0; p<NUM_PROCS; p++)); do             # launch NUM_PROCS processes per test
      n_start=$((p * CHUNK_SIZE + 1))
      n_end=$(( (p+1) * CHUNK_SIZE ))
      if (( p == NUM_PROCS - 1 )); then
        n_end=$TOTAL_SETS                         # last worker will take remainder
      fi
      run_parallel_test_loop "$test_name" "$n_start" "$n_end" &
      worker_pids+=($!)  # Capture PID
    done
  done

  # launch monitoring progress, only for sufficient large jobs
  if (( TOTAL_TEST_EX > 50 )); then
    monitor_overall_progress "$batch_job_start" "${worker_pids[@]}" &
    monitor_pid=$!
  fi
  
  wait                                            # wait for all child processes to finish

  # Kill monitor if still running
  kill $monitor_pid 2>/dev/null
  wait $monitor_pid 2>/dev/null

  # Clean up progress files
  rm -f progress_${TEST_STR}_*.txt progress_${TEST_STR}_*.lock

  aggregate_counts_from_summary                   # get counts for final reporting
fi

##########################################################
# ========== TALLY AND DISPLAY RESULTS / LOGS ========== #
##########################################################

# Display results summary
##########################################################
tests_not_run=0
tests_full_pass=0
tests_slow=0
tests_failed=0
tests_skipped=0

all_passed=false                                  # tracks whether all tests PASSED, even if some were SLOW
all_fully_passed=false                            # tracks whether all tests PASSED and none were SLOW
if (( PASSED + SLOW == TOTAL_TEST_EX )); then
  all_passed=true
  if (( SLOW == 0 )); then 
    all_fully_passed=true
  fi
fi
pass_or_slow_rate=$((100 * (PASSED + SLOW)/ TOTAL_TEST_EX))
full_pass_rate=$((100 * PASSED / TOTAL_TEST_EX))

# Determine which tests Fully Passed (no SLOW runs, no FAILED runs)
# Tally up NOT RUN, SLOW, FAILED, and Fully Passing
calculate_fully_passing_tests "$all_fully_passed"
tests_not_run=$(count_file_lines "unique_tests_not_run_${TEST_STR}.txt")
tests_slow=$(count_file_lines "unique_slow_tests_${TEST_STR}.txt")
tests_failed=$(count_file_lines "unique_failed_tests_${TEST_STR}.txt")
tests_full_pass=$(count_file_lines "unique_full_pass_tests_${TEST_STR}.txt")

echo
echo "========================================================"
echo
printf "${BOLD_UNDERLINE}RESULTS SUMMARY${RESET_ALL}             ${batch_style}${BATCH_TYPE} MODE${RESET_ALL}\n"
printf "  Sets to run:              ${BRIGHT_CYAN_BOLD_ON_BLACK}${TOTAL_SETS}${RESET_ALL}\n"
printf "  Tests to run:             $(format_test_names "results")\n"
echo "    # Tests:                ${NUM_TESTS}"
printf "  Total test executions:    ${BRIGHT_CYAN_ON_BLACK}${TOTAL_TEST_EX}${RESET_ALL}\n"

set_color_styles                        # set colors and styles based on test results

# Print tallies
printf "    ${not_run_style}Not run:                ${NOT_RUN}${RESET_ALL}\n"
printf "    ${passed_style}Passed:                 ${PASSED}${RESET_ALL}\n"
printf "    ${slow_style}Slow:                   ${SLOW}${RESET_ALL}\n"
printf "    ${failed_style}Failed:                 ${FAILED}${RESET_ALL}\n"
printf "    ${failed_style}Skipped (post-fail):    ${SKIPPED}${RESET_ALL}\n"
printf "    ${pass_or_slow_rate_style}Pass or slow rate:      ${pass_or_slow_rate}%%${RESET_ALL}\n"
printf "    ${full_pass_rate_style}Pass rate (ex-slow):    ${full_pass_rate}%%${RESET_ALL}\n"
echo

# Print verbal summary
##########################################################

## All PASSED
if [[ $all_passed == true ]]; then
  echo "All tests passed."

  # If all within SLOW time limit (Fully Passed)
  if [[ $all_fully_passed == true ]]; then 

    # If total sets of tests ≥ 100 OR total tests executed >= 500, not bad
    # Set to 2 and 10 for testing
    if (( TOTAL_SETS >= 100 || TOTAL_TEST_EX >= 500 )); then
      printf "${full_pass_rate_style}Nice.${RESET_ALL}\n"
    fi

  # Some SLOW
  else
    printf "However, ${slow_style}${tests_slow} test(s)${RESET_ALL} had runs that took longer than ${slow_style}${SLOW_TIME}${RESET_ALL} to complete.\n"
  fi

## Not Fully Passing
else

  # Some tests NOT_RUN
  if (( tests_not_run > 0 )); then
    printf "${not_run_style}${tests_not_run} test(s)${RESET_ALL} were not run due to test name error.\n"
  fi

  # Some tests Fully Passed?
  if (( tests_full_pass > 0 )); then
    printf "${passed_style}${tests_full_pass} test(s)${RESET_ALL} fully passed.\n"
  else
    echo "Sadly, 0 tests fully passed."
  fi

  # Some tests SLOW
  if (( tests_slow > 0 )); then
    printf "${slow_style}${tests_slow} test(s)${RESET_ALL} had runs that took longer than ${slow_style}${SLOW_TIME}${RESET_ALL} to complete.\n"
  fi

  # Some tests FAILED
  if (( tests_failed > 0 )); then
    printf "${failed_style}${tests_failed} test(s)${RESET_ALL} experienced failures.\n"
  fi
fi

echo
echo "========================================================"
echo

# Display additional detail and logs
##########################################################

# Check edge case
total_tests_check=$((NOT_RUN + PASSED + SLOW + FAILED + SKIPPED))
if (( total_tests_check != TOTAL_TEST_EX )); then
  printf "${RED_BOLD_SLOW_BLINK_ON_GREY}Test execution count validation error:${RESET_ALL}\n"
  printf "NOT_RUN + PASSED + SLOW + FAILED + SKIPPED = ${RED_BOLD_ON_GREY}${total_tests_check}${RESET_ALL}\n"
  printf "Tally of tests executed does not match expected value of ${BRIGHT_CYAN_BOLD_ON_BLACK}${TOTAL_TEST_EX}${RESET_ALL}.\n"
  echo "There may be older log files that need to be expunged prior to re-running tests."
  echo "Or there is a bug in the script. Gasp!"
  echo
fi

# Print any tests NOT RUN
if (( tests_not_run > 0 )); then
  printf "${not_run_style}TESTS NOT RUN:${RESET_ALL}\n"
  cat unique_tests_not_run_${TEST_STR}.txt
  echo
fi

# Print any Fully Passed tests
if (( tests_full_pass > 0 )); then
  printf "${passed_style}FULLY PASSING TESTS:${RESET_ALL}\n"
  cat unique_full_pass_tests_${TEST_STR}.txt
  echo
fi

# If not all Fully Passing with flying colors...
if [[ $all_fully_passed == false ]]; then

  # Print any SLOW tests
  if (( tests_slow > 0 )); then
    printf "${slow_style}SLOW TESTS:${RESET_ALL}\n"
    cat unique_slow_tests_${TEST_STR}.txt
    echo
  fi

  # Print any FAILED tests
  if (( tests_failed > 0 )); then
    printf "${failed_style}FAILED TESTS + REASONS:${RESET_ALL}\n"
    cat unique_failed_tests_${TEST_STR}.txt
    echo
  fi

  # Show test log files
  printf "${BOLD_UNDERLINE}Test logs:\n"
  ls -la output_${TEST_STR}_*.log 2>/dev/null
  echo
fi
