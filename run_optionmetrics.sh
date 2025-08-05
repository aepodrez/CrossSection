#!/bin/bash
#$ -cwd
#$ -m abe

# Run OptionMetrics processing R script
echo "Starting OptionMetrics processing at `date`"

cd ~/PrepScripts

# Run the R script
/usr/local/bin/R --slave < OptionMetricsProcessing.R

echo "OptionMetrics processing completed at `date`" 