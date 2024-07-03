#!/bin/bash

SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
REPOPATH=`realpath $SCPATH/../../..`
PROJNAME=`echo $SCPATH | awk -F "/" '{print $(NF-1)}'`
export TZ=Asia/Taipei
PART_I=${1:-"0"}
PART_SIZE=5
TABLE_FILE="${REPOPATH}/projects/${PROJNAME}/table/Table_NonEmptyCols_fake.tsv"
OUT_PROCLOG="${SCPATH}/run_recurcount_nonempty_part${PART_I}.log"




nl_begin=$(((${PART_I}-1)*${PART_SIZE}+1))
nl_end=$((${PART_I}*${PART_SIZE}))

LIST_ANACOLS=(`cat ${TABLE_FILE} | grep -v -e "fake_id" -e "timestamp" | awk -F "\t" 'NR==1{
 for (idx=1;idx<=NF;idx++)
 {
  if ($idx=="schema1d_name")
   idxF=idx;
  if ($idx=="is_nonempty")
   idxN=idx;
 }
 next
}
{
 if ($idxN=="true")
  print $idxF;
}' | sed -n ''${nl_begin}','${nl_end}'p'`)

echo "processing partion $PART_I line number #$nl_begin to #$nl_end"
printf '%s\n' "${LIST_ANACOLS[@]}"

rm ${OUT_PROCLOG} | echo "creating a new log"
for iCOL in ${LIST_ANACOLS[@]};
do
 echo processing ${iCOL} ...
 $SCPATH/run_recurcount_nonempty.sh ${iCOL} >> ${OUT_PROCLOG} 2>&1
done

cat $OUT_PROCLOG | grep "RESULT:" | sort | uniq
