## /fs/homeu1/eccc/cmod/ords/production/smco502/.suites/rdps_20191231/r1/sequencing/wrapped/main/cmoi_post_processing/SRPD/Switch_GRIB/DND_extra.+18.wrapped.20211129180000.149726-eccc-ppp4-20211129205506
# CONFIG -> /fs/homeu1/eccc/cmod/ords/production/smco502/.suites/rdps_20211130/components/r1/cmoi_post_processing/config/main/cmoi_post_processing/grib_config/grib2.config
# DICT_DND -> /fs/homeu1/eccc/cmod/ords/production/smco502/.suites/rdps_20211130/components/r1/cmoi_post_processing/config/main/cmoi_post_processing/grib_config/grib2.dictionary_dnd
# PROCESS -> /fs/homeu1/eccc/cmod/ords/production/smco502/.suites/rdps_20211130/components/r1/cmoi_post_processing/config/main/cmoi_post_processing/grib_config/process.grib2



export ORDSOUMET_date_started="$(LC_TIME=C date)"
export ORDSOUMET_date_submitted="Mon Nov 29 20:55:07 UTC 2021"
export ORDSOUMET_date_begin_ordsoumet="Mon Nov 29 20:55:06 UTC 2021"
export ORDSOUMET_date_prepared="Mon Nov 29 20:55:07 UTC 2021"
export ORDSOUMET_date_submit_done=""

echo DND_extra.+18:0: JOB_ORDSOUMET at "$ORDSOUMET_date_begin_ordsoumet" Name=DND_extra.+18 Dest=eccc-ppp4 CPUS=1 SYSTEM_ID="${JOB_ID:-${LOADL_STEP_ID%.*}}"
echo DND_extra.+18:1: JOB_PREPARED at "$ORDSOUMET_date_prepared" Name=DND_extra.+18 Dest=eccc-ppp4 CPUS=1 SYSTEM_ID="${JOB_ID:-${LOADL_STEP_ID%.*}}"
echo DND_extra.+18:2: JOB_SUBMITTED at "$ORDSOUMET_date_submitted" Name=DND_extra.+18 Dest=eccc-ppp4 CPUS=1 SYSTEM_ID="${JOB_ID:-${LOADL_STEP_ID%.*}}"
echo DND_extra.+18:3: JOB_SUBMIT_DONE at "$ORDSOUMET_date_submit_done" Name=DND_extra.+18 Dest=eccc-ppp4 CPUS=1 SYSTEM_ID=""
echo DND_extra.+18:4: JOB_RUNWRAPPER at "$ORDSOUMET_date_runwrapper" Name=DND_extra.+18 Dest=eccc-ppp4 CPUS=1 SYSTEM_ID="${JOB_ID:-${LOADL_STEP_ID%.*}}"
echo DND_extra.+18:5: JOB_STARTED at "$ORDSOUMET_date_started" Name=DND_extra.+18 Dest=eccc-ppp4 CPUS=1 SYSTEM_ID="${JOB_ID:-${LOADL_STEP_ID%.*}}"

export JobStartTime=$(date +%s -d "$ORDSOUMET_date_started")
export JobTimeLimit=3600
export OMP_NUM_THREADS=1

# temporaire tant que je n'aurai pas trouvé de solution homogène...
if [[ "PBSPro" != "LoadLeveler5" ]]; then
	((OMP_NUM_THREADS=OMP_NUM_THREADS*1))
fi
export BATCH_MPI_CPUS=1

if [ -n "" ]; then
	export DISPLAY=
fi
if [ -n "" ]; then
	xterm -ls &
fi

# Define COSHEP_JOB_ID regardless of the version of coshep
export COSHEP_JOB_ID=${COSHEP_JOB_ID:-$GECOSHEP_JOB_ID}

# Find out using russ calls what are the fasttmp directories for each slot of the job in persistent containers
# For non-persistent container, use /tmp/$USER
gettmpfsdir ()
{
time for i in $(seq 0 $(($(rurun --count) - 1)) )
do
   (
    fspcstatus=$(rurun $i ruexec +/fspc/status -d fasttmp -t ${COSHEP_JOB_ID}-$i 2> /dev/null | tail -n +2 | tail -1 | awk '{print $2}' 2> /dev/null)
    if [ -z "$fspcstatus" ]
    then
        fspcstatus=/tmp/$USER
        rurun $i mkdir -p $fspcstatus
    fi
    echo "tmpfsdir[$i]="$fspcstatus
   ) &
done
time wait
}

# run gettmpfsdir and execute the output to define the array tmpfsdir
eval $(gettmpfsdir)

# As arrays are not exportable, copy it in an environment variable
export TMPFSDIR="${tmpfsdir[@]}"
unset tmpfsdir

# Define TMPDIR from the value of TMPFSDIR (first slot only)
. set_tmpdir.dot
#!/bin/ksh
#***
#
#.*****************************************************************************
#.
#.     JOB NAME - DND_extra.tsk
#.
#.     STATUS -  OPERATIONAL - SEMI-ESSENTIAL - to produce grib2 data for DND.
#.
#.     ORIGINATOR -  CMOI
#.
#.     DESCRIPTION - this job Generates GRIB2 data for ADS (DND) Client from the RDPS model 
#                          The data contains the extra fields that are not already part of the Ninjo production
#      July 25 2018 - Cathy Xie (CMOI)
#                        - Modified Benoit's version to add fields GZ TT ES UU VV HR GEOW(WT1) MX and levels for making grib2 data.         
#      July  2018   - Benoit Archambault (CMOI)
#                        - Produce GRIB2 data to ADS(DND) usage
#    February 2020  - David Anselmo (CMOI)
#                      - Apply a new etiket to the field WT1 in order to have the correct process id value.
#
#.*****************************************************************************

export TASK_CONFIG=${TASK_BASEDIR}/configs

typeset -Z3 PROG
typeset -i hh
resolution=ps10km
model=reg
grid="GRILLE(PS,935,824,456.2,732.4,10000.,21.0,NORD)"

# for WT1(GEOW)
hgt_wt1_2="1015,1000,985,970,950,925,900,875,850,800,750,700,650,600,550,500"
hgt_wt1_1="450,400,350,300,275,250,225,200,175,150,100,50" 
#for ES GZ TT UU VV HR WT1 
hgt_1="30,20,10"
# FOR HR
hgt_2="1015,985,970,950,875,550,450,350,275,225,175" 
 
# 
# Later, we will have to add also the hours between 36 and 120h
# for PROG in $(seq -w 0 3 33) $(seq 36 6 120); do
#
 
for PROG in $(seq -w 0 3 48) ; do
 rm -f pgsmpres pgsmeta 
 if [ $PROG -eq 000 ] ; then
    cat > pgsmeta <<EOF
 SORTIE(STD,400,A)
 $grid
 SETINTX(LINEAIR)
 HEURE(0)
 COMPAC=-12
 CHAMP('MX',0)
EOF
  fi
 echo " SORTIE(STD,400,A)"             >> pgsmeta
 echo " $grid"                         >> pgsmeta
 echo " HEURE($PROG)"                  >> pgsmeta
 echo " COMPAC=-12"                    >> pgsmeta
 echo " CHAMP('PN',0)"                 >> pgsmeta
 echo " SETINTX(LINEAIR)"              >> pgsmeta
 echo " SORTIE(STD,400,A)"             >> pgsmpres
 echo " $grid"                         >> pgsmpres
 echo " SETINTX(CUBIQUE)"              >> pgsmpres
 echo " HEURE($PROG)"                  >> pgsmpres
 echo " COMPAC=-12"                    >> pgsmpres
 echo " CHAMP('ES',${hgt_1})"          >> pgsmpres
 echo " CHAMP('TT',${hgt_1})"          >> pgsmpres
 echo " CHAMP('GZ',${hgt_1})"          >> pgsmpres
 echo " CHAMP('UU',${hgt_1})"          >> pgsmpres
 echo " CHAMP('VV',${hgt_1})"          >> pgsmpres
 echo " CHAMP('HR',${hgt_2})"          >> pgsmpres
 echo " CHAMP('HR',${hgt_1})"          >> pgsmpres
#####
 echo " SORTIE(STD,400,A)"             >> pgsmdiag
 echo " $grid"                         >> pgsmdiag
 echo " SETINTX(VOISIN)"               >> pgsmdiag
 echo " HEURE($PROG)"                  >> pgsmdiag
 echo " COMPAC=-12"                    >> pgsmdiag
 echo " CHAMP('T6',0)"                 >> pgsmdiag

###
 ficheta=`${TASK_BIN}/fgen+ -p ${suite_input}/gridpt.usr/prog/eta  -t $(r.date ${SEQ_SHORT_DATE}) -s$PROG -e$PROG -d 3 -c`
 fichpres=`${TASK_BIN}/fgen+ -p ${suite_input}/gridpt.usr/prog/pres -t $(r.date ${SEQ_SHORT_DATE}) -s$PROG -e$PROG -d 3 -c`
 fichdiag=`${TASK_BIN}/fgen+ -p ${suite_input}/gridpt.usr/prog/diag -t $(r.date ${SEQ_SHORT_DATE}) -s$PROG -e$PROG -d 3 -c`
 $TASK_BIN/pgsm -iment $ficheta  -ozsrt pgsm_out_eta_$PROG -i pgsmeta
 $TASK_BIN/pgsm -iment $fichpres -ozsrt pgsm_out_pres_$PROG -i pgsmpres
 $TASK_BIN/pgsm -iment $fichdiag -ozsrt pgsm_out_diag_$PROG -i pgsmdiag
 
 # to add MX to all forecast time
 if [ $PROG -ne 000 ] ; then
   hh=$PROG  
   ${TASK_BIN}/editfst -s pgsm_out_eta_000 -d pgsm_out_eta_$PROG -i <<EOF
   DESIRE(-1,'MX',-1,-1,-1,-1,-1)
   ZAP(-1,-1,-1,-1,-1,$hh,-1)
EOF
# to change time, NPAS for MX 
# WARNING: The following factor is defined by the number of timesteps per hour, which may change
#          with updates to the RDPS model.
 let npas=12*$hh
 #echo "####### npas=$npas #############"
d.zapdate -i pgsm_out_eta_$PROG -dateo_out $(r.date ${SEQ_SHORT_DATE}) -nomvar MX -npas $npas
fi   
 ############## ADD WT1

 ${TASK_BIN}/editfst -s $fichpres -d tmp_omega2w -i <<EOF
 exclure(-1,'!!')
EOF
 d.omega2w -s tmp_omega2w -d tmpfichpres_${PROG} -toc_kind 2

 cat >> pgsmeta <<EOF
 SORTIE(STD,400,A)
 $grid
 HEURE($PROG)
 SETINTX(LINEAIR)
 CHAMP('WT1',${hgt_wt1_2})
 CHAMP('WT1',${hgt_wt1_1})
 CHAMP('WT1',${hgt_1})
EOF

 $TASK_BIN/pgsm -iment tmpfichpres_${PROG} -ozsrt omega_${PROG}.std  -i pgsmeta
 rm -f tmp_omega2w tmpfichpres_${PROG}

 cat > editfstdir <<EOT
 DESIRE(-1,['WT1'])
 ZAP(-1,['GEOW'],['R1DIAGWT1'])
EOT

 $TASK_BIN/editfst -s omega_${PROG}.std -d omega2_${PROG}.std -i editfstdir
 rm -f omega_${PROG}.std

 ################# END ADD WT1
            
 OUTPUT_FILENAME="${SEQ_SHORT_DATE}_P${PROG}_rdps_10km_DND_extra"

 cat > editdir <<EOF
 exclure(-1,['PN'])
EOF

 $TASK_BIN/editfst -s  pgsm_out_eta_$PROG pgsm_out_pres_$PROG omega2_${PROG}.std pgsm_out_diag_$PROG -d ${OUTPUT_FILENAME}.fstd -i editdir
 rm -f omega2_${PROG}.std









 $TASK_BIN/grib2_encode -i ${OUTPUT_FILENAME}.fstd  \
                        -o ${OUTPUT_FILENAME}.grib2 \
                        -g2dic $TASK_CONFIG/DICT_DND \
                        -g2proc  $TASK_CONFIG/PROCESS \
                        -config $TASK_CONFIG/CONFIG -v






ln -s ${TASK_WORK}/${OUTPUT_FILENAME}.grib2 ${TASK_OUTPUT}/${OUTPUT_FILENAME}.grib2
 #${TASK_BIN}/scp -rv CMC_${model}_${resolution}_${SEQ_SHORT_DATE}_P${PROG}_NCEP.grib2 ${pds_destination}/${model}_grib2  ###pour EC
#dissemination is done in gbdnd job
# c.dissem -c scp_pds -p pdsfile -j ${SEQ_NODE} -d /${model}_grib2 ${OUTPUT_FILENAME}.grib2

done


set +x
ORDSOUMET_date_ended="$(LC_TIME=C date)"
echo DND_extra.+18:6: JOB_ENDED at "$ORDSOUMET_date_ended" WallTime=$(echo  $(date +%s -d "$ORDSOUMET_date_ended") - $JobStartTime | bc)s Name=DND_extra.+18 Dest=eccc-ppp4 CPUS=1 SYSTEM_ID="${JOB_ID:-${LOADL_STEP_ID%.*}}"
