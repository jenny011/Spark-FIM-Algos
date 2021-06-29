'''
获取 template string:
1. 
with open(可用的slurm脚本 ,'r') as f:
	print(f.readlines())
2.
复制stdout里的list至此脚本
删除所有', '等多余字符
3.
替换所有需要自动化的参数
'''

for d in ["retail", "kosarak", "chainstore", "record"]:
    for c in [0,20000,40000,60000,80000,100000]:
        for p in [4,8,12]:
            for n in range(7):
                template = f"""#!/bin/bash\n#SBATCH -p parallel\n#SBATCH --ntasks-per-node={p+1}\n#SBATCH --nodes 1\n#SBATCH -t 2-00:00\n#SBATCH --mem=128GB\n#SBATCH --job-name freno_{d}_{c}_{p}_{n}\n#SBATCH --array=51,41,31,21,11,1\n#SBATCH --output=/gpfsnyu/home/yfw215/PFP/exp_output/{d}/perf/%a_{c}_{p}/{n}.txt\n#SBATCH --constraint=g6248\n\nmodule purge\nmodule load anaconda3/5.2.0\nmodule load java/1.8.0_131\nmodule load spark\n\n\nBASE="/gpfsnyu/home/jz2915/PFP"\n\nipnip=$(hostname -i)\necho -e "  ssh -N -L 4040:$ipnip:4040 $USER@hpc.shanghai.nyu.edu\\n"\n\nNUM={n}\nPARTITION={p}\nMINSUP=$SLURM_ARRAY_TASK_ID\nINTERVAL={c}\nDATABASE={d}\n\nDBDIR="$BASE/exp_output/$DATABASE"\nif [ ! -d $DBDIR ]; then\n\tmkdir $DBDIR\nfi\n\nRESULTDIR="$DBDIR/result"\nif [ ! -d $RESULTDIR ]; then\n\tmkdir $RESULTDIR\nfi\n\nOUTFILE="$RESULTDIR/$MINSUP"_"$INTERVAL"_"$PARTITION.out"\n\nspark-submit --num-executors $PARTITION \\\n\t--executor-cores 1 \\\n\t--executor-memory 124G \\\n\t--py-files Archive.zip \\\n\t--conf spark.executorEnv.PYTHONHASHSEED=321 \\\n\t--driver-memory 61g \\\n\t--conf spark.rpc.message.maxSize=1024 \\\n\t--conf spark.driver.maxResultSize=0 \\\n\t--conf spark.default.parallelism=$PARTITION \\\n\trun.py -d $DATABASE -m $MINSUP -p $PARTITION -i $INTERVAL > $OUTFILE\n"""

                with open(f"freno_slurms/freno_{d}_{c}_{p}_{n}.slurm", 'w') as f:
                    f.write(template)
