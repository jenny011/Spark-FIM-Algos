# 自动化sbatch脚本
## 将下列文件或文件夹放入相应的位置
<ul>
<li>gen_slurms</li>
<li>batch.sh</li>
<li>gendir.sh</li>
</ul>

## 第一步 本地 gen_slurms
建freno_slurms/文件夹 <br>
`$ python gen.py` 生成所有参数组合脚本

## 第二步 上传 freno_slurms/ 至hpc的freno目录

## 第三步 hpc的freno目录

### 相关文件夹结构如下
<ol>
<li>7次实验的slurm脚本分别放入7个文件夹：slurms/slurms[0-6]</li>
<li>实验结果输出目录：exp_output</li>
<li>频繁项集输出目录：exp_output/[db]/result/[minsup]_[chunkSize]_[partition].out</li>
<li>速度结果输出目录：exp_output/[db]/perf/[minsup]_[chunkSize]_[partition]/[0-6].txt</li>
<li>自动sbatch的脚本：batch.sh</li>
</ol>

### 相关操作如下
<ol>
<li>`mv "slurms/freno_*_*_*_0.slurm" slurms/slurms0`</li>
<li>生成exp_output/[db]：`for d in retail ...; do mkdir exp_output/$d; done`</li>
<li>生成exp_output/[db]/perf; result：`mkdir exp_output/*/perf; mkdir exp_output/*/result`</li>
<li>生成exp_output/[db]/perf的子目录：在exp_output里跑`$ bash gendir.sh`</li>
<li>自动sbatch: `$ bash batch.sh [0-6]`</li>
</ol>
batch.sh修改后可以不同的时间间隔自动提交任意文件夹内的slurm文件<br>
<b>一次性提交的任务可能很多，遇到问题请调整slurms文件夹和batch.sh</b>
