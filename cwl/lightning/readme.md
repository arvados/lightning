Running tiling workflow
===

Command
---

arvados-cwl-runner --submit --no-wait --project-uuid <project_uuid> fasta2numpy-wf.cwl <input_yml>

For examples of input yml files, see yml/fasta2numpy-wf-100test.yml and yml/fasta2numpy-wf-0831_0315.yml

Notable parameters for input yml
---

fastadirs: an array of fasta directories, in our implementation, each directory consists of around 100 fasta pairs
batchsize: an integer determining the batch size when running lighting-import step, e.g., for batchsize 12, we run lightning-import for 12 fasta directories together as a batch, the resulting libraries then get merged by lightning-slice
matchgenome: a string pattern used for obtaining a subset of the cohort, e.g, matchgenome "ADNI|WCAP" runs tiling for all samples with "ADNI" or "WCAP" in their name, matchgenome "" runs for the entire cohort
trainingsetsize: a float between 0 and 1 to determine the training set size
