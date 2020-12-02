#!/bin/bash

set -ex

PATH="${GOPATH:-${HOME}/go}/bin:${PATH}"
go install
lightning build-docker-image
arv keep docker lightning-runtime

priority=501
project=su92l-j7d0g-jzei0m9yvgauhjf
ref_fa=su92l-4zz18-u77iyyy7cb05xqv/hg38.fa.gz
ref37_fa=su92l-4zz18-caw3g2ji89jxix8/human_g1k_v37.fasta.gz
gvcf=${HOME}/keep/by_id/su92l-4zz18-bgyq36m6gctk63q
info=su92l-4zz18-ykpcoea5nisz74f
tagset=su92l-4zz18-92bx4zjg5hgs3yc/tagset.fa.gz

genome=$(lightning     ref2genome   -project ${project} -priority ${priority} -ref ${ref_fa})                                                          ; echo genome=${genome}
fasta=$(lightning      vcf2fasta    -project ${project} -priority ${priority} -ref ${ref_fa} -genome ${genome} -mask=true ${gvcf})                     ; echo fasta=${fasta}
# fasta=su92l-4zz18-9nq05jifgz7iult

ref37_lib=$(lightning  import       -project ${project} -priority ${priority} -tag-library ${tagset} -skip-ooo=true -output-tiles=true -save-incomplete-tiles=true ${ref37_fa}) ; echo ref37_lib=${ref37_lib}
# ref37_lib=su92l-4zz18-vnhlv3g6yp1azls/library.gob
# 539s
# ref37_lib=su92l-4zz18-v0xfm2o1tu3u1w3/library.gob.gz
# 2751s @ 4a3899f

ref38_lib=$(lightning  import       -project ${project} -priority ${priority} -tag-library ${tagset} -skip-ooo=true -output-tiles=true -save-incomplete-tiles=true ${ref_fa}) ; echo ref38_lib=${ref38_lib}
# ref38_lib=su92l-4zz18-swebknshfwsvys6/library.gob

bed37=$(lightning export       -project ${project} -priority ${priority} -i ${ref37_lib} -output-format hgvs -ref /mnt/$ref37_fa -output-bed hg37.bed) ; echo bed37=${bed37}
# bed37=su92l-4zz18-gb3hihiiaz0xaz9/export.csv
# 463s @ 870319f
bed38=$(lightning export       -project ${project} -priority ${priority} -i ${ref38_lib} -output-format hgvs -ref /mnt/$ref_fa -output-bed hg38.bed) ; echo bed38=${bed38}

unfiltered=$(lightning import       -project ${project} -priority ${priority} -tag-library ${tagset} -skip-ooo=true -output-tiles=true ${fasta})       ; echo unfiltered=${unfiltered}
# unfiltered=su92l-4zz18-mz3546bib6oj1gg/library.gob
# unfiltered=su92l-4zz18-72ovi5qrderxudv/library.gob
# 24674s @ pre-38e6e7c
# unfiltered=su92l-4zz18-ywhkc1hgdzxwp5u/library.gob
# 18497s @ 64vcpu bf0968a
# _____s @ 32vcpu 83983ad


merged=$(lightning     merge        -project ${project} -priority ${priority} ${unfiltered} ${ref37_lib})                                              ; echo merged=${merged}
# merged=su92l-4zz18-svw5xqe5g0ct2v1/library.gob
# 2400s

exportvcf=$(lightning  export       -project ${project} -priority ${priority} -i ${merged} -output-format vcf -ref /mnt/su92l-4zz18-caw3g2ji89jxix8/human_g1k_v37.fasta.gz -output-bed export.bed) ; echo exportvcf=${exportvcf}
# exportvcf=su92l-4zz18-gz4svr6zyvipueu/export.csv
# 5506s

exporthgvs=$(lightning export       -project ${project} -priority ${priority} -i ${merged38} -output-format hgvs -ref /mnt/su92l-4zz18-u77iyyy7cb05xqv/hg38.fa.gz -output-bed hg38.bed) ; echo exporthgvs=${exporthgvs}
# 
# 

stats=$(lightning      stats        -project ${project} -priority ${priority} -i ${merged})                                                            ; echo stats=${stats}

filtered=$(lightning   filter       -project ${project} -priority ${priority} -i ${merged} -min-coverage "0.9" -max-variants "30")                     ; echo filtered=${filtered}

annotations=$(lightning annotate    -project ${project} -priority ${priority} -i ${merged})                                                            ; echo annotations=${annotations}

pca=$(lightning        pca-go       -project ${project} -priority ${priority} -i ${unfiltered} -min-coverage "0.9" -max-variants "30")                 ; echo pca=${pca}
# pca=su92l-4zz18-e3xhi2mzp8rqevd/pca.npy
# 3987s @ c237c16
plot=$(lightning       plot         -project ${project} -priority ${priority} -i ${pca} -labels-csv ${info}/sample_info.csv -sample-fasta-dir ${fasta})
echo >&2 "https://workbench2.${plot%%-*}.arvadosapi.com/collections/${plot}"
echo ${plot%%/*}
# plot=su92l-4zz18-xyei3lnyxmgo7lh/plot.png
# 535s @ c237c16

merged38=$(lightning   merge        -project ${project} -priority ${priority} ${unfiltered} ${ref38_lib})                                              ; echo merged38=${merged38}
# merged38=su92l-4zz18-xq17gtaltjxbm3n/library.gob
# 1602s
# merged38=su92l-4zz18-5kcaci3hqzukjv2/library.gob
# 2815s @ 83983ad
# merged38=su92l-4zz18-nq8dmtng68ozovu/library.gob.gz
# 9803s @ 69b71af

numpy=$(lightning      export-numpy -project ${project} -priority ${priority} -i ${merged38})                                                          ; echo numpy=${numpy}
# numpy=su92l-4zz18-w3dx5k79mtbz6qt/matrix.npy
# 6155s
# numpy=su92l-4zz18-g1y2eg9qvngvkkq/matrix.npy
# 6633s @ 83983ad
# numpy=su92l-4zz18-cpw0i3z7wery77o/matrix.npy
# 6311s @ 4e6ada0
# numpy=su92l-4zz18-hljgbqs6c87wles/matrix.npy
# 6824s @ 2e1cb2e
# numpy=su92l-4zz18-vw31l0qzenyb44l/matrix.npy
# 7403s @ 6785271
# pcapy=$(lightning      pca          -project ${project} -priority ${priority} -i ${numpy})                                                             ; echo pcapy=${pcapy}
comvar=$(lightning     numpy-comvar -project ${project} -priority ${priority} -i ${numpy} -annotations ${numpy%/matrix.npy}/annotations.tsv)           ; echo comvar=${comvar}
# comvar=su92l-4zz18-s1yhngobdvcoc2e/commonvariants.csv
