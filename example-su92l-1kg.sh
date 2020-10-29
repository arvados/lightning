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

ref37_lib=$(lightning  import       -project ${project} -priority ${priority} -tag-library ${tagset} -skip-ooo=true -output-tiles=true -include-no-calls ${ref37_fa}) ; echo ref37_lib=${ref37_lib}
# ref37_lib=su92l-4zz18-vnhlv3g6yp1azls/library.gob
# 539s

ref38_lib=$(lightning  import       -project ${project} -priority ${priority} -tag-library ${tagset} -skip-ooo=true -output-tiles=true -include-no-calls ${ref_fa}) ; echo ref38_lib=${ref38_lib}

unfiltered=$(lightning import       -project ${project} -priority ${priority} -tag-library ${tagset} -skip-ooo=true -output-tiles=true ${fasta})       ; echo unfiltered=${unfiltered}
# unfiltered=su92l-4zz18-mz3546bib6oj1gg/library.gob

merged=$(lightning     merge        -project ${project} -priority ${priority} ${unfiltered} ${ref37_lib})                                              ; echo merged=${merged}
# merged=su92l-4zz18-svw5xqe5g0ct2v1/library.gob
# 2400s

exportvcf=$(lightning  export       -project ${project} -priority ${priority} -i ${merged} -output-format vcf -ref /mnt/su92l-4zz18-caw3g2ji89jxix8/human_g1k_v37.fasta.gz -output-bed export.bed) ; echo exportvcf=${exportvcf}
# exportvcf=su92l-4zz18-gz4svr6zyvipueu/export.csv
# 5506s

stats=$(lightning      stats        -project ${project} -priority ${priority} -i ${merged})                                                            ; echo stats=${stats}

filtered=$(lightning   filter       -project ${project} -priority ${priority} -i ${merged} -min-coverage "0.9" -max-variants "30")                     ; echo filtered=${filtered}

annotations=$(lightning annotate    -project ${project} -priority ${priority} -i ${merged})                                                            ; echo annotations=${annotations}

pca=$(lightning        pca-go       -project ${project} -priority ${priority} -i ${filtered} -one-hot)                                                 ; echo pca=${pca}
plot=$(lightning       plot         -project ${project} -priority ${priority} -i ${pca} -labels-csv ${info}/sample_info.csv -sample-fasta-dir ${fasta})
echo >&2 "https://workbench2.${plot%%-*}.arvadosapi.com/collections/${plot}"
echo ${plot%%/*}

numpy=$(lightning      export-numpy -project ${project} -priority ${priority} -i ${filtered} -one-hot)
