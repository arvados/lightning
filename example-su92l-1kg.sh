#!/bin/bash

set -ex

PATH="${GOPATH:-${HOME}/go}/bin:${PATH}"
go install
lightning build-docker-image
arv keep docker lightning-runtime

priority=501
project=su92l-j7d0g-jzei0m9yvgauhjf
ref_fa=su92l-4zz18-u77iyyy7cb05xqv/hg38.fa.gz
gvcf=${HOME}/keep/by_id/su92l-4zz18-bgyq36m6gctk63q
info=su92l-4zz18-ykpcoea5nisz74f
tagset=su92l-4zz18-92bx4zjg5hgs3yc/tagset.fa.gz

genome=$(lightning     ref2genome   -project ${project} -priority ${priority} -ref ${ref_fa})
fasta=$(lightning      vcf2fasta    -project ${project} -priority ${priority} -ref ${ref_fa} -genome ${genome} -mask=true ${gvcf})
unfiltered=$(lightning import       -project ${project} -priority ${priority} -tag-library ${tagset} -skip-ooo=true ${fasta})
filtered=$(lightning   filter       -project ${project} -priority ${priority} -i ${unfiltered} -min-coverage "0.9" -max-variants "30")
numpy=$(lightning      export-numpy -project ${project} -priority ${priority} -i ${filtered})
pca=$(lightning        pca          -project ${project} -priority ${priority} -i ${numpy})
plot=$(lightning       plot         -project ${project} -priority ${priority} -i ${pca} -labels-csv ${info}/sample_info.csv -sample-fasta-dir ${fasta})
echo >&2 "https://workbench2.${plot%%-*}.arvadosapi.com/collections/${plot}"
echo ${plot%%/*}
