package lightning

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"

	"git.arvados.org/arvados.git/lib/cmd"
	"github.com/mattn/go-isatty"
	"github.com/sirupsen/logrus"
)

var (
	handler = cmd.Multi(map[string]cmd.Handler{
		"version":   cmd.Version,
		"-version":  cmd.Version,
		"--version": cmd.Version,

		"ref2genome":         &ref2genome{},
		"vcf2fasta":          &vcf2fasta{},
		"import":             &importer{},
		"annotate":           &annotatecmd{},
		"export":             &exporter{},
		"export-numpy":       &exportNumpy{},
		"numpy-comvar":       &numpyComVar{},
		"filter":             &filtercmd{},
		"build-docker-image": &buildDockerImage{},
		"pca-go":             &goPCA{},
		"pca-py":             &pythonPCA{},
		"plot":               &pythonPlot{},
		"diff-fasta":         &diffFasta{},
		"stats":              &statscmd{},
		"merge":              &merger{},
		"dumpgob":            &dumpGob{},
	})
)

func Main() {
	if !isatty.IsTerminal(os.Stderr.Fd()) {
		logrus.StandardLogger().Formatter = &logrus.TextFormatter{DisableTimestamp: true}
	}
	os.Exit(handler.RunCommand(os.Args[0], os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

type buildDockerImage struct{}

func (cmd *buildDockerImage) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		fmt.Fprint(stderr, err)
		return 1
	}
	defer os.RemoveAll(tmpdir)
	err = ioutil.WriteFile(tmpdir+"/Dockerfile", []byte(`FROM debian:bullseye
RUN DEBIAN_FRONTEND=noninteractive \
  apt-get update && \
  apt-get dist-upgrade -y && \
  apt-get install -y --no-install-recommends bcftools bedtools samtools python2 python3-sklearn python3-matplotlib ca-certificates && \
  apt-get clean
`), 0644)
	if err != nil {
		fmt.Fprint(stderr, err)
		return 1
	}
	docker := exec.Command("docker", "build", "--tag=lightning-runtime", tmpdir)
	docker.Stdout = stdout
	docker.Stderr = stderr
	err = docker.Run()
	if err != nil {
		return 1
	}
	fmt.Fprintf(stderr, "built and tagged new docker image, lightning-runtime\n")
	return 0
}
