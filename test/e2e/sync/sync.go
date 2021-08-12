// Copyright 2021 IBM Corp
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sync

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/ppc64le-cloud/pvsadm/pkg/utils"
	"github.com/ppc64le-cloud/pvsadm/test/e2e/framework"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type Spec []struct {
	Source `yaml:"source"`
	Target `yaml:"target"`
}

type Source struct {
	Bucket string `yaml:"bucket"`
	Cos    string `yaml:"cos"`
	Object string `yaml:"object"`
	Plan   string `yaml:"plan"`
	Region string `yaml:"region"`
}

type Target []struct {
	Bucket string `yaml:"bucket"`
	Plan   string `yaml:"plan"`
	Region string `yaml:"region"`
}

func runSyncCMD(args ...string) (int, string, string) {
	args = append([]string{"image", "sync"}, args...)
	return utils.RunCMD("pvsadm", args...)
}

// Returns an int >= min, < max
func randomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

func generateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		bytes[i] = byte(randomInt(65, 91))
	}
	fmt.Println(string(bytes))
	return string(bytes)
}

func generateStruct() Spec {

	planSlice := []string{"smart", "standard", "lite"}
	regionSlice := []string{"us-east", "us-west", "jp-tok", "us-south"}
	noOfSources := 2
	noOfTargetsPerSource := 3
	var spec Spec
	spec = make(Spec, noOfSources)

	for src := 0; src < noOfSources; src++ {
		spec[src].Source.Bucket = "Bucket-" + generateRandomString(6)
		spec[src].Source.Cos = "COS-Test-" + generateRandomString(6)
		spec[src].Source.Object = ""
		spec[src].Source.Plan = planSlice[randomInt(0, 3)]
		spec[src].Source.Region = regionSlice[randomInt(0, 3)]
		spec[src].Target = make(Target, noOfTargetsPerSource)

		for tgt := 0; tgt < noOfTargetsPerSource; tgt++ {
			spec[src].Target[tgt].Bucket = "Bucket-" + generateRandomString(6)
			spec[src].Target[tgt].Plan = planSlice[randomInt(0, 3)]
			spec[src].Target[tgt].Region = regionSlice[randomInt(0, 3)]
		}
	}

	return spec

	/*

		fmt.Println("\n\n\n\n\n\n _____________ Called ____________ \n\n\n\n\n\n")
		fmt.Println(spec)
		fmt.Println("\n\n\n\n\n\n _____________ Called ____________ \n\n\n\n\n\n")

		d, err := yaml.Marshal(&spec)
		if err != nil {
			log.Fatalf("error: %v", err)
		}
		fmt.Printf("--- m dump:\n%s\n\n", string(d))

		/*
				var source Source
				var target := make(Target, noOnoOfTargetsPerSource)

				for src:=0; src<noOfSources; src++{
			        source = Source{
						Bucket: "Bucket-" + generateRandomString(6),
						Cos: "COS-Test-" + generateRandomString(6),
						Object: "",
						Plan: planSlice[randomInt(0, 3)],
						Region: regionSlice[randomInt(0, 3)],
					}
					for tgt:=0; target<noOfTargetsPerSource; tgt++{
						target[tgt].Bucket = "Bucket-" + generateRandomString(6),
						target[tgt].Plan = planSlice[randomInt(0, 3)],
						target[tgt].Region = Region: regionSlice[randomInt(0, 3)],
					}
					spec[src].Source = source
					spec[src].Target = target
				}

				fmt.Println("\n\n\n\n\n\n ________Generate_____ Called ____________ \n\n\n\n\n\n", srcRegionSlice, destRegionSlice)
	*/
}

func createResources(spec Spec) bool {
	return true
}

func createRequiredBuckets() bool {
	fmt.Println("\n\n\n\n\n\n _____________ Called ____________ \n\n\n\n\n\n")
	fmt.Println(generateRandomString(10))
	var spec Spec
	spec = generateStruct()
	fmt.Println("________", spec)
	return true
}

var _ = CMDDescribe("pvsadm image sync tests", func() {

	It("run with --help option", func() {
		status, stdout, stderr := runSyncCMD(
			"--help",
		)
		Expect(status).To(Equal(0))
		Expect(stderr).To(Equal(""))
		Expect(stdout).To(ContainSubstring("Examples:"))
	})

	framework.NegativeIt("run without spec-file flag", func() {
		status, _, stderr := runSyncCMD()
		Expect(status).NotTo(Equal(0))
		Expect(stderr).To(ContainSubstring(`"spec-file" not set`))
	})

	framework.NegativeIt("run with yaml file that doesn't exist", func() {
		status, _, stderr := runSyncCMD("--spec-file", "fakefile.yaml")
		Expect(status).NotTo(Equal(0))
		Expect(stderr).To(ContainSubstring(`no such file or directory`))
	})

	Context("Copy Objects between buckets", func() {

		It("has 0 items", func() {})
		// It("has 0 units", func() {})
		_ = createRequiredBuckets()
		Specify("the total amount is 0.00", func() {})
	})
})
