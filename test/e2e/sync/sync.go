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
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/IBM-Cloud/bluemix-go/api/resource/resourcev1/management"
	"github.com/IBM-Cloud/bluemix-go/api/resource/resourcev2/controllerv2"
	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
	"github.com/ppc64le-cloud/pvsadm/pkg/client"
	"github.com/ppc64le-cloud/pvsadm/pkg/utils"
	"github.com/ppc64le-cloud/pvsadm/test/e2e/framework"
	"gopkg.in/yaml.v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	APIKey      = string(os.Getenv("IBMCLOUD_API_KEY"))
	FolderName  = "folder" + strings.ToLower(generateRandomString(6))
	PlanSlice   = []string{"smart", "standard", "vault", "cold"}
	RegionSlice = []string{"us-east", "jp-tok", "us-south"}
)

const (
	ServiceType            = "cloud-object-storage"
	ResourceGroupAPIRegion = "global"
	ServicePlan            = "standard"
	ResourceGrp            = "powervs-ipi-resource-group"
	Debug                  = false
	Recursive              = false
	InstanceType           = "service_instance"
	NoOfSources            = 2
	NoOfTargetsPerSource   = 2
	NoOfObjects            = 5
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

func randomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

func generateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		bytes[i] = byte(randomInt(65, 91))
	}
	return string(bytes)
}

func generateStruct() Spec {

	fmt.Println("STEP: Generating Struct")
	spec := make(Spec, NoOfSources)

	for src := 0; src < NoOfSources; src++ {
		spec[src].Source.Bucket = "bucket-" + strings.ToLower(generateRandomString(6))
		spec[src].Source.Cos = "COS-Test-" + generateRandomString(6)
		spec[src].Source.Object = ""
		spec[src].Source.Plan = PlanSlice[randomInt(0, len(PlanSlice))]
		spec[src].Source.Region = RegionSlice[randomInt(0, len(RegionSlice))]
		spec[src].Target = make(Target, NoOfTargetsPerSource)

		for tgt := 0; tgt < NoOfTargetsPerSource; tgt++ {
			spec[src].Target[tgt].Bucket = "bucket-" + strings.ToLower(generateRandomString(6))
			spec[src].Target[tgt].Plan = PlanSlice[randomInt(0, len(PlanSlice))]
			spec[src].Target[tgt].Region = RegionSlice[randomInt(0, len(RegionSlice))]
		}
	}
	return spec
}

func createSpecFile(spec Spec) bool {

	fmt.Println("STEP: Creating Spec file")

	f, err := os.Create("spec.yaml")
	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}
	defer f.Close()

	specString, merr := yaml.Marshal(&spec)
	if merr != nil {
		fmt.Println("ERROR: ", merr)
		return false
	}
	_, err2 := f.WriteString(string(specString))
	if err2 != nil {
		fmt.Println("ERROR: ", err2)
		return false
	}

	fmt.Println(string(specString))
	return true
}

func createCOSInstances(spec Spec) bool {
	fmt.Println("STEP: Creating COS instances")

	bxCli, err := client.NewClientWithEnv(APIKey, client.DefaultEnv, Debug)
	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}

	resourceGroupQuery := management.ResourceGroupQuery{
		AccountID: bxCli.User.Account,
	}

	resGrpList, err := bxCli.ResGroupAPI.List(&resourceGroupQuery)
	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}

	var resourceGroupNames []string
	for _, resgrp := range resGrpList {
		resourceGroupNames = append(resourceGroupNames, resgrp.Name)
	}

	for _, src := range spec {
		fmt.Println("STEP: Creating COS instance", src.Cos)
		_, err = bxCli.CreateServiceInstance(src.Cos, ServiceType, ServicePlan,
			ResourceGrp, ResourceGroupAPIRegion)
		if err != nil {
			fmt.Println("ERROR: ", err)
			return false
		}
	}

	return true

}

func deleteCOSInstances(spec Spec) bool {

	fmt.Println("STEP: Deleting COS instances")
	bxCli, err := client.NewClientWithEnv(APIKey, client.DefaultEnv, Debug)
	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}

	for _, src := range spec {
		fmt.Println("STEP: Deleting COS instance", src.Cos)

		svcs, err := bxCli.ResourceClientV2.ListInstances(controllerv2.ServiceInstanceQuery{
			Type: InstanceType,
			Name: src.Cos,
		})

		if err != nil {
			fmt.Println("ERROR: ", err)
			return false
		}

		for _, svc := range svcs {
			if svc.Name == src.Cos {
				err = bxCli.DeleteServiceInstance(svc.ID, Recursive)
				if err != nil {
					fmt.Println("ERROR: ", err)
					return false
				}
				fmt.Println("Service Instance Deleted: ", svc.Name)
			}
		}

	}

	return true
}

func createBuckets(spec Spec) bool {

	fmt.Println("STEP: Create Required Buckets")
	bxCli, err := client.NewClientWithEnv(APIKey, client.DefaultEnv, Debug)
	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}

	for _, src := range spec {
		s3Cli, err := client.NewS3Client(bxCli, src.Cos, src.Region)
		if err != nil {
			fmt.Println("ERROR: ", err)
			return false
		}

		fmt.Println("STEP: Create Required Bucket", src.Bucket)
		_, err = s3Cli.S3Session.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(src.Bucket),
			CreateBucketConfiguration: &s3.CreateBucketConfiguration{
				LocationConstraint: aws.String(src.Region + "-" + src.Plan),
			},
		})
		if err != nil {
			fmt.Println("ERROR: ", err)
			return false
		}

		err = s3Cli.S3Session.WaitUntilBucketExists(&s3.HeadBucketInput{
			Bucket: aws.String(src.Bucket),
		})

		if err != nil {
			fmt.Println("ERROR: ", err)
			return false
		}

		for _, tgt := range src.Target {
			s3Cli, err := client.NewS3Client(bxCli, src.Cos, tgt.Region)
			if err != nil {
				fmt.Println("ERROR: ", err)
				return false
			}

			fmt.Println("STEP: Create Required Bucket", tgt.Bucket)
			_, err = s3Cli.S3Session.CreateBucket(&s3.CreateBucketInput{
				Bucket: aws.String(tgt.Bucket),
				CreateBucketConfiguration: &s3.CreateBucketConfiguration{
					LocationConstraint: aws.String(tgt.Region + "-" + tgt.Plan),
				},
			})

			if err != nil {
				fmt.Println("ERROR: ", err)
				return false
			}

			err = s3Cli.S3Session.WaitUntilBucketExists(&s3.HeadBucketInput{
				Bucket: aws.String(tgt.Bucket),
			})

			if err != nil {
				fmt.Println("ERROR: ", err)
				return false
			}
		}

	}

	return true
}

func createFiles() bool {

	fmt.Println("STEP: Create Required Files")
	var (
		content  string
		fileName string
	)

	err := os.Mkdir(FolderName, 0777)

	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}

	for i := 0; i < NoOfObjects; i++ {
		content = generateRandomString(100)
		fileName = FolderName + "/" + strings.ToLower(generateRandomString(6)) + ".txt"
		f, err := os.Create(fileName)

		if err != nil {
			fmt.Println("ERROR: ", err)
			return false
		}
		defer f.Close()

		_, err = f.WriteString(content)
		if err != nil {
			fmt.Println("ERROR: ", err)
			return false
		}
	}
	return true
}

func deleteFiles() bool {
	fmt.Println("STEP: Delete created Files")
	err := os.RemoveAll(FolderName)
	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}
	err = os.RemoveAll("spec.yaml")
	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}
	return true
}

func uploadObjects(spec Spec) bool {

	fmt.Println("STEP: UploaOjects to Buckets")
	var filePath string
	files, err := ioutil.ReadDir(FolderName)
	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}

	bxCli, err := client.NewClientWithEnv(APIKey, client.DefaultEnv, Debug)

	if err != nil {
		fmt.Println("ERROR: ", err)
		return false
	}

	for _, src := range spec {

		s3Cli, err := client.NewS3Client(bxCli, src.Cos, src.Region)
		if err != nil {
			fmt.Println("ERROR: ", err)
			return false
		}
		for _, f := range files {
			filePath = FolderName + "/" + f.Name()
			fmt.Println("STEP: Uploading File ", filePath, " To Bucket ", src.Bucket)
			err = s3Cli.UploadObject(filePath, f.Name(), src.Bucket)
			if err != nil {
				fmt.Println("ERROR: ", err)
				return false
			}
		}
	}

	return true

}

func createResources(spec Spec) bool {

	if !createSpecFile(spec) {
		return false
	}

	if !createCOSInstances(spec) {
		return false
	}

	if !createBuckets(spec) {
		return false
	}

	if !createFiles() {
		return false
	}

	if !uploadObjects(spec) {
		return false
	}

	return true
}

func deleteResources(spec Spec) bool {
	if !deleteCOSInstances(spec) {
		return false
	}

	if !deleteFiles() {
		return false
	}
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

	It("Copy Object Between Buckets", func() {
		spec := generateStruct()
		res := createResources(spec)
		Expect(res).To(BeTrue())
		status, _, _ := runSyncCMD("--spec-file", "spec.yaml")
		Expect(status).To(Equal(0))
		res = deleteResources(spec)
		Expect(res).To(BeTrue())
	})

})
