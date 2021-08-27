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
	"errors"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/IBM-Cloud/bluemix-go/api/resource/resourcev1/management"
	"github.com/IBM-Cloud/bluemix-go/api/resource/resourcev2/controllerv2"
	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
	"github.com/ppc64le-cloud/pvsadm/pkg"
	"github.com/ppc64le-cloud/pvsadm/pkg/client"
	"github.com/ppc64le-cloud/pvsadm/pkg/utils"
	"github.com/ppc64le-cloud/pvsadm/test/e2e/framework"
	"gopkg.in/yaml.v2"
	"k8s.io/klog/v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// Test case variables
var (
	APIKey            = os.Getenv("IBMCLOUD_API_KEY")
	ObjectsFolderName = "tempFolder"
	PlanSlice         = []string{"smart", "standard", "vault", "cold"}
	RegionSlice       = []string{"us-east", "jp-tok", "us-south", "au-syd", "eu-de", "ca-tor"}
	SpecFileName      = "spec/spec.yaml"
)

// Test case constants
const (
	ServiceType            = "cloud-object-storage"
	ResourceGroupAPIRegion = "global"
	ServicePlan            = "standard"
	Debug                  = false
	Recursive              = false
	InstanceType           = "service_instance"
	NoOfSources            = 2
	NoOfTargetsPerSource   = 2
	NoOfObjects            = 200
	NoOfUploadWorkers      = 20
)

// Run sync command
func runSyncCMD(args ...string) (int, string, string) {
	args = append([]string{"image", "sync"}, args...)
	return utils.RunCMD("pvsadm", args...)
}

// Generate random int between min and max
func randomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

// Generate random string of given length
func generateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		bytes[i] = byte(randomInt(97, 122))
	}
	return string(bytes)
}

// Generate Specifications
func generateSpec() pkg.Spec {
	klog.Infoln("STEP: Generating Spec")
	var spec pkg.Spec
	spec.Source = pkg.Source{
		Bucket:       "image-sync-" + generateRandomString(6),
		Cos:          "cos-image-sync-test-" + generateRandomString(6),
		Object:       "",
		StorageClass: PlanSlice[randomInt(0, len(PlanSlice))],
		Region:       RegionSlice[randomInt(0, len(RegionSlice))],
	}

	spec.Target = make([]pkg.TargetItem, 0)
	for tgt := 0; tgt < NoOfTargetsPerSource; tgt++ {
		spec.Target = append(spec.Target, pkg.TargetItem{
			Bucket:       "image-sync-" + generateRandomString(6),
			StorageClass: PlanSlice[randomInt(0, len(PlanSlice))],
			Region:       RegionSlice[randomInt(0, len(RegionSlice))],
		})
	}

	return spec
}

// Create Specifications yaml file
func createSpecFile(spec []pkg.Spec) error {
	klog.Infof("STEP: Creating Spec file")
	dir, err := ioutil.TempDir(".", "spec")
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	file, err := ioutil.TempFile(dir, "spec.*.yaml")
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}
	defer file.Close()

	SpecFileName = file.Name()
	specString, merr := yaml.Marshal(&spec)
	if merr != nil {
		klog.Errorf("ERROR: %v", merr)
		return merr
	}

	_, err = file.WriteString(string(specString))
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	klog.Infoln(string(specString))
	return nil
}

// Create Cloud Object Storage Service instance
func createCOSInstance(instanceName string) error {
	klog.Infoln("STEP: Creating COS instance :", instanceName)
	bxCli, err := client.NewClientWithEnv(APIKey, client.DefaultEnv, Debug)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	resourceGroupQuery := management.ResourceGroupQuery{
		AccountID: bxCli.User.Account,
	}
	resGrpList, err := bxCli.ResGroupAPI.List(&resourceGroupQuery)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	var resourceGroupNames []string
	for _, resgrp := range resGrpList {
		resourceGroupNames = append(resourceGroupNames, resgrp.Name)
	}
	klog.Infoln("Resource Group names: ", resourceGroupNames)

	_, err = bxCli.CreateServiceInstance(instanceName, ServiceType, ServicePlan,
		resourceGroupNames[0], ResourceGroupAPIRegion)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	return nil
}

// Delete Cloud Object Storage Service instance
func deleteCOSInstance(instanceName string) error {
	klog.Infoln("STEP: Deleting COS instance", instanceName)
	bxCli, err := client.NewClientWithEnv(APIKey, client.DefaultEnv, Debug)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	svcs, err := bxCli.ResourceClientV2.ListInstances(controllerv2.ServiceInstanceQuery{
		Type: InstanceType,
		Name: instanceName,
	})
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	for _, svc := range svcs {
		if svc.Name == instanceName {
			err = bxCli.DeleteServiceInstance(svc.ID, Recursive)
			if err != nil {
				klog.Errorf("ERROR: %v", err)
				return err
			}
			klog.Infoln("Service Instance Deleted: ", svc.Name)
		}
	}

	return nil
}

// Create S3 bucket in the given region and storage class
func createBucket(bucketName string, cos string, region string, storageClass string) error {
	klog.Infof("STEP: Creating Bucket %s in region %s in COS %s storageClass %s", bucketName, region, cos, storageClass)
	bxCli, err := client.NewClientWithEnv(APIKey, client.DefaultEnv, Debug)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	s3Cli, err := client.NewS3Client(bxCli, cos, region)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	_, err = s3Cli.S3Session.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(region + "-" + storageClass),
		},
	})
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	err = s3Cli.S3Session.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	return nil
}

// Create Local object files
func createObjects() error {
	klog.Infoln("STEP: Create Required Files")
	var content string
	dir, err := ioutil.TempDir(".", "objects")
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	ObjectsFolderName = dir
	for i := 0; i < NoOfObjects; i++ {
		file, err := ioutil.TempFile(ObjectsFolderName, "image-sync-*.txt")
		if err != nil {
			klog.Errorf("ERROR: %v", err)
			return err
		}
		defer file.Close()

		content = generateRandomString(200)
		_, err = file.WriteString(content)
		if err != nil {
			klog.Errorf("ERROR: %v", err)
			return err
		}
	}

	return nil
}

// Delete Temporarily created local object files and spec file
func deleteTempFiles() error {
	klog.Infoln("STEP: Delete created Files")
	specFolder := strings.Split(SpecFileName, "/")[0]
	klog.Infoln("Spec folder : ", specFolder)

	err := os.RemoveAll(specFolder)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
	}

	err = os.RemoveAll(ObjectsFolderName)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
	}

	return nil
}

// upload worker
func uploadWorker(s3Cli *client.S3Client, bucketName string, workerId int, filepaths <-chan string, results chan<- bool) {
	for filepath := range filepaths {
		fileName := strings.Split(filepath, "/")[len(strings.Split(filepath, "/"))-1]
		err := s3Cli.UploadObject(filepath, fileName, bucketName)
		if err != nil {
			klog.Errorf("ERROR: %v, File %s upload failed", err, filepath)
			results <- false
		}
		results <- true
	}
}

// Upload object from local dir to s3 bucket
func uploadObjects(src pkg.Source) error {
	klog.Infoln("STEP: Upload Objects to source Bucket ", src.Bucket)
	var filePath string
	files, err := ioutil.ReadDir(ObjectsFolderName)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	bxCli, err := client.NewClientWithEnv(APIKey, client.DefaultEnv, Debug)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	s3Cli, err := client.NewS3Client(bxCli, src.Cos, src.Region)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	filepaths := make(chan string, len(files))
	results := make(chan bool, len(files))

	for w := 1; w <= NoOfUploadWorkers; w++ {
		go uploadWorker(s3Cli, src.Bucket, w, filepaths, results)
	}

	for _, f := range files {
		filePath = ObjectsFolderName + "/" + f.Name()
		filepaths <- filePath
	}
	close(filepaths)

	for i := 1; i <= len(files); i++ {
		if !<-results {
			return errors.New("FAIL: Upload Objects failed")
		}
	}

	return nil
}

// Verify the copied Objects exists in the target bucket
func verifyBucketObjects(tgt pkg.TargetItem, cos string, files []fs.FileInfo, regex string) error {
	klog.Infoln("STEP: Verify objects in Bucket ", tgt.Bucket)
	bxCli, err := client.NewClientWithEnv(APIKey, client.DefaultEnv, Debug)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	s3Cli, err := client.NewS3Client(bxCli, cos, tgt.Region)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	objects, err := s3Cli.SelectObjects(tgt.Bucket, regex)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	for _, f := range files {
		fileName := f.Name()
		res := false
		klog.Infoln("Verifying object", fileName)

		for _, item := range objects {
			if item == fileName {
				res = true
				break
			}
		}
		if !res {
			klog.Errorf("ERROR: Object %s not found in the bucket %s", fileName, tgt.Bucket)
			return errors.New("ERROR: Object not found in the bucket ")
		}
	}

	return nil
}

// Verify objects copied from source bucket to dest buckets
func verifyObjectsCopied(spec []pkg.Spec) error {
	klog.Infoln("STEP: Verify Objects Copied to dest buckets")
	files, err := ioutil.ReadDir(ObjectsFolderName)
	if err != nil {
		klog.Errorf("ERROR: %v", err)
		return err
	}

	for _, src := range spec {
		for _, tgt := range src.Target {
			err = verifyBucketObjects(tgt, src.Cos, files, src.Object)
			if err != nil {
				klog.Errorf("ERROR: %v", err)
				return err
			}
		}
	}

	return nil
}

// Create necessary resources to run the sync command
func createResources(spec []pkg.Spec) error {
	klog.Infoln("STEP: Create resources")
	err := createSpecFile(spec)
	if err != nil {
		return err
	}

	err = createObjects()
	if err != nil {
		return err
	}

	for _, src := range spec {
		err = createCOSInstance(src.Cos)
		if err != nil {
			return err
		}

		err = createBucket(src.Bucket, src.Cos, src.Region, src.StorageClass)
		if err != nil {
			return err
		}

		err = uploadObjects(src.Source)
		if err != nil {
			return err
		}

		for _, tgt := range src.Target {
			err = createBucket(tgt.Bucket, src.Cos, tgt.Region, tgt.StorageClass)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Delete the resources
func deleteResources(spec []pkg.Spec) error {
	klog.Infoln("STEP: Delete resources")
	for _, src := range spec {
		err := deleteCOSInstance(src.Cos)
		if err != nil {
			return err
		}
	}

	err := deleteTempFiles()
	if err != nil {
		return err
	}

	return nil
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
		specSlice := make([]pkg.Spec, 0)
		for i := 0; i < NoOfSources; i++ {
			specSlice = append(specSlice, generateSpec())
		}

		err := createResources(specSlice)
		Expect(err).NotTo(HaveOccurred())
		defer deleteResources(specSlice)

		status, _, _ := runSyncCMD("--spec-file", SpecFileName)
		Expect(status).To(Equal(0))

		err = verifyObjectsCopied(specSlice)
		Expect(err).NotTo(HaveOccurred())
	})

})
