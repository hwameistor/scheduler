DOCKER_REGISTRY ?= hwameistor.io/scheduler
RELEASE_DOCKER_REGISTRY ?= hwameistor.io/hwameistor

GO_VERSION = $(shell go version)
BUILD_TIME = ${shell date +%Y-%m-%dT%H:%M:%SZ}
BUILD_VERSION = ${shell git rev-parse --short "HEAD^{commit}" 2>/dev/null}
BUILD_ENVS = CGO_ENABLED=0 GOOS=linux
BUILD_FLAGS = -X 'main.BUILDVERSION=${BUILD_VERSION}' -X 'main.BUILDTIME=${BUILD_TIME}' -X 'main.GOVERSION=${GO_VERSION}'
BUILD_OPTIONS = -a -mod vendor -installsuffix cgo -ldflags "${BUILD_FLAGS}"

PROJECT_SOURCE_CODE_DIR ?= $(CURDIR)
BINS_DIR = ${PROJECT_SOURCE_CODE_DIR}/_build
CMDS_DIR = ${PROJECT_SOURCE_CODE_DIR}/cmd
IMAGES_DIR = ${PROJECT_SOURCE_CODE_DIR}/images

BUILD_CMD = go build
RUN_CMD = go run
K8S_CMD = kubectl

BUILDER_NAME = hwameistor/scheduler-builder
BUILDER_TAG = latest
BUILDER_MOUNT_SRC_DIR = ${PROJECT_SOURCE_CODE_DIR}/../
BUILDER_MOUNT_DST_DIR = /go/src/github.com/hwameistor
BUILDER_WORKDIR = /go/src/github.com/hwameistor/local-storage

DOCKER_SOCK_PATH=/var/run/docker.sock
DOCKER_MAKE_CMD = docker run --rm -v ${BUILDER_MOUNT_SRC_DIR}:${BUILDER_MOUNT_DST_DIR} -v ${DOCKER_SOCK_PATH}:${DOCKER_SOCK_PATH} -w ${BUILDER_WORKDIR} -i ${BUILDER_NAME}:${BUILDER_TAG}
DOCKER_DEBUG_CMD = docker run --rm -v ${BUILDER_MOUNT_SRC_DIR}:${BUILDER_MOUNT_DST_DIR} -v ${DOCKER_SOCK_PATH}:${DOCKER_SOCK_PATH} -w ${BUILDER_WORKDIR} -it ${BUILDER_NAME}:${BUILDER_TAG}
DOCKER_BUILDX_CMD_AMD64 = DOCKER_CLI_EXPERIMENTAL=enabled docker buildx build --platform=linux/amd64 -o type=docker
DOCKER_BUILDX_CMD_ARM64 = DOCKER_CLI_EXPERIMENTAL=enabled docker buildx build --platform=linux/arm64 -o type=docker
MUILT_ARCH_PUSH_CMD = ${PROJECT_SOURCE_CODE_DIR}/build/utils/docker-push-with-multi-arch.sh

CLUSTER_CRD_DIR = ${PROJECT_SOURCE_CODE_DIR}/deploy/crds

# image_tag/release_tag will be applied to all the images
IMAGE_TAG ?= 99.9-dev
RELEASE_TAG ?= $(shell tagged="$$(git describe --tags --match='v*' --abbrev=0 2> /dev/null)"; if [ "$$tagged" ] && [ "$$(git rev-list -n1 HEAD)" = "$$(git rev-list -n1 $$tagged)" ]; then echo $$tagged; fi)

SCHEDULER_NAME = hwameistor-scheduler
SCHEDULER_IMAGE_DIR = ${PROJECT_SOURCE_CODE_DIR}
SCHEDULER_BUILD_BIN = ${BINS_DIR}/${SCHEDULER_NAME}-run
SCHEDULER_BUILD_MAIN = ${CMDS_DIR}/main.go

SCHEDULER_IMAGE_NAME = ${DOCKER_REGISTRY}/${SCHEDULER_NAME}
RELEASE_SCHEDULER_IMAGE_NAME=${RELEASE_DOCKER_REGISTRY}/${SCHEDULER_NAME}

.PHONY: compile
compile:
	GOARCH=amd64 ${BUILD_ENVS} ${BUILD_CMD} ${BUILD_OPTIONS} -o ${SCHEDULER_BUILD_BIN} ${SCHEDULER_BUILD_MAIN}

.PHONY: scheduler_arm64
scheduler_arm64:
	GOARCH=arm64 ${BUILD_ENVS} ${BUILD_CMD} ${BUILD_OPTIONS} -o ${SCHEDULER_BUILD_BIN} ${SCHEDULER_BUILD_MAIN}

.PHONY: image
image: 
	${DOCKER_MAKE_CMD} make scheduler
	docker build -t ${SCHEDULER_IMAGE_NAME}:${IMAGE_TAG} -f ${SCHEDULER_IMAGE_DIR}/Dockerfile ${PROJECT_SOURCE_CODE_DIR}
	docker push ${SCHEDULER_IMAGE_NAME}:${IMAGE_TAG}

.PHONY: release
release: 
	# build for amd64 version
	${DOCKER_MAKE_CMD} make scheduler
	${DOCKER_BUILDX_CMD_AMD64} -t ${RELEASE_SCHEDULER_IMAGE_NAME}:${RELEASE_TAG}-amd64 -f ${SCHEDULER_IMAGE_DIR}/Dockerfile ${PROJECT_SOURCE_CODE_DIR}
	# build for arm64 version
	${DOCKER_MAKE_CMD} make scheduler_arm64
	${DOCKER_BUILDX_CMD_ARM64} -t ${RELEASE_SCHEDULER_IMAGE_NAME}:${RELEASE_TAG}-arm64 -f ${SCHEDULER_IMAGE_DIR}/Dockerfile ${PROJECT_SOURCE_CODE_DIR}
	# push to a public registry
	${MUILT_ARCH_PUSH_CMD} ${RELEASE_SCHEDULER_IMAGE_NAME}:${RELEASE_TAG}

.PHONY: builder
builder:
	docker build -t ${BUILDER_NAME}:${BUILDER_TAG} -f builder.Dockerfile .

.PHONY: debug
debug:
	${DOCKER_DEBUG_CMD} ash

.PHONY: vendor
vendor:
	go mod tidy -compat=1.17
	go mod vendor

.PHONY: clean
clean:
	go clean -r -x
	rm -rf ${BINS_DIR}
	docker container prune -f
	docker rmi -f $(shell docker images -f dangling=true -qa)

unit-test:
	bash shell/unit-test.sh
