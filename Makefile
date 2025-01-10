IMAGENAME = "slaclab/psdm_configdb"

.PHONY: check-tag

check-tag:
ifndef TAG
	$(error Please define the tag using the TAG environment variable)
endif

build: check-tag
	docker build -t ${IMAGENAME}:${TAG} .

tag:
	docker tag ${IMAGENAME}:${TAG} cr.stanford.edu/psdm/psdm_k8s_images/${IMAGENAME}:${TAG}

push:
	docker push cr.stanford.edu/psdm/psdm_k8s_images/${IMAGENAME}:${TAG}

.DEFAULT_GOAL := all
all: build tag push
