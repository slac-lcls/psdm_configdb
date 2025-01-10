CONFIGDB_SECRET_PATH ?= secret/lcls/psdm/dev/configdb/mongo
ROLEDB_SECRET_PATH ?= secret/lcls/psdm/dev/roledb/mongo
K8S_CONTEXT ?= psdm-datamgr

.PHONY: ensure-context
ensure-context:
	@kubectl config use-context ${K8S_CONTEXT}

secrets:
	mkdir -p etc/.secrets/
	for i in hosts username password; do vault kv get -format=table --field=$$i $(CONFIGDB_SECRET_PATH) > etc/.secrets/configdb_$$i; done
	for i in hosts username password; do vault kv get -format=table --field=$$i $(ROLEDB_SECRET_PATH) > etc/.secrets/roledb_$$i; done

clean-secrets:
	rm -rf etc/

run-apply:
	kubectl apply -k .

apply: ensure-context secrets run-apply clean-secrets

run-delete:
	kubectl delete -k .

delete: ensure-context secrets run-delete clean-secrets

run-dump:
	kubectl kustomize .

dump: secrets run-dump clean-secrets
