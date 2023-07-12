docker-build:
		docker buildx build --platform=linux/amd64 -t romaklimenko.azurecr.io/dirty-archive:latest .

docker-push:
		docker buildx build --platform=linux/amd64 -t romaklimenko.azurecr.io/dirty-archive:latest . && docker push romaklimenko.azurecr.io/dirty-archive

helm-upgrade:
		helm upgrade dirty-archive ./helm --install --history-max 3

helm-dry-run:
		helm upgrade dirty-archive ./helm --install --history-max 3 --dry-run

create-secret:
		kubectl create secret generic account-key --from-file=account-key.json=./account-key.json