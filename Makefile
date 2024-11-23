.PHONY:	commit build package release
export PROJECT_NAME := $$(basename $$(pwd))
export PROJECT_VERSION := $(shell cat VERSION)

commit:
		git commit -am "Version $(shell cat VERSION)"
		git push -u origin
merge:
		git checkout main
		git pull origin main
		git merge "Version_$(shell cat VERSION)"
		git push origin main
branch:
		git checkout -b "Version_$(shell cat VERSION)"
		git push --set-upstream origin "Version_$(shell cat VERSION)"
build:
		bumpversion --allow-dirty build
patch:
		bumpversion --allow-dirty patch
minor:
		bumpversion --allow-dirty minor
major:
		bumpversion --allow-dirty major
setup:
		python setup.py sdist
package:
		mvn clean install -P couchbase
release:
		gh release create -R "mminichino/$(PROJECT_NAME)" \
		-t "Release $(PROJECT_VERSION)" \
		-n "Release $(PROJECT_VERSION)" \
		$(PROJECT_VERSION)
upload:
		$(eval REV_FILE := $(shell ls -tr distribution/target/*.zip | tail -1))
		gh release upload --clobber -R "mminichino/$(PROJECT_NAME)" $(PROJECT_VERSION) $(REV_FILE)
remove:
		$(eval REV_FILE := $(shell ls -tr distribution/target/*.zip | tail -1))
		$(eval ASSET_NAME := $(shell basename $(REV_FILE)))
		gh release delete-asset -R "mminichino/$(PROJECT_NAME)" $(PROJECT_VERSION) $(ASSET_NAME) -y
