.PHONY:lint codegen

lint:
	git add .
	pre-commit

codegen:
	datamodel-codegen\
		--input $(path)\
		--input-file-type json\
		--output models/source/$(name).py\
		--class-name $(name)\
		--snake-case-field\
		--use-schema-description\
		--use-title-as-name\
		--target-python-version 3.9
