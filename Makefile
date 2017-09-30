.PHONY: test

test:
	cd package-test
	dbt seed
	dbt run
	dbt test
