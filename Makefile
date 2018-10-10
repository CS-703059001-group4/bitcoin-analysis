FILE := main.py
RUN := docker-compose run --service-ports app
FLAGS := --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11
CMD := bash

all:
	@$(RUN)

shell:
	@$(RUN) make docker-shell CMD="pyspark $(FLAGS)"

submit:
	@$(RUN) make docker-shell CMD="spark-submit $(FLAGS) $(FILE)"

.PHONY: notebook
notebook:
	@$(RUN) make docker-shell CMD="./scripts/notebook"

docker-shell: env
	@. env/bin/activate && $(CMD)

env:
	@virtualenv env

down:
	@docker-compose down

clean:
	@docker-compose down --rmi local
	@rm -rf env
