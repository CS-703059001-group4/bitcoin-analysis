FILE := main.py
RUN := docker-compose run app
FLAGS := --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11
CMD := bash

all:
	@$(RUN)

shell:
	@$(RUN) make docker-shell CMD="pyspark $(FLAGS)"

submit:
	@$(RUN) make docker-shell CMD="spark-submit $(FLAGS) $(FILE)"

docker-shell: env
	@. env/bin/activate && $(CMD)

env:
	@virtualenv env

clean:
	@docker-compose down --rmi local
	@rm -rf env
