RAPHTORY_COMMIT := $(shell git rev-parse HEAD)
CURRENT_TIME := $(shell date +"%Y-%m-%dT%H-%M-%S")

K6_IP=$(shell terraform output k6_ip | jq -r '.')
RAPHTORY_IP=$(shell terraform output raphtory_ip | jq -r '.')


# Logs from the last run. Nothing in here reaches the terminal on a run that worked: the server
# and the bench are both noisy (tsc/tsup, the graph load, k6's own summary), and `--kill-others`
# means a server that dies takes k6 down with it, so what you want is the whole lot on disk and
# surfaced only when something breaks. process-k6-output.py prints them when it finds no results,
# and BENCH_VERBOSE=1 prints its report on a good run too.
#
# The k6 log is gzipped as it is written: a long run emits a lot of progress lines, and any
# request/response sampling that is switched on emits a whole JSON blob per sample, which reached
# 200MB uncompressed. Read it with `gzip -dc k6.log.gz | less` (or `| tail -n 100`).
BUILD_LOG ?= build.log
SERVER_LOG ?= server.log
K6_LOG ?= k6.log.gz

build:
	@{ pnpm install --no-frozen-lockfile && pnpm build ; } > $(BUILD_LOG) 2>&1 \
		|| { echo ">>> build failed:"; cat $(BUILD_LOG); exit 1; }

# Purely informational, so nothing in here is allowed to fail.
bench-env:
	@echo ">>> bench environment"
	@k6 version || echo "k6 not on PATH"
	@python -V || true
	@python -c 'import pandas; print("pandas", pandas.__version__)' || echo "pandas not importable"
	@echo ">>> graphs available to the server:"
	@ls -la data/apache 2>&1 || true

bench-local: build
	@rm -f output.csv.gz output.json $(SERVER_LOG) $(K6_LOG)
	@$(MAKE) --no-print-directory bench-env 2>&1 | gzip > $(K6_LOG) || true
	@k6 version > /dev/null 2>&1 || echo ">>> WARNING: k6 is not on PATH"
	@python -c 'import pandas' > /dev/null 2>&1 \
		|| echo ">>> WARNING: pandas is not importable, results cannot be processed"
	@echo ">>> running the bench; $(K6_LOG) and $(SERVER_LOG) have the details"
	@pnpm concurrently --raw --kill-others --names 'raphtory,bench' \
		'python -u server.py > $(SERVER_LOG) 2>&1' \
		'{ sh wait-for-server.sh && k6 run --out csv=output.csv.gz dist/bench.js ; } 2>&1 | gzip >> $(K6_LOG)' \
		; echo ">>> concurrently exited with status $$?" | gzip >> $(K6_LOG)
	@BENCH_SERVER_LOG=$(SERVER_LOG) BENCH_K6_LOG=$(K6_LOG) python process-k6-output.py

# call this like: make bench-e2-standard-16
bench-%:
	TF_VAR_machine_type=$* make bench

bench: deploy push-graphs build-raphtory
	make run-raphtory > /tmp/raphtory-server-logs &
	make run-bench
	make destroy

stress-test: build
	K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_EXPORT=stress-test-report.html k6 run dist/stress-test.js

# Samples request/response pairs into $(SAMPLES) as JSONL (appends across runs).
# SAMPLE_RATE is the chance of recording any given pair; failures and requests slower than
# SLOW_MS are recorded whatever the rate, so `SAMPLE_RATE=0 make stress-test-sampled` keeps just
# those two. The slow ones are pretty printed into $(SLOW_LOG) when the run ends.
SAMPLES ?= samples.jsonl
SAMPLE_RATE ?= 0.01
SLOW_MS ?= 1000
SLOW_LOG ?= slow_req.log
stress-test-sampled: build
	SAMPLE_RATE=$(SAMPLE_RATE) SLOW_MS=$(SLOW_MS) K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_EXPORT=stress-test-report.html \
		k6 run --log-format=raw --console-output=$(SAMPLES) dist/stress-test.js; \
		status=$$?; python slow_requests.py $(SAMPLES) --output $(SLOW_LOG); exit $$status

# Appends the slow requests from $(SAMPLES) to $(SLOW_LOG), slowest first: the query behind the
# max the summary reports, its variables, and whether the time went on the server (waiting) or
# somewhere between k6 and it. Entries already in the log are skipped, so this is safe to repeat.
# N limits how many are appended (0 = all), MIN_MS overrides the run's own SLOW_MS threshold.
N ?= 0
MIN_MS ?= 0
slowest:
	python slow_requests.py $(SAMPLES) --output $(SLOW_LOG) --top $(N) --min-ms $(MIN_MS)

stress-test-multiple: build # 12 x 10m = 2h
	for _ in $(seq 1 12); do \
		K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_EXPORT=stress-test-report.html k6 run --duration=10m dist/stress-test.js
	done

stress-test-long: build
	for _ in $(seq 1 12); do \
		K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_EXPORT=stress-test-report.html k6 run --duration=2h dist/stress-test.js
	done

build-docker-debug:
	cd .. && docker build -t raphtory:debug-base --build-arg RAPHTORY_PROFILE=release-with-debug .
	docker build -t raphtory:debug . -f debug.Dockerfile

stress-until-error-with-docker-and-samply: build
	date
	@while true; do \
		docker run --rm -itd --privileged -v ./profiles:/profiles -p 1736:1736 --name=debug raphtory:debug && \
		if k6 run --duration='5m' dist/stress-test.js; then \
			echo "stopping with SIGKILL" && docker stop --signal=9 debug; \
		else \
			echo "exiting normally" && break; \
		fi; \
	done
	docker exec -it debug sh -c 'apt-get install -y lsof && kill $$(lsof -t -i :1736)'

stress-until-error:
	touch ./profiles/$$(date +"%Y-%m-%dT%H-%M-%S")-start
	cargo build --profile=release-with-debug -p raphtory-graphql
	pnpm install --frozen-lockfile
	pnpm build
	while true; do \
		samply record --save-only --output ./profiles/$$(date +"%Y-%m-%dT%H-%M-%S").json.gz ../target/release-with-debug/raphtory-graphql & \
		if k6 run --vus=1000 --duration='10m' dist/stress-test.js; then \
		    echo ">>> successful stress test, keep going" && kill -9 $$(lsof -t -i :1736); \
		else \
			echo ">>> stress test failed, finishing loop" && break; \
		fi; \
	done
	kill -9 $$(lsof -t -i :1736)

project:
	gcloud config set project $(PROJECT)

auth: project
	gcloud auth login
	gcloud auth configure-docker europe-west2-docker.pkg.dev
	gcloud artifacts locations list

deploy:
	# export $$(grep -v '^#' .env | xargs) && terraform apply -var="commit=$$(git rev-parse HEAD)" -var="ssh_user=$$USER"
	terraform apply -auto-approve -var="ssh_user=$$USER"

# FIXME: RAPHTORY_IP points to the server for graphql benchmarks, not the server for the vector tests
# TODO: move somewhere else
bench-vectors:
	cd vectors && terraform apply -auto-approve -var="ssh_user=$$USER"
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'sudo apt-get update && sudo apt-get install -y git build-essential protobuf-compiler'
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'git clone https://github.com/Pometry/Raphtory.git' || true
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'cd Raphtory && git checkout $(RAPHTORY_COMMIT)'
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'curl https://sh.rustup.rs -sSf | sh'
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t '. "$$HOME/.cargo/env" && cargo run --release -p raphtory-benchmark --bin vectorise'
	cd vectors && terraform destroy -auto-approve -var="ssh_user=$$USER"

destroy:
	# export $$(grep -v '^#' .env | xargs) && terraform destroy -var="commit=$$(git rev-parse HEAD)" -var="ssh_user=$$USER"
	terraform destroy -auto-approve -var="ssh_user=$$USER"

push-graphs:
	cd ../applications/apache/server/data/graphs && tar -czvf /tmp/graphs.tar.gz .
	scp -o 'StrictHostKeyChecking no' -r /tmp/graphs.tar.gz $(RAPHTORY_IP):/tmp/graphs.tar.gz
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'mkdir /tmp/graphs && tar -xzvf /tmp/graphs.tar.gz -C /tmp/graphs'

build-raphtory:
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'sudo apt-get update && sudo apt-get install -y git docker.io'
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'git clone https://github.com/Pometry/Raphtory.git' || true
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'cd Raphtory && git checkout $(RAPHTORY_COMMIT)'
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'cd Raphtory && sudo docker build -t raphtory .'
	# ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'cd Raphtory && sudo docker run --rm -v /tmp/graphs:/app/graphs -p 80:1736 raphtory'

run-raphtory:
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'cd Raphtory && sudo docker run --rm -v /tmp/graphs:/app/graphs -p 80:1736 raphtory'

raphtroy-logs:
	tail -f /tmp/raphtory-server-logs

run-bench:
	mkdir -p results
	mkdir -p reports
	pnpm build
	ssh -o 'StrictHostKeyChecking no' $(K6_IP) -t 'rm -fr /tmp/bench'
	scp -o 'StrictHostKeyChecking no' -r ./dist $(K6_IP):/tmp/bench
	ssh -o 'StrictHostKeyChecking no' $(K6_IP) -t 'RAPHTORY_URL=http://$(RAPHTORY_IP) K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_EXPORT=/tmp/bench/report.html k6 run --out csv=/tmp/bench/output.gz /tmp/bench/test.js' || true
	scp -o 'StrictHostKeyChecking no' $(K6_IP):/tmp/bench/report.html reports/$(CURRENT_TIME).html
	scp -o 'StrictHostKeyChecking no' $(K6_IP):/tmp/bench/output.gz results/$(CURRENT_TIME).gz
	# TODO: give name to file based on type of machine, raphtory commit and date

ssh-k6:
	ssh -o 'StrictHostKeyChecking no' $(K6_IP)

ssh-raphtory:
	ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP)

archive:
	# wget https://raw.githubusercontent.com/Kentzo/git-archive-all/refs/heads/master/git_archive_all.py -O /tmp/git-archive-all.py
	# chmod +x /tmp/git-archive-all.py
	# /tmp/git-archive-all.py -C ../applications/apache/custom_raphtory/raphtory raphtory.tar.gz
	# # tar -xzf raphtory.tar.gz -C /tmp/raphtory
	cd ../applications/apache/custom_raphtory/raphtory/pometry-storage-private && git archive --format=tar HEAD | gzip > /tmp/pometry-storage-private.tar.gz

push-storage:
	cd ../applications/apache/custom_raphtory/raphtory && make build-python
	python build_disk_graphs.py
	cd ../applications/apache/custom_raphtory/raphtory/pometry-storage-private && git archive --format=tar HEAD | gzip > /tmp/pometry-storage-private.tar.gz
	scp -o 'StrictHostKeyChecking no' -r /tmp/pometry-storage-private.tar.gz $(RAPHTORY_IP):/tmp/pometry-storage-private.tar.gz
	scp -o 'StrictHostKeyChecking no' -r storage.Dockerfile $(RAPHTORY_IP):/tmp/storage.Dockerfile
	scp -o 'StrictHostKeyChecking no' -r /tmp/graphs/* $(RAPHTORY_IP):/tmp/graphs/
	# TODO: remove pometry-storage-private from .dockerignore
	# TODO: move storage.Dockerfile to /Raphtory
	# TODO: move graphs
	# ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'tar -xzvf /tmp/pometry-storage-private.tar.gz -C /Raphtory/pometry-storage-private'
	# ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'make activate-storage'
	# ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'docker build -t storage . -f storage.Dockerfile'
	# ssh -o 'StrictHostKeyChecking no' $(RAPHTORY_IP) -t 'docker run -v /tmp/graphs:/app/graphs -p 80:1736 storage'
