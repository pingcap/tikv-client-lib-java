PD_PATH=$(GOPATH)/src/github.com/pingcap/pd
all: reminder

reminder:
	@echo "Before you start, remeber you create table in tidb"
	@echo "the command is the following:"
	@echo "./bin/tidb-server --store=tikv --path="127.0.0.1:2379""
	@echo "If you want to connect with tidb server, use the following:"
	@echo "mysql -h 127.0.0.1 -P 4000 -u root -D test --prompt="tidb\>""

tikv: kv_build
	@echo "starting tikv as daemon"
	../tikv/bin/tikv-server --pd="127.0.0.1:2379" --store=tikv -C ../tikv/etc/config-template.toml&

pd: pd_build
	@echo "starting pd as deamon"
	../pd/bin/pd-server&


pd_build: 
	rm -rf $(PD_PATH) && ln -s $(PWD)/../pd $(PD_PATH)
	cd $(PD_PATH) && (make > /dev/null  2>&1)

kv_build: rust_nightly
	which ../tikv/bin/tikv-server > /dev/null || cd tikv && make

rust_nightly:
	@echo "rust nightly installation takes a lot of time. Leave it for yourself. "
	@echo 'curl -s https://static.rust-lang.org/rustup.sh | sh -s -- --channel=nightly'


