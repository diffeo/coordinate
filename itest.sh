#!/bin/sh -x
# Integration test script.
# This doesn't so much check for correct behaivor as non-crashing.
# Correct behavior can usually be determined by reading the log.
#
# Unit tests have better coverage of correct behavior, but don't
# exrecise the command line tool well, so there's this to test the
# command line tool.
#
# usage:
# mkdir tmp
# cd tmp
# ../itest.sh > TESTLOG 2>&1
# egrep -i 'traceback|error' TESTLOG

PORT=5932
HTTPPORT=5980
if `pip freeze | grep -c Jinja2`; then
# pip install Jinja2
  python -m coordinate.run --port=${PORT} --httpd=:${HTTPPORT} --pid=icoordinated.pid &
else
  python -m coordinate.run --port=${PORT} --pid=icoordinated.pid &
fi

cat >iconfig.yaml<<EOF
coordinate:
  address: "127.0.0.1:${PORT}"
EOF

for i in addwork  delete  help   prioritize  retry    status     work_specs  workers clear    failed  load   quit        rpc      summary    work_unit config   flow    pause  resume      run_one  work_spec  work_units; do
    python -m coordinate.coordinatec -c iconfig.yaml help ${i}
    if $?; then echo "ERROR getting help ${i}"; fi
done

cat >iworkspec.yaml<<EOF
name: toyws1
min_gb: 0.1
module: coordinate.tests.test_job_client
run_function: run_function
EOF


python -m coordinate.coordinatec -c iconfig.yaml load --work-spec=iworkspec.yaml --no-work

python -m coordinate.coordinatec -c iconfig.yaml work_specs
python -m coordinate.coordinatec -c iconfig.yaml work_spec --work-spec-name=toyws1 --json
python -m coordinate.coordinatec -c iconfig.yaml work_spec --work-spec-name=toyws1 --yaml
python -m coordinate.coordinatec -c iconfig.yaml summary

python -m coordinate.coordinatec -c iconfig.yaml addwork --work-spec-name=toyws1 -u - <<EOF
{"wu1":{}}
{"wu2":{}}
EOF

python -m coordinate.coordinatec -c iconfig.yaml summary

python -m coordinate.coordinatec -c iconfig.yaml work_unit --work-spec-name=toyws1 wu1 wu2

python -m coordinate.coordinatec -c iconfig.yaml work_units --work-spec-name=toyws1

python -m coordinate.coordinatec -c iconfig.yaml run_one

python -m coordinate.coordinatec -c iconfig.yaml work_units --work-spec-name=toyws1
python -m coordinate.coordinatec -c iconfig.yaml work_units --work-spec-name=toyws1 --details
python -m coordinate.coordinatec -c iconfig.yaml work_units --work-spec-name=toyws1 --details -s available
python -m coordinate.coordinatec -c iconfig.yaml work_units --work-spec-name=toyws1 --details -s finished


python -m coordinate.coordinatec -c iconfig.yaml clear --work-spec-name=toyws1 -s finished
python -m coordinate.coordinatec -c iconfig.yaml status --work-spec-name=toyws1
python -m coordinate.coordinatec -c iconfig.yaml clear --work-spec-name=toyws1 -s available
python -m coordinate.coordinatec -c iconfig.yaml status --work-spec-name=toyws1


cat > iflow.yaml<<EOF
flows:
  first:
    min_gb: 0.1
    module: coordinate.tests.test_job_flow
    run_function: first_function
    then: second
  second:
    min_gb: 0.1
    module: coordinate.tests.test_job_flow
    run_function: second_function
    run_params: ["foo"]
EOF

python -m coordinate.coordinatec -c iconfig.yaml flow iflow.yaml
python -m coordinate.coordinatec -c iconfig.yaml summary

python -m coordinate.coordinatec -c iconfig.yaml addwork --work-spec-name=first -u - <<EOF
{"aa":{}}
{"bb":{}}
{"cc":{}}
{"dd":{}}
{"ee":{}}
EOF

python -m coordinate.coordinatec -c iconfig.yaml prioritize first cc

# doesn't show anything about priority, maybe it should:
#python -m coordinate.coordinatec -c iconfig.yaml work_units --work-spec-name=first --details

# runs cc
python -m coordinate.coordinatec -c iconfig.yaml run_one

python -m coordinate.coordinatec -c iconfig.yaml work_units --work-spec-name=first --details

python -m coordinate.coordinatec -c iconfig.yaml pause first

python -m coordinate.coordinatec -c iconfig.yaml run_one

python -m coordinate.coordinatec -c iconfig.yaml resume first

python -m coordinate.coordinatec -c iconfig.yaml run_one
python -m coordinate.coordinatec -c iconfig.yaml run_one
python -m coordinate.coordinatec -c iconfig.yaml run_one

python -m coordinate.coordinatec -c iconfig.yaml pause second

python -m coordinate.coordinatec -c iconfig.yaml run_one

python -m coordinate.coordinatec -c iconfig.yaml resume second

python -m coordinate.coordinatec -c iconfig.yaml config

# nop
python -m coordinate.coordinatec -c iconfig.yaml quit

# won't show any because we don't have any persistent workers, just run_one
python -m coordinate.coordinatec -c iconfig.yaml workers


python -m coordinate.coordinatec -c iconfig.yaml delete -y

# TODO: things not exercised: retry, rpc

kill -9 `cat icoordinated.pid`
