# onyx-batch-fix

An Onyx 0.6.0 application that does distributed things. This project has been populated with a sample job and some basic Onyx idioms to make development easier to use.

## Usage

### Launch the Sample Job in Development

Run the `deftest`s in `test/jobs//sample_job_test.clj`. The tests automatically start and stop the development environment, so make sure don't already have the dev environment (explained below) running - otherwise you'd get a port conflict.

### Start the Development Mode

Load up `env/dev/user.clj`. Evaluate the `go` function.

### Reset the Development Mode

Load up `env/dev/user.clj`. Evaluate the `reset` function.

### Stop the Development Mode

Load up `env/dev/user.clj`. Evaluate the `stop` function.

### Launch the Development Sample Job in a REPL

```clojure
(user/go)
(require 'onyx-batch-fix.jobs.sample-submit-job)
(onyx-batch-fix.jobs.sample-submit-job/submit-job user/system)
```

### Production Mode Peers

Launch the `src/onyx-batch-fix/launcher/launch_prod_peers.clj` main function, giving it an Onyx ID. Optionally, a Dockerfile has been created at the root of the project to package this up into an uberjar for a Java 8 Ubuntu 14 environment.

### Launch the Sample Job in Production

Launch the `src/onyx-batch-fix/launcher/submit_prod_sample_job.clj` main function, giving it an Onyx ID and ZooKeeper address.

## License

Copyright © 2015 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
