## Data Warehouse

*See example.py for an example usage*. The main method is called `run()`.

This requires `$ bq` command line tool, to download that, do:

    $ sudo snap install google-cloud-sdk

 And the first time you use it, you will need to authenticate:

    $ gcloud auth login

And finally, to set the correct project, run

    $ gcloud set project first-outlet-750

---

There are three helper folders:

1. `var` is the directory log files are written to.
2. `data` is the directory the sql results are (temporarily) written to.
3. `proc` tracks running and completed processes.
