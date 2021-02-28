# tap-momentum

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [FIXME](http://example.com)
- Extracts the following resources:
  - [FIXME](http://example.com)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

To run, 

* from `/pyver/tap-momentum` folder,
  * To setup,
    * run `python3 -m venv .venv`
    * run `source ~/.venv/bin/activate`
    * run `pip install -e .`
    
  * To run,
    * run `source .venv/bin/activate`
    * run `tap-momentum -c sample_config.json --catalog  discovery.json`





Copyright &copy; 2018 Stitch
