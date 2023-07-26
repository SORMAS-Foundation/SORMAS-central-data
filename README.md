# SORMAS-central-data

Provides international infrastructure data for continents, sub-continents and countries, \
as-well as localized countries, regions, disticts and communities for Germany.

This includes a uuid which is important to enable:

* a central management and updating of infrastructure data across SORMAS instances
* exchanging data across SORMAS instances that references the same infrastructure data

The data is provided in the [`out folder`](out) in the following formats:

* **csv** to be imported into SORMAS instances via the infrastructure admin UI.
  The international data is also copied to the [SORMAS backend resources](https://github.com/SORMAS-Foundation/SORMAS-Project/tree/development/sormas-backend/src/main/resources) and can be imported as default infrastructure in SORMAS.
* **json** to be imported into a central etcd server used to synchronize the infrastructure data to multiple SORMAS instances.

When new data needs to be added, it can be copied to the [`in folder`](in). Executing the [main.py] python script will write it to the out folder. \
When data is changed make sure to revert any changes made to the UUID!

## Utility Scripts

The repository also provides python scripts to clean up and verify infrastructure data in a SORMAS database. \
Each script is accompanied by a docker file that allows to run it out of the box.

* alignment: Aligns existing infrastructure data with provided central data based on uuid, name and id, and iso and uno code (in order).
* assessment: Identify duplicate infrastructure data in a SORMAS database
* insert missing: Update uuids of existing communities and add missing communities to a SORMAS database. \
  Not clear how this differs from the generic alignment script.
* verify: Check whether infrastructure data with each uuid exists. If not the data is added. \
  Not clear how this differs from the generic alginment script. Also the fact that is called verify, but adds data is quite misleading.




