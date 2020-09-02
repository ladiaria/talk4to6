
# Coral Talk data migration script from coral talk v4 to v6.

# Minimal dependencies:

- talk v4 mongo database on localhost.
- talk v6 mongo database on localhost created in a new talk v6 installation with a "site" and a "tennant" created.
- Python modules in requirements.txt installed.

# Purpose:

Migrate APPROVED comments from talk v4 to talk v6.

WARNING: not all the data is migrated exactly, some attributes can be ommited or changed, we wrote this script only to
         migrate important data such as approved comments.

# Usage:

 1. copy settings-sample.py to settings.py and fill it with your environment dependant values.
 2. copy helpers-sample.py to helpers.py and implement your helper functions. (An example is given to use with a custom Django site)
 3. run migrate script
