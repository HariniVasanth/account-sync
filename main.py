import logging
import os
import sys
import time
from datetime import datetime
import csv
import json

import planon
import ipass.utils as utils
from requests.exceptions import HTTPError

# ==========================================================================================================================
# LOGGING
# ==========================================================================================================================

log_level = os.environ.get("LOG_LEVEL", "INFO")
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logging.basicConfig(stream=sys.stdout, level=log_level, format=log_format)

# Set the log to use GMT time zone
logging.Formatter.converter = time.gmtime

# Add milliseconds
logging.Formatter.default_msec_format = "%s.%03d"

log = logging.getLogger(__name__)

# ==========================================================================================================================
# SETUP
# ==========================================================================================================================

today = datetime.today().date()

with open("account_sync_excludes.json", "r") as file:
    account_sync_excludes = {account["account_name"]: account for account in json.load(file)}
    log.debug(f"Account groups to exclude: {account_sync_excludes}")

# ==========================================================
# PLANON API
# ==========================================================

# Planon API configuration
log.info("Setting Planon API site")
planon.PlanonResource.set_site(site=os.environ["PLANON_API_URL"])

log.info("Setting Planon API authentication headers")
planon.PlanonResource.set_header(jwt=os.environ["PLANON_API_KEY"])

# ==========================================================================================================================
# MAIN
# Updates - Update the displayname for accounts that do not have one
# Creates -  if there is no account for the person then create an account and then link to the user-group & person
# Deletes - if the accounts exists in Planon and not in HRMS , then lookup the account & remove the user group
# Person record will be created at this point with Planon-Prod-people , if they do not exist do not link them -
# link only if person record exists
# ==========================================================================================================================

# ==========================================================
# SOURCE DARTMOUTH PEOPLE
# ==========================================================

log.info("Caching iPaaS data")
dart_people = {person["netid"]: person for person in utils.get_people()}
log.info(f"Total number of Dartmouth people: {len(dart_people)}")

dart_people_with_excludes = {person["netid"]: person for person in utils.get_people() if (person["first_name"] != "nonperson" and  person["first_name"] != "NONPERSON" and person["first_name"] != "NonPerson") and person["dartmouth_affiliation"] != "SERVICE"}
log.info(f"Total number of Dartmouth people excluding service and non-person accounts: {len(dart_people_with_excludes)}")

# ==========================================================
# PLANON DATA
# ==========================================================

# All active Dartmouth people with a Planon account should be in this group
log.info("Caching Planon data")
account_groups = {account_group.Description: account_group for account_group in planon.AccountGroup.find()}
planon_requestor_group = account_groups["DC - Requestors"]

account_filter = {"filter": {"EndDate": {"exists": False}}}  # without end-date condition it pulls inactive accounts as well which the REST API does not support at this time
log.debug("Getting list of Planon accounts")
planon_accounts = {account.Accountname.lower(): account for account in planon.Account.find(account_filter)}
log.debug(f"Total number of Planon accounts: {len(planon_accounts)}")

# ========================================================================================
# INSERTS
# Find the difference between Dart people and planon accounts
# Create an account and link the group for those accounts
# ========================================================================================

inserts_succeeded = []
inserts_skipped = []
inserts_failed = []
inserts_failed_to_link_person = []

inserts = set(dart_people_with_excludes) - set(planon_accounts)
log.info(f"Total number of people to be inserted in Planon {len(inserts)}")

for insert in inserts:
    log.debug(f"Processing insert for {insert}")

    try:
        dart_person = dart_people_with_excludes[insert]

        # Create Planon account
        log.debug(f"Inserting account {insert}")
        planon_account = planon.Account.create(
            values={
                "Accountname": insert,
                "Description": dart_person["name"],  # name in ipaas
                "BeginDate": today.strftime("%Y-%m-%d"),
                "PasswordNeverExpires": True,
            }
        )

        log.info(f"Inserted account for {insert}")

        # Link account to a user group
        log.info("Linking group")
        account_group_link = planon.AccountAccountGroup.create(
            values={
                "AccountGroupRef": planon_requestor_group.Syscode,
                "AccountRef": planon_account.Syscode,
            }
        )
        log.info(f"Linked account for {insert} to requestor group {planon_requestor_group.Description}, syscode {planon_requestor_group.Syscode} ")

        # Link the Planon account to the Planon person, if the person exists, this process should run after the person sync process to ensure
        # that the person exists in Planon
        log.debug("Finding person to link to account")
        person_filter = {
            "filter": {
                "FreeString7": {"eq": insert},
            }
        }

        (planon_person,) = planon.UsrPerson.find(person_filter)

        log.debug("Linking account to person")
        account_person_link = planon.AccountPerson.create(values={"PersonRef": planon_person.Syscode, "AccountRef": planon_account.Syscode})

        log.info(f"Account created for {insert} and linked to person {planon_person.Code}")

        inserts_succeeded.append(insert)

    except HTTPError as e:
        # There is a limitation in the Planon API where it does not support reference date awareness (RDA), which prevents us from querying
        # accounts that already exist.  This causes the logic that compares potential inserts to existing accounts to fail.  We can catch
        # the exception and skip the insert if the account already exists. The error is generally presented as an HTTP 422 "unprocessable entity",
        # with a description of "description": "The User name field with value D36616B on Users is not unique."


        if "not unique" in str(e):
            log.warning(f"Account {insert} is not unique and was skipped, this is due to reference date awareness (RDA).")
            inserts_skipped.append(insert)
        elif "unpack" in str(e):
            # Handle unpack errors that causes failure to link to a PER record
            log.error(f"Account {insert} failed to link to a PER record due to {e} ")
            inserts_failed_to_link_person.append(insert)
        else:
            # Handle other HTTPError exceptions here
            log.error(f"Account {insert} failed to create due to {e} ")
            inserts_failed.append(insert)

    except Exception as e:
        if "unpack" in str(e):
            # Handle unpack errors that causes failure to link to a PER record
            log.error(f"Account {insert} failed to link to a PER record due to {e} ")
            inserts_failed_to_link_person.append(insert)
        else:
            log.error(f"Creation of account for {insert} triggered an exception {e}")
            inserts_failed.append(insert)

# ===============================================================================
# UPDATES
# Find the set union between Dart people and planon accounts
# Update displayname to accounts that do not have a displayname and not present in iPaas
# =====================================================================================

updates_succeeded = []
updates_failed = []

account_filter = {"filter": {"EndDate": {"exists": False}}}  # without end-date condition it pulls inactive accounts as well which the REST API does not support at this time
planon_accounts = {account.Accountname.lower(): account for account in planon.Account.find(account_filter)}
updates = set(planon_accounts) & set(dart_people)

for update in updates:
    try:
        dart_person = dart_people[update]
        planon_account = planon_accounts[update]

        # check if name matches both in ipass and Planon Person BO
        if dart_person["name"] != planon_account.Description:
            planon_account.Description = dart_person["name"]  # name in Person BO
            planon_account = planon_account.save()

            updates_succeeded.append(update)
            log.info(f"Displayname {planon_account.Description} updated for {planon_account.Accountname}")

        else:
            log.debug(f"Displayname {planon_account.Description} matches for {planon_account.Accountname}, no update required")

    except Exception as e:
        updates_failed.append(update)
        log.info(f"Failed to find the linked person for  {update} exception {e}")

# ==========================================================================================
# DEACTIVATES
# Filter inactive_excludes by looping through each accountgroupref from json file
# Find difference between planon accounts and dart accounts and minus excludes for the result
# loop through the result and inactivate account by adding an end date as today
# ==========================================================================================

deactivates_succeeded = []
deactivates_failed = []

deactivates = set(planon_accounts) - set(dart_people) - set(account_sync_excludes)
log.info(f"Total number of accounts to be deactivated: {len(deactivates)}")

for deactivate in deactivates:
    log.debug(f"Processing deactivation for {deactivate}")

    try:
        planon_account = planon_accounts[deactivate]
        planon_account.EndDate = today  # change end date to today

        inactivated_account = planon_account.save()
        deactivates_succeeded.append(deactivate)
        log.info(f"Deactivated account {deactivate} by changing the end-date to {today-1}")

    except Exception as e:
        deactivates_failed.append(deactivate)
        log.exception(f"Failed to deactivate account {deactivate} exception {e}")


# ========================================================================================================================================== #
# PWD_EXPIRES
# filter accounts with password_expires set to yes
# loop through the result and changing 'PasswordNeverExpires' to Yes
# ==========================================================================================
pwd_expire_succeeded=[]
pwd_expire_failed=[]

# #filter accounts with password_expires set to yes
account_filter_pwd = {"filter":{"EndDate": {"exists": False},"PasswordNeverExpires":{"eq": False}}} 
planon_accounts_with_pwd_expires = {account.Accountname.lower(): account for account in planon.Account.find(account_filter_pwd)}
pwd_expires=set(planon_accounts_with_pwd_expires)
log.info(f"Total number of accounts set to expire passwords {len(planon_accounts_with_pwd_expires)}")

for pwd_expire in pwd_expires :
    try:
        planon_account = planon_accounts[pwd_expire]
        planon_account.PasswordNeverExpires = True  # change PasswordNeverExpires to Yes

        pwd_expire_account = planon_account.save()
        pwd_expire_succeeded.append(pwd_expire)
        log.info(f"Modified account {pwd_expire} by changing the PasswordNeverExpires to Yes")

    except Exception as e:
        pwd_expire_failed.append(pwd_expire)
        log.exception(f"Failed to modify account {pwd_expire} due to exception {e}")

# ========================================================================================================================================== #

log.info(
    f"""Logging results\n
# ======================= RESULTS ======================= #

Inserts Succeeded: {len(inserts_succeeded)}
Inserts Failed: {len(inserts_failed)}

Updates Succeeded: {len(updates_succeeded)}
Updates Failed: {len(updates_failed)}

Deactivates Succeeded: {len(deactivates_succeeded)}
Deactivates Failed: {len(deactivates_failed)}

Pwd_expire Succeeded: {len(pwd_expire_succeeded)}
Pwd_expire  Failed: {len(pwd_expire_failed)}

Inserts_failed_to_link_person: {len(inserts_failed_to_link_person)}

"""
)

# *************************************************************************************************
# Set exit code
# *************************************************************************************************

if inserts_skipped:
    log.warning(f"There are {len(inserts_skipped)}accounts that were not created as they already exist with RDA inactive and/or accounts that were not linked to Person record")
    # log the accounts that were skipped

    # Accounts as a list:
    # log.warning(f"Skipped accounts: {inserts_skipped}")

    # Display one account per line:
    for insert_skipped in inserts_skipped:
        log.warning(f"Skipped creating account: {insert_skipped}")
    sys.exit(os.EX_DATAERR)

if inserts_failed_to_link_person:
    log.warning(f"There are accounts{len(inserts_failed_to_link_person)} that were not created as they already exist with RDA inactive and/or accounts that were not linked to Person record")
    # log the accounts that failed to link to a PER record
    for inserts_failed_to_link_person in inserts_failed_to_link_person:
        log.warning(f"Failed to link person record for: {inserts_failed_to_link_person}")
    sys.exit(os.EX_DATAERR)

else:
    log.info("Inserts were processed successfully, exiting")
    sys.exit(os.EX_OK)
