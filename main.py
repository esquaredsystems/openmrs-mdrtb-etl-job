# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from jobs.migrate_openmrs import run_job

if __name__ == "__main__":
    run_job(hard_reset=False)