from dotenv import load_dotenv
import os

load_dotenv()

BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10000))

# Hard cutoff: keep only data from year > 2020 (i.e. on/after 2021-01-01).
# Encounters (and their obs/orders/lab rows) before this are excluded from the load and purged from the
# target by sp_apply_data_cutoff; patients with no remaining activity are voided (not deleted).
CUTOFF_DATE = '2021-01-01'
